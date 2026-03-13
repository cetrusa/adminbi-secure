import logging
from typing import Optional, Callable, Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, SQLAlchemyError, IntegrityError, DBAPIError
import time
import gc
import ast
import numpy as np
import datetime
import re
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic

# Configuración global de logging
logging.basicConfig(
    filename="logExtractor.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)


class ExtraeBiConfig:
    """Clase para manejar la configuración y conexiones a bases de datos."""

    def __init__(self, database_name: str):
        self.config_basic = ConfigBasic(database_name)
        self.config = self.config_basic.config
        self.engine_mysql_bi = self._create_engine_mysql_bi()
        self.engine_mysql_out = self._create_engine_mysql_out()
        import os

        db_path = os.path.join("media", "mydata.db")
        self.engine_sqlite = con.ConexionSqlite(db_path)

    def _create_engine_mysql_bi(self):
        c = self.config
        return con.ConexionMariadb3(
            str(c.get("nmUsrIn")),
            str(c.get("txPassIn")),
            str(c.get("hostServerIn")),
            int(c.get("portServerIn")),
            str(c.get("dbBi")),
        )

    def _create_engine_mysql_out(self):
        c = self.config
        return con.ConexionMariadb3(
            str(c.get("nmUsrOut")),
            str(c.get("txPassOut")),
            str(c.get("hostServerOut")),
            int(c.get("portServerOut")),
            str(c.get("dbSidis")),
        )


class ExtraeBiExtractor:
    """Clase principal para la extracción e inserción de datos BI."""

    def __init__(
        self,
        config: ExtraeBiConfig,
        IdtReporteIni: str,
        IdtReporteFin: str,
        user_id: Optional[int] = None,
        id_reporte: Optional[int] = None,
        batch_size: Optional[int] = None,
        progress_callback: Optional[Callable] = None,
    ):
        self.config = config.config
        self.config_basic = config.config_basic
        self.engine_mysql_bi = config.engine_mysql_bi
        self.engine_mysql_out = config.engine_mysql_out
        self.engine_sqlite = config.engine_sqlite
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.user_id = user_id
        self.id_reporte = id_reporte
        self.batch_size = batch_size
        self.progress_callback = progress_callback
        # Variables de proceso
        self.txTabla = None
        self.nmReporte = None
        self.nmProcedure_out = None
        self.nmProcedure_in = None
        self.txSql = None
        self.txSqlExtrae = None
        self._table_columns_cache = {}
        self._primary_keys_cache = {}
        self._disposed = False

    def _dispose_engines(self):
        """Cierra los pools de conexiones para liberar conexiones MySQL.

        Seguro de llamar múltiples veces (idempotente).
        """
        if self._disposed:
            return
        self._disposed = True
        for engine in (self.engine_mysql_bi, self.engine_mysql_out, self.engine_sqlite):
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass
        logging.info("Engines dispuestos correctamente.")

    def __del__(self):
        """Safety net: si el extractor es destruido sin haber llamado _dispose_engines."""
        if not getattr(self, "_disposed", True):
            try:
                self._dispose_engines()
            except Exception:
                pass

    def _resolve_chunk_size(self, fallback: int, minimum: int, maximum: int) -> int:
        raw_value = self.batch_size if self.batch_size is not None else fallback
        try:
            resolved = int(raw_value)
        except (TypeError, ValueError):
            logging.warning(
                f"batch_size inválido ({raw_value}). Se usará valor por defecto: {fallback}."
            )
            return fallback

        clamped = max(minimum, min(resolved, maximum))
        if clamped != resolved:
            logging.warning(
                f"batch_size fuera de rango ({resolved}). Ajustado a {clamped}."
            )
        return clamped

    def _resolve_read_chunk_size(self) -> int:
        # Lectura agresiva: chunks grandes reducen round-trips a la BD origen.
        return self._resolve_chunk_size(fallback=50000, minimum=1000, maximum=200000)

    def _resolve_insert_chunk_size(self) -> int:
        # PyMySQL reescribe executemany a multi-row INSERT; chunks mas grandes = menos round-trips.
        return self._resolve_chunk_size(fallback=15000, minimum=1000, maximum=50000)

    def _get_table_columns(self, table_name: str) -> dict:
        """Obtiene metadata de columnas desde information_schema.columns (cacheado).

        Retorna dict: {col_name: {data_type, is_nullable(bool), column_default}}
        """
        schema = str(self.config.get("dbBi"))
        cache_key = (schema, table_name)
        cached = self._table_columns_cache.get(cache_key)
        if cached is not None:
            return cached

        query = text(
            """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            """
        )
        try:
            with self.engine_mysql_bi.connect() as connection:
                rows = connection.execute(query, {"schema": schema, "table": table_name}).fetchall()
        except Exception as e:
            logging.error(f"Error consultando INFORMATION_SCHEMA.COLUMNS para {schema}.{table_name}: {e}")
            self._table_columns_cache[cache_key] = {}
            return {}

        cols = {}
        for col_name, data_type, is_nullable, col_default in rows:
            cols[str(col_name)] = {
                "data_type": (str(data_type).lower() if data_type is not None else ""),
                "is_nullable": (str(is_nullable).upper() == "YES"),
                "column_default": col_default,
            }

        self._table_columns_cache[cache_key] = cols
        return cols

    @staticmethod
    def _quote_ident(name: str) -> str:
        # Backticks para MariaDB/MySQL. Escapa backticks dobles.
        safe = name.replace("`", "``")
        return f"`{safe}`"

    @staticmethod
    def _timedelta_to_time_str(value: Any) -> str:
        """Convierte pandas/py timedelta a string compatible con MariaDB TIME."""
        # pd.Timedelta / numpy timedelta64 / datetime.timedelta
        if isinstance(value, pd.Timedelta):
            total_seconds = int(value.total_seconds())
        elif isinstance(value, np.timedelta64):
            # Evita dependencias de overloads de pandas: convierte a segundos vía numpy
            total_seconds = int(value / np.timedelta64(1, "s"))
        elif isinstance(value, datetime.timedelta):
            total_seconds = int(value.total_seconds())
        else:
            # fallback: mejor devolver string
            return str(value)

        sign = "-" if total_seconds < 0 else ""
        total_seconds = abs(total_seconds)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"

    @staticmethod
    def _default_for_type(data_type: str) -> Any:
        dt = (data_type or "").lower()
        if dt in {"time"}:
            return "00:00:00"
        if dt in {"datetime", "timestamp"}:
            return "1970-01-01 00:00:00"
        if dt in {"date"}:
            return "1970-01-01"
        if dt in {"int", "integer", "bigint", "smallint", "tinyint", "mediumint", "decimal", "numeric", "float", "double", "real"}:
            return 0
        if dt in {"bit", "bool", "boolean"}:
            return 0
        # varchar/text/enum/otros: por defecto string vacío
        return ""

    def _normalize_and_filter_records_for_table(self, table_name: str, records: list[dict]) -> tuple[list[dict], list[str]]:
        """Filtra columnas inexistentes y normaliza valores incompatibles/NULL antes del INSERT."""
        table_cols = self._get_table_columns(table_name)
        if not table_cols:
            # Sin metadata: no filtramos para no romper, pero sí intentamos normalizar Timedelta.
            normalized = []
            for rec in records:
                out = {}
                for k, v in rec.items():
                    if isinstance(v, (pd.Timedelta, np.timedelta64, datetime.timedelta)):
                        out[k] = self._timedelta_to_time_str(v)
                        logging.warning(f"[WARN] Conversión Timedelta->TIME aplicada: {k}")
                    else:
                        out[k] = v
                normalized.append(out)
            return (normalized, list(records[0].keys())) if records else ([], [])

        valid_columns = [c for c in records[0].keys() if c in table_cols]
        for c in records[0].keys():
            if c not in table_cols:
                logging.warning(f"[WARN] Columna omitida del INSERT: {c} (NO EXISTE EN TABLA)")

        # Requisito D: si una columna es NOT NULL y llega NULL (en TODO el payload), se omite del INSERT.
        # Si viene mezclada (algunas filas NULL), se normaliza (requisito C) para no perder datos.
        to_drop: list[str] = []
        for col in list(valid_columns):
            meta = table_cols[col]
            if meta.get("is_nullable", True):
                continue
            all_null = True
            for rec in records:
                v = rec.get(col)
                if v is None:
                    continue
                if isinstance(v, float) and np.isnan(v):
                    continue
                all_null = False
                break
            if all_null:
                to_drop.append(col)

        if to_drop:
            for col in to_drop:
                logging.warning(
                    f"[WARN] Columna omitida del INSERT: {col} (NULL / NOT NULL)"
                )
                if col in valid_columns:
                    valid_columns.remove(col)

        normalized_records: list[dict] = []
        null_normalized_counts: dict[str, int] = {}
        timedelta_converted: set[str] = set()
        for rec in records:
            out: dict[str, Any] = {}
            for col in valid_columns:
                meta = table_cols[col]
                v = rec.get(col)

                if isinstance(v, (pd.Timedelta, np.timedelta64, datetime.timedelta)):
                    v = self._timedelta_to_time_str(v)
                    timedelta_converted.add(col)

                if v is None and not meta["is_nullable"]:
                    default_v = self._default_for_type(meta.get("data_type", ""))
                    v = default_v
                    null_normalized_counts[col] = null_normalized_counts.get(col, 0) + 1

                out[col] = v

            normalized_records.append(out)

        # Resumen de conversiones (una línea por columna en vez de por registro)
        for col in timedelta_converted:
            logging.warning(f"[WARN] Conversión Timedelta->TIME aplicada: {col}")
        for col, count in null_normalized_counts.items():
            default_v = self._default_for_type(table_cols[col].get("data_type", ""))
            logging.warning(
                f"[WARN] Valor NULL normalizado por NOT NULL: {col} -> '{default_v}' ({count} registros)"
            )

        return normalized_records, valid_columns

    @staticmethod
    def _sanitize_bind_key(col: str) -> str:
        """Genera un nombre de parámetro SQLAlchemy seguro (sin espacios/acentos)."""
        safe = re.sub(r"[^0-9A-Za-z_]", "_", col or "")
        if safe and safe[0].isdigit():
            safe = f"_{safe}"
        return safe or "_col"

    def _build_bind_map(self, columnas: list[str]) -> tuple[dict[str, str], str]:
        """Crea mapping columna→clave_segura y placeholder string (sin necesidad de datos)."""
        bind_map: dict[str, str] = {}
        used: set[str] = set()
        for col in columnas:
            base = self._sanitize_bind_key(col)
            candidate = base
            suffix = 1
            while candidate in used:
                candidate = f"{base}_{suffix}"
                suffix += 1
            bind_map[col] = candidate
            used.add(candidate)
        placeholders = ", ".join(f":{bind_map[col]}" for col in columnas)
        return bind_map, placeholders

    def _build_bindings(self, data_list: list[dict], columnas: list[str]) -> tuple[list[dict], str]:
        """Crea diccionarios con claves saneadas y placeholder string para SQLAlchemy text()."""
        bind_map, placeholders = self._build_bind_map(columnas)
        data_bound = [
            {bind_map[k]: v for k, v in rec.items() if k in bind_map}
            for rec in data_list
        ]
        return data_bound, placeholders

    def _precompute_insert_context(self, table_name: str, sample_columns: list[str]) -> Optional[dict]:
        """Pre-computa metadatos, columnas validas, bind_map y query INSERT una sola vez por tabla.

        Se llama UNA vez antes del loop de chunks para evitar re-calcular en cada chunk.

        Args:
            table_name: nombre de la tabla destino.
            sample_columns: columnas del DataFrame (del primer chunk o de la query).

        Returns:
            dict con: valid_columns, table_cols, bind_map, insert_query_odku, insert_query_ignore
            o None si no hay columnas validas.
        """
        table_cols = self._get_table_columns(table_name)
        if not table_cols:
            valid_columns = list(sample_columns)
        else:
            valid_columns = [c for c in sample_columns if c in table_cols]
            for c in sample_columns:
                if c not in table_cols:
                    logging.warning(f"[WARN] Columna omitida del INSERT: {c} (NO EXISTE EN TABLA)")

        if not valid_columns:
            logging.error(f"No quedaron columnas válidas para insertar en {table_name}.")
            return None

        bind_map, placeholders = self._build_bind_map(valid_columns)
        columnas_str = ", ".join(self._quote_ident(c) for c in valid_columns)

        # Query para ON DUPLICATE KEY UPDATE
        update_columns = ", ".join(
            f"{self._quote_ident(col)}=VALUES({self._quote_ident(col)})" for col in valid_columns
        )
        insert_query_odku = (
            f"INSERT INTO {table_name} ({columnas_str})\n"
            f"VALUES ({placeholders})\n"
            f"ON DUPLICATE KEY UPDATE {update_columns};"
        )

        # Query para INSERT IGNORE
        insert_query_ignore = (
            f"INSERT IGNORE INTO {table_name} ({columnas_str})\n"
            f"VALUES ({placeholders});"
        )

        return {
            "valid_columns": valid_columns,
            "table_cols": table_cols,
            "bind_map": bind_map,
            "insert_query_odku": insert_query_odku,
            "insert_query_ignore": insert_query_ignore,
        }

    def _normalize_dataframe_for_insert(
        self, df: pd.DataFrame, valid_columns: list[str], table_cols: dict
    ) -> pd.DataFrame:
        """Normaliza el DataFrame completo para INSERT usando operaciones vectorizadas.

        Reemplaza la normalizacion fila-por-fila de _normalize_records_chunk.
        Operaciones:
        - Filtra solo valid_columns
        - Convierte columnas Timedelta a string TIME
        - Rellena NULL con defaults en columnas NOT NULL
        - Reemplaza NaN y strings vacios con None (para columnas nullable)
        """
        result = df[valid_columns].copy()

        if not table_cols:
            # Sin metadata: solo convertir Timedeltas
            for col in valid_columns:
                if hasattr(result[col], 'dt') and result[col].dtype == 'timedelta64[ns]':
                    result[col] = result[col].apply(self._timedelta_to_time_str)
            return result

        timedelta_cols = []
        not_null_fills = {}  # col -> default_value

        for col in valid_columns:
            meta = table_cols.get(col, {})
            # Detectar columnas timedelta
            if hasattr(result[col], 'dt') and result[col].dtype == 'timedelta64[ns]':
                timedelta_cols.append(col)
            elif result[col].dtype == object:
                # Verificar si hay valores timedelta mixtos en columnas object
                sample = result[col].dropna().head(5)
                if len(sample) > 0 and any(
                    isinstance(v, (pd.Timedelta, np.timedelta64, datetime.timedelta))
                    for v in sample
                ):
                    timedelta_cols.append(col)

            # Preparar defaults para NOT NULL
            if not meta.get("is_nullable", True):
                not_null_fills[col] = self._default_for_type(meta.get("data_type", ""))

        # Convertir timedeltas vectorizadamente
        for col in timedelta_cols:
            result[col] = result[col].apply(
                lambda v: self._timedelta_to_time_str(v)
                if isinstance(v, (pd.Timedelta, np.timedelta64, datetime.timedelta))
                else v
            )
            logging.info(f"[OPT] Conversión Timedelta->TIME aplicada vectorizadamente: {col}")

        # float64 no puede almacenar None (lo convierte de vuelta a NaN),
        # convertir columnas float con NaN a object para que .where() funcione
        float_with_nan = [
            col for col in result.columns
            if result[col].dtype.kind == 'f' and result[col].isna().any()
        ]
        if float_with_nan:
            result[float_with_nan] = result[float_with_nan].astype(object)

        # Reemplazar NaN/empty con None (para columnas nullable)
        result = result.where(pd.notnull(result), None)
        result = result.replace({"": None})

        # Rellenar defaults para columnas NOT NULL
        for col, default_val in not_null_fills.items():
            null_count = result[col].isna().sum()
            if null_count > 0:
                result[col] = result[col].fillna(default_val)
                # fillna no cubre None explícitos en columnas object; usar where
                result[col] = result[col].where(result[col].notna(), default_val)
                logging.warning(
                    f"[WARN] NULL normalizado por NOT NULL: {col} -> '{default_val}' ({null_count} registros)"
                )

        return result

    def _determine_valid_columns(self, table_name: str, df: pd.DataFrame) -> tuple[list[str], dict]:
        """Determina columnas válidas para INSERT usando operaciones de DataFrame (sin to_dict masivo).

        Retorna (valid_columns, table_cols_metadata).
        """
        table_cols = self._get_table_columns(table_name)
        if not table_cols:
            return list(df.columns), {}

        valid_columns = [c for c in df.columns if c in table_cols]
        for c in df.columns:
            if c not in table_cols:
                logging.warning(f"[WARN] Columna omitida del INSERT: {c} (NO EXISTE EN TABLA)")

        # NOT NULL columns que son enteramente NULL → se omiten del INSERT.
        to_drop: list[str] = []
        for col in list(valid_columns):
            meta = table_cols[col]
            if meta.get("is_nullable", True):
                continue
            if df[col].isna().all():
                to_drop.append(col)

        if to_drop:
            for col in to_drop:
                logging.warning(
                    f"[WARN] Columna omitida del INSERT: {col} (NULL / NOT NULL)"
                )
                if col in valid_columns:
                    valid_columns.remove(col)

        return valid_columns, table_cols

    def _normalize_records_chunk(
        self, records: list[dict], valid_columns: list[str], table_cols: dict
    ) -> list[dict]:
        """Normaliza valores de un chunk de records (Timedelta→str, NULL→default en NOT NULL)."""
        if not table_cols:
            # Sin metadata: solo convertir Timedeltas.
            normalized = []
            for rec in records:
                out = {}
                for k in valid_columns:
                    v = rec.get(k)
                    if isinstance(v, (pd.Timedelta, np.timedelta64, datetime.timedelta)):
                        out[k] = self._timedelta_to_time_str(v)
                    else:
                        out[k] = v
                normalized.append(out)
            return normalized

        normalized: list[dict] = []
        for rec in records:
            out: dict[str, Any] = {}
            for col in valid_columns:
                meta = table_cols[col]
                v = rec.get(col)
                if isinstance(v, (pd.Timedelta, np.timedelta64, datetime.timedelta)):
                    v = self._timedelta_to_time_str(v)
                if v is None and not meta["is_nullable"]:
                    v = self._default_for_type(meta.get("data_type", ""))
                out[col] = v
            normalized.append(out)
        return normalized

    @staticmethod
    def _strip_sql_comments(sql: str) -> str:
        # Remueve comentarios -- ... y /* ... */ para facilitar parsing liviano.
        sql_no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
        sql_no_line = re.sub(r"--.*?$", " ", sql_no_block, flags=re.M)
        return sql_no_line

    @staticmethod
    def _find_keyword_at_depth(sql: str, keyword: str) -> int:
        """Encuentra la primera ocurrencia de keyword (case-insensitive) a profundidad de paréntesis 0."""
        kw = keyword.upper()
        s = sql
        depth = 0
        i = 0
        while i < len(s):
            ch = s[i]
            if ch == "'":
                # Salta strings simples
                i += 1
                while i < len(s):
                    if s[i] == "'" and s[i - 1] != "\\":
                        i += 1
                        break
                    i += 1
                continue
            if ch == "\"":
                # Salta strings dobles
                i += 1
                while i < len(s):
                    if s[i] == "\"" and s[i - 1] != "\\":
                        i += 1
                        break
                    i += 1
                continue
            if ch == "`":
                # Salta identificadores entre backticks
                i += 1
                while i < len(s):
                    if s[i] == "`":
                        i += 1
                        break
                    i += 1
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)

            if depth == 0:
                # match keyword como palabra completa
                if s[i : i + len(kw)].upper() == kw:
                    before_ok = (i == 0) or not (s[i - 1].isalnum() or s[i - 1] == "_")
                    after_ok = (i + len(kw) >= len(s)) or not (
                        s[i + len(kw)].isalnum() or s[i + len(kw)] == "_"
                    )
                    if before_ok and after_ok:
                        return i
            i += 1
        return -1

    @staticmethod
    def _split_top_level_commas(segment: str) -> list[str]:
        parts = []
        depth = 0
        current = []
        i = 0
        while i < len(segment):
            ch = segment[i]
            if ch == "'":
                current.append(ch)
                i += 1
                while i < len(segment):
                    current.append(segment[i])
                    if segment[i] == "'" and segment[i - 1] != "\\":
                        i += 1
                        break
                    i += 1
                continue
            if ch == "`":
                current.append(ch)
                i += 1
                while i < len(segment):
                    current.append(segment[i])
                    if segment[i] == "`":
                        i += 1
                        break
                    i += 1
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            if ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                i += 1
                continue
            current.append(ch)
            i += 1
        tail = "".join(current).strip()
        if tail:
            parts.append(tail)
        return parts

    @staticmethod
    def _extract_select_alias(expr: str) -> Optional[str]:
        e = expr.strip()
        # expr AS alias
        m = re.search(r"\s+AS\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s*$", e, flags=re.I)
        if m:
            alias = m.group(1)
            return alias.strip("`")
        # expr alias
        m = re.search(r"\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s*$", e)
        if m:
            token = m.group(1)
            # Si la expresión termina en ')' y no hay espacio, esto puede ser función sin alias
            if token.startswith("`") and token.endswith("`"):
                return token.strip("`")
            if token.isidentifier():
                # ojo: podría ser palabra reservada pero igual sirve como alias
                return token
        # fallback: último identificador tras punto
        m = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*$", e)
        return m.group(1) if m else None

    def _rewrite_select_on_duplicate_to_insert(self, sql: str, table_name: str) -> str:
        """Convierte `SELECT ... ON DUPLICATE KEY UPDATE ...` a `INSERT INTO table (...) SELECT ... ON DUPLICATE ...`.

        Solo aplica si la consulta inicia con SELECT y contiene ON DUPLICATE KEY UPDATE.
        """
        raw = sql.strip().lstrip("(")
        raw_nocomments = self._strip_sql_comments(raw)
        if not raw_nocomments.strip().upper().startswith("SELECT"):
            return sql
        if "ON DUPLICATE KEY UPDATE" not in raw_nocomments.upper():
            return sql

        from_pos = self._find_keyword_at_depth(raw_nocomments, "FROM")
        if from_pos < 0:
            return sql
        select_prefix = raw_nocomments[:from_pos]
        from_and_beyond = raw_nocomments[from_pos:]

        # Quita el SELECT inicial
        select_list_str = re.sub(r"^\s*SELECT\s+", "", select_prefix.strip(), flags=re.I)
        select_items = self._split_top_level_commas(select_list_str)
        if not select_items:
            return sql

        table_cols = self._get_table_columns(table_name)
        if not table_cols:
            # Sin metadata: no podemos filtrar, pero sí envolvemos con INSERT sin lista de columnas (más riesgoso).
            logging.warning(
                f"[WARN] No se pudo obtener metadata de {table_name}; reescribiendo a INSERT INTO sin lista de columnas."
            )
            return f"INSERT INTO {table_name} {raw_nocomments}"

        kept_select_items: list[str] = []
        kept_cols: list[str] = []

        for item in select_items:
            alias = self._extract_select_alias(item)
            if not alias:
                continue
            if alias not in table_cols:
                logging.warning(
                    f"[WARN] Columna omitida del INSERT: {alias} (NO EXISTE EN TABLA)"
                )
                continue
            kept_select_items.append(item)
            kept_cols.append(alias)

        if not kept_cols:
            logging.error(
                f"No se pudieron inferir columnas válidas para INSERT INTO {table_name} desde SELECT."
            )
            return sql

        # Filtra assignments del ON DUPLICATE KEY UPDATE a columnas válidas
        odku_pos = self._find_keyword_at_depth(from_and_beyond, "ON DUPLICATE KEY UPDATE")
        if odku_pos >= 0:
            before_odku = from_and_beyond[:odku_pos]
            odku_tail = from_and_beyond[odku_pos:]
            update_list_str = re.sub(
                r"^\s*ON\s+DUPLICATE\s+KEY\s+UPDATE\s+",
                "",
                odku_tail.strip(),
                flags=re.I,
            )
            # Quita ';' final si existe
            update_list_str = update_list_str.rstrip().rstrip(";")
            assignments = self._split_top_level_commas(update_list_str)
            kept_assignments = []
            for a in assignments:
                m = re.match(r"\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\s*=", a)
                if not m:
                    continue
                col = m.group(1)
                if col not in table_cols:
                    logging.warning(
                        f"[WARN] Columna omitida del INSERT: {col} (NO EXISTE EN TABLA)"
                    )
                    continue
                kept_assignments.append(a)
            if kept_assignments:
                from_and_beyond = (
                    before_odku.rstrip()
                    + "\nON DUPLICATE KEY UPDATE\n    "
                    + ",\n    ".join(kept_assignments)
                    + ";"
                )
            else:
                # Si no queda nada por actualizar, removemos el ODKU para evitar sintaxis rara
                from_and_beyond = before_odku.rstrip() + ";"

        cols_sql = ", ".join(self._quote_ident(c) for c in kept_cols)
        select_sql = ", ".join(kept_select_items)
        rewritten = f"INSERT INTO {table_name} ({cols_sql})\nSELECT\n    {select_sql}\n{from_and_beyond.lstrip()}"
        logging.warning(
            f"[WARN] Reescritura aplicada: SELECT ... ON DUPLICATE -> INSERT INTO {table_name} (...) SELECT ..."
        )
        return rewritten

    @staticmethod
    def _optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Downcast tipos numéricos para reducir uso de memoria (30-60% reducción)."""
        for col in df.select_dtypes(include=["int64"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="integer")
        for col in df.select_dtypes(include=["float64"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="float")
        return df

    @staticmethod
    def _log_memory(label: str = ""):
        """Log del uso actual de memoria RAM del proceso."""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / (1024 ** 2)
            logging.info(f"[MEM] {label} RAM usada: {mem_mb:.1f} MB")
        except ImportError:
            pass

    def run(self):
        """Método principal para ejecutar el proceso completo."""
        return self.extractor()

    def extractor(self):
        logging.info("Iniciando extractor")
        errores_tablas = []  # Lista para recolectar errores por tabla
        try:
            txProcedureExtrae = self.config.get("txProcedureExtrae", [])
            if isinstance(txProcedureExtrae, str):
                txProcedureExtrae = ast.literal_eval(txProcedureExtrae)
            total = len(txProcedureExtrae)
            for idx, a in enumerate(txProcedureExtrae, 1):
                sql = text("SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = :a")
                result = self.config_basic.execute_sql_query(sql, {"a": a})
                df = result
                if not df.empty:
                    self.txTabla = df["txTabla"].iloc[0]
                    self.nmReporte = df["nmReporte"].iloc[0]
                    self.nmProcedure_out = df["nmProcedure_out"].iloc[0]
                    self.nmProcedure_in = df["nmProcedure_in"].iloc[0]
                    self.txSql = df["txSql"].iloc[0]
                    self.txSqlExtrae = df["txSqlExtrae"].iloc[0]
                    logging.info(f"Se va a procesar {self.nmReporte}")
                    if self.progress_callback:
                        progress_percent = int((idx - 1) / total * 100)
                        self.progress_callback(
                            {
                                "stage": f"Procesando {a}",
                                "tabla": self.txTabla,
                                "nmReporte": self.nmReporte,
                                "progress": progress_percent,
                            },
                            progress_percent,
                        )
                    try:
                        self.procedimiento_a_sql()
                        logging.info(
                            f"La información se generó con éxito de {self.nmReporte}"
                        )
                    except Exception as e:
                        logging.error(
                            f"No fue posible extraer la información de {self.nmReporte} por {e}"
                        )
                        errores_tablas.append(
                            {
                                "tabla": self.txTabla,
                                "nmReporte": self.nmReporte,
                                "error": str(e),
                            }
                        )
                else:
                    logging.warning(f"No se encontraron resultados para nbSql = {a}")
                    errores_tablas.append(
                        {
                            "tabla": None,
                            "nmReporte": a,
                            "error": f"No se encontraron resultados para nbSql = {a}",
                        }
                    )
            if self.progress_callback:
                self.progress_callback(
                    {
                        "stage": "Extracción completada",
                        "tabla": None,
                        "nmReporte": None,
                        "progress": 100,
                    },
                    100,
                )
            logging.info("Extracción completada con éxito")
            return {
                "status": "completed",
                "success": True,
                "message": "Extracción completada con éxito",
                "errores_tablas": errores_tablas,
                "tablas_procesadas": (
                    [
                        {
                            "tabla": getattr(self, "txTabla", None),
                            "nmReporte": getattr(self, "nmReporte", None),
                        }
                    ]
                    if hasattr(self, "txTabla") and hasattr(self, "nmReporte")
                    else []
                ),
            }
        except Exception as e:
            logging.error(f"Error general en el extractor: {e}")
            errores_tablas.append({"tabla": None, "nmReporte": None, "error": str(e)})
            return {
                "status": "completed",
                "success": False,
                "message": f"Error general en el extractor: {e}",
                "errores_tablas": errores_tablas,
                "error": str(e),
            }
        finally:
            self._dispose_engines()
            logging.info("Finalizado el procedimiento de ejecución SQL.")

    def procedimiento_a_sql(self):
        read_chunk_size = self._resolve_read_chunk_size()
        insert_chunk_size = self._resolve_insert_chunk_size()
        primary_keys = self.obtener_claves_primarias()

        for intento in range(3):
            try:
                step_start = time.perf_counter()
                rows_deleted = self.consulta_sql_bi()
                if rows_deleted == 0:
                    logging.warning(
                        "No se borraron filas en consulta_sql_bi, pero se continuará con la inserción de datos."
                    )
                if self.txSqlExtrae:
                    # Contexto pre-computado: se inicializa con el primer chunk (lazy)
                    insert_context = [None]  # mutable para captura en closure
                    gc_counter = [0]

                    def process_chunk(chunk_df: pd.DataFrame, chunk_num: int, total_read: int):
                        # Lazy init del contexto con las columnas del primer chunk
                        if insert_context[0] is None:
                            ctx = self._precompute_insert_context(
                                str(self.txTabla), list(chunk_df.columns)
                            )
                            insert_context[0] = ctx
                            if ctx:
                                logging.info(
                                    f"[OPT] Contexto INSERT pre-computado para {self.txTabla}: "
                                    f"{len(ctx['valid_columns'])} columnas válidas."
                                )

                        self.insertar_sql(
                            resultado_out=chunk_df,
                            primary_keys=primary_keys,
                            chunk_size=insert_chunk_size,
                            insert_context=insert_context[0],
                        )
                        # gc.collect solo cada 10 chunks para reducir overhead
                        gc_counter[0] += 1
                        if gc_counter[0] % 10 == 0:
                            gc.collect()
                        logging.info(
                            f"Chunk {chunk_num} procesado para {self.txTabla}. Total leído acumulado: {total_read:,}."
                        )

                    extraction_result = self.consulta_sql_out_extrae(
                        chunksize=read_chunk_size,
                        chunk_callback=process_chunk,
                    )
                    if extraction_result and extraction_result.get("total_rows", 0) > 0:
                        elapsed = time.perf_counter() - step_start
                        logging.info(
                            f"Proceso completado para {self.txTabla}: {extraction_result.get('total_rows', 0):,} filas en {elapsed:.2f}s."
                        )
                    else:
                        logging.warning(
                            "No se obtuvieron resultados en consulta_sql_out_extrae, inserción cancelada."
                        )
                        continue
                else:
                    logging.warning(
                        "Se intentó insertar sin un SQL de extracción definido. Proceso cancelado."
                    )
                    break
                logging.info(f"Proceso completado para {self.txTabla}.")
                return
            except Exception as e:
                logging.error(
                    f"Error en procedimiento_a_sql (Intento {intento + 1}/3): {e}"
                )
                # Solo reintentar errores transitorios de conexión/locking.
                err_txt = str(e).lower()
                retryable = isinstance(e, OperationalError)
                if isinstance(e, DBAPIError):
                    retryable = retryable or (
                        "deadlock" in err_txt
                        or "lock wait timeout" in err_txt
                        or "server has gone away" in err_txt
                        or "lost connection" in err_txt
                    )
                if isinstance(e, IntegrityError):
                    retryable = False

                if not retryable:
                    logging.error(
                        "Error no recuperable en procedimiento_a_sql. Se cancela sin reintentos."
                    )
                    raise

                if intento >= 2:
                    logging.error(
                        "Se agotaron los intentos. No se pudo ejecutar el procedimiento."
                    )
                    raise
                logging.info(f"Reintentando procedimiento (Intento {intento + 1}/3)...")
                time.sleep(5)

    def consulta_sql_bi(self) -> int:
        if not self.txSql:
            logging.warning(
                "La variable txSql no contiene ninguna consulta SQL válida."
            )
            return 0
        for intento in range(3):
            try:
                with self.engine_mysql_bi.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as connection:
                    sql_to_run = self.txSql
                    # Corrección obligatoria: nunca permitir SELECT ... ON DUPLICATE (inválido).
                    # Si llega un agregado en txSql, lo reescribimos a INSERT INTO ... SELECT ... ON DUPLICATE.
                    if (
                        isinstance(sql_to_run, str)
                        and sql_to_run.strip().upper().startswith("SELECT")
                        and "ON DUPLICATE KEY UPDATE" in sql_to_run.upper()
                    ):
                        sql_to_run = self._rewrite_select_on_duplicate_to_insert(
                            sql_to_run, str(self.txTabla)
                        )

                    sqldelete = text(sql_to_run)
                    result = connection.execute(
                        sqldelete, {"fi": self.IdtReporteIni, "ff": self.IdtReporteFin}
                    )
                    rows_deleted = result.rowcount
                    logging.info(
                        f"Datos borrados correctamente. Filas afectadas: {rows_deleted} {sql_to_run}"
                    )
                    return rows_deleted
            except Exception as e:
                logging.error(
                    f"Error al borrar datos en consulta_sql_bi (Intento {intento + 1}/3): {e}"
                )
                if intento >= 2:
                    logging.error(
                        "Se agotaron los intentos. No se pudo ejecutar la consulta_sql_bi."
                    )
                    break
                logging.info(
                    f"Reintentando consulta_sql_bi (Intento {intento + 1}/3)..."
                )
                time.sleep(5)
        return 0

    def consulta_sql_out_extrae(
        self,
        chunksize: int = 10000,
        chunk_callback: Optional[Callable[[pd.DataFrame, int, int], None]] = None,
    ) -> Optional[Any]:
        """
        Ejecuta consulta SQL en la base de datos de salida con lectura en chunks.
        
        Args:
            chunksize (int): Tamaño de cada chunk. Por defecto 10,000 registros.
        """
        max_retries = 3
        if self.txSqlExtrae:
            txSqlUpper = self.txSqlExtrae.strip().upper()
            if txSqlUpper.startswith("INSERT") or txSqlUpper.startswith("CALL"):
                isolation_level = "AUTOCOMMIT"
            else:
                isolation_level = "READ COMMITTED"
        else:
            logging.warning("La variable txSqlExtrae está vacía.")
            return None
        
        for retry_count in range(max_retries):
            try:
                with self.engine_mysql_out.connect().execution_options(
                    isolation_level=isolation_level
                ) as connection:
                    sqlout = text(self.txSqlExtrae)

                    # Leer datos en chunks para evitar timeouts
                    chunks = [] if chunk_callback is None else None
                    total_rows = 0
                    total_chunks = 0
                    read_start = time.perf_counter()

                    logging.info(f"Iniciando lectura de datos en chunks de {chunksize:,} registros...")

                    stream_connection = connection.execution_options(
                        stream_results=True,
                        max_row_buffer=10000,
                    )
                    chunk_iterator = pd.read_sql_query(
                        sql=sqlout,
                        con=stream_connection,
                        params={"fi": self.IdtReporteIni, "ff": self.IdtReporteFin},
                        chunksize=chunksize
                    )
                    try:
                        for chunk_num, chunk in enumerate(chunk_iterator, start=1):
                            chunk_rows = len(chunk)
                            total_chunks = chunk_num
                            total_rows += chunk_rows
                            logging.info(f"Chunk {chunk_num}: {chunk_rows:,} registros leídos (Total acumulado: {total_rows:,})")
                            if chunk_callback is not None:
                                chunk_callback(chunk, chunk_num, total_rows)
                                del chunk
                            else:
                                chunks.append(chunk)
                    finally:
                        # Cerrar el iterador para consumir/liberar el cursor SSCursor
                        # antes de que la conexión se cierre. Evita los errores de
                        # 'NoneType' has no attribute 'settimeout' al terminar.
                        chunk_iterator.close()

                    elapsed = max(time.perf_counter() - read_start, 0.001)
                    rows_per_second = total_rows / elapsed

                    if chunk_callback is not None:
                        if total_rows > 0:
                            logging.info(
                                f"Consulta ejecutada con éxito en {isolation_level}. Total: {total_rows:,} registros en {elapsed:.2f}s ({rows_per_second:,.1f} filas/s)."
                            )
                            return {
                                "total_rows": total_rows,
                                "total_chunks": total_chunks,
                                "elapsed_seconds": elapsed,
                            }
                        logging.warning("No se obtuvieron datos de la consulta.")
                        return {
                            "total_rows": 0,
                            "total_chunks": 0,
                            "elapsed_seconds": elapsed,
                        }

                    if chunks:
                        resultado = pd.concat(chunks, ignore_index=True)
                        logging.info(
                            f"Consulta ejecutada con éxito en {isolation_level}. Total: {total_rows:,} registros en {elapsed:.2f}s ({rows_per_second:,.1f} filas/s)."
                        )
                        return resultado
                    else:
                        logging.warning("No se obtuvieron datos de la consulta.")
                        return pd.DataFrame()
                        
            except Exception as e:
                logging.error(
                    f"Error en consulta_sql_out_extrae (Intento {retry_count + 1}/3): {e}"
                )
                if retry_count == max_retries - 1:
                    logging.error(
                        "Se agotaron los intentos en consulta_sql_out_extrae."
                    )
                    return None
                logging.info(
                    f"Reintentando consulta_sql_out_extrae (Intento {retry_count + 1}/{max_retries})..."
                )
                time.sleep(1)

    def insertar_sql(
        self,
        resultado_out: pd.DataFrame,
        primary_keys: Optional[list[str]] = None,
        chunk_size: Optional[int] = None,
        insert_context: Optional[dict] = None,
    ):
        insert_start = time.perf_counter()
        if resultado_out.empty:
            logging.warning(
                "Intento de insertar un DataFrame vacío. Inserción cancelada."
            )
            return

        # Obtener claves primarias antes de procesar
        if primary_keys is None:
            primary_keys = self.obtener_claves_primarias()

        # Filtrar registros con claves primarias NULL
        if primary_keys:
            registros_antes_filtro = len(resultado_out)
            for pk_col in primary_keys:
                if pk_col in resultado_out.columns:
                    registros_null = resultado_out[pk_col].isna().sum()
                    if registros_null > 0:
                        logging.warning(
                            f"Se encontraron {registros_null:,} registros con '{pk_col}' NULL. Serán excluidos."
                        )
                        resultado_out = resultado_out[resultado_out[pk_col].notna()]

            registros_despues_filtro = len(resultado_out)
            if registros_antes_filtro > registros_despues_filtro:
                logging.warning(
                    f"Se excluyeron {registros_antes_filtro - registros_despues_filtro:,} registros con PK NULL."
                )

            if resultado_out.empty:
                logging.error(
                    "Después de filtrar claves primarias NULL, no quedan registros para insertar."
                )
                return

        # Procesamiento de columnas numéricas específicas (latitud/longitud)
        for col in ("latitud_cl", "longitud_cl"):
            if col in resultado_out.columns:
                resultado_out[col] = pd.to_numeric(resultado_out[col], errors="coerce")

        # macrozona_id: asegurar que es numérico
        if "macrozona_id" in resultado_out.columns:
            resultado_out["macrozona_id"] = (
                resultado_out["macrozona_id"].replace({"": 0}).fillna(0)
            )

        # macro: solo convertir a numérico si la columna destino lo requiere
        if "macro" in resultado_out.columns:
            table_cols_meta = (
                insert_context["table_cols"] if insert_context else self._get_table_columns(str(self.txTabla))
            )
            macro_dtype = table_cols_meta.get("macro", {}).get("data_type", "")
            _NUMERIC_TYPES = frozenset({
                "int", "integer", "bigint", "smallint", "tinyint", "mediumint",
                "decimal", "numeric", "float", "double", "real",
            })
            if macro_dtype in _NUMERIC_TYPES:
                resultado_out["macro"] = (
                    pd.to_numeric(resultado_out["macro"], errors="coerce").fillna(0)
                )

        # Usar contexto pre-computado o calcularlo
        if insert_context:
            valid_columns = insert_context["valid_columns"]
            table_cols = insert_context["table_cols"]
        else:
            table_name = str(self.txTabla)
            ctx = self._precompute_insert_context(table_name, list(resultado_out.columns))
            if not ctx:
                logging.error(f"No hay columnas válidas para insertar en {self.txTabla}.")
                return
            valid_columns = ctx["valid_columns"]
            table_cols = ctx["table_cols"]
            insert_context = ctx

        # Normalización vectorizada del DataFrame (reemplaza normalizacion dict-por-dict)
        resultado_out = self._normalize_dataframe_for_insert(resultado_out, valid_columns, table_cols)

        # drop_duplicates solo para tablas SIN PK (ON DUPLICATE KEY maneja duplicados en BD)
        if not primary_keys and len(resultado_out) > 0:
            registros_originales = len(resultado_out)
            resultado_out = resultado_out.drop_duplicates()
            registros_sin_duplicados = len(resultado_out)
            if registros_sin_duplicados < registros_originales:
                logging.info(
                    f"Se eliminaron {registros_originales - registros_sin_duplicados:,} duplicados antes de insertar"
                )

        effective_chunk_size = chunk_size or self._resolve_insert_chunk_size()
        if primary_keys:
            query = insert_context["insert_query_odku"]
        else:
            query = insert_context["insert_query_ignore"]

        method_name = "ON DUPLICATE KEY" if primary_keys else "INSERT IGNORE"
        self._stream_insert_chunks(
            resultado_out, query, valid_columns, table_cols,
            insert_context["bind_map"], effective_chunk_size, method_name,
        )
        elapsed = time.perf_counter() - insert_start
        logging.info(
            f"Insertar_sql finalizado para {self.txTabla}: {len(resultado_out):,} filas en {elapsed:.2f}s."
        )

    def obtener_claves_primarias(self):
        cache_key = (str(self.config.get("dbBi")), str(self.txTabla))
        cached = self._primary_keys_cache.get(cache_key)
        if cached is not None:
            return cached

        query = text(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{self.config.get("dbBi")}' 
            AND TABLE_NAME = '{self.txTabla}'
            AND CONSTRAINT_NAME = 'PRIMARY';
        """
        )
        try:
            with self.engine_mysql_bi.connect() as connection:
                resultado = connection.execute(query)
                primary_keys = [row[0] for row in resultado.fetchall()]
                self._primary_keys_cache[cache_key] = primary_keys
                return primary_keys
        except Exception as e:
            logging.error(f"Error obteniendo claves primarias de {self.txTabla}: {e}")
            return []

    def insertar_con_on_duplicate_key(self, df, chunk_threshold, chunk_size):
        """Inserta con ON DUPLICATE KEY UPDATE. Mantiene compatibilidad con llamadas externas."""
        total_rows = len(df)
        if total_rows == 0:
            logging.warning(f"No hay registros para insertar en {self.txTabla}")
            return
        table_name = str(self.txTabla)
        ctx = self._precompute_insert_context(table_name, list(df.columns))
        if not ctx:
            return
        df = self._normalize_dataframe_for_insert(df, ctx["valid_columns"], ctx["table_cols"])
        self._stream_insert_chunks(
            df, ctx["insert_query_odku"], ctx["valid_columns"], ctx["table_cols"],
            ctx["bind_map"], chunk_size, "ON DUPLICATE KEY",
        )
        logging.info(f"Se insertaron {total_rows:,} registros en {table_name} con ON DUPLICATE KEY.")

    def insertar_con_ignore(self, df, chunk_threshold, chunk_size):
        """Inserta con INSERT IGNORE. Mantiene compatibilidad con llamadas externas."""
        total_rows = len(df)
        if total_rows == 0:
            logging.warning(f"No hay registros para insertar en {self.txTabla}")
            return
        table_name = str(self.txTabla)
        ctx = self._precompute_insert_context(table_name, list(df.columns))
        if not ctx:
            return
        df = self._normalize_dataframe_for_insert(df, ctx["valid_columns"], ctx["table_cols"])
        self._stream_insert_chunks(
            df, ctx["insert_query_ignore"], ctx["valid_columns"], ctx["table_cols"],
            ctx["bind_map"], chunk_size, "INSERT IGNORE",
        )
        logging.info(f"Se insertaron {total_rows:,} registros en {table_name} con INSERT IGNORE.")

    def _stream_insert_chunks(
        self,
        df: pd.DataFrame,
        query: str,
        valid_columns: list[str],
        _table_cols: dict,
        bind_map: dict[str, str],
        chunk_size: int,
        method_name: str,
    ):
        """Inserta DataFrame ya normalizado por chunks via executemany.

        El DataFrame ya viene normalizado por _normalize_dataframe_for_insert,
        solo necesita: to_dict → bind_map → execute.
        _table_cols se mantiene por compatibilidad de firma.
        """
        total_rows = len(df)
        max_retries = 3
        batch_start = time.perf_counter()

        # Renombrar columnas del DF a bind keys de una vez (evita transformacion dict-por-dict)
        df_for_insert = df.rename(columns=bind_map)
        bind_keys = [bind_map[c] for c in valid_columns]

        for start_idx in range(0, total_rows, chunk_size):
            end_idx = min(start_idx + chunk_size, total_rows)

            # to_dict solo del sub-chunk, con columnas ya renombradas
            bound = df_for_insert.iloc[start_idx:end_idx][bind_keys].to_dict(orient="records")

            # Red de seguridad: NaN en columnas numéricas sobrevive .where() de pandas
            # en ciertos escenarios (mixed-type object columns, versiones de pandas).
            # v != v es True SOLO para NaN (IEEE 754).
            for rec in bound:
                for k, v in rec.items():
                    if isinstance(v, float) and v != v:
                        rec[k] = None

            # Insertar con reintentos
            chunk_success = False
            for attempt in range(max_retries):
                try:
                    with self.engine_mysql_bi.begin() as connection:
                        connection.execute(text(query), bound)
                    logging.debug(
                        f"[{method_name}] Chunk {start_idx}-{end_idx} insertado."
                    )
                    chunk_success = True
                    break
                except OperationalError as e:
                    logging.warning(
                        f"Error conexión chunk {start_idx}-{end_idx} (Intento {attempt + 1}/{max_retries}): {e}"
                    )
                    try:
                        con.clear_connection_cache()
                    except Exception:
                        pass
                    time.sleep(2 * (attempt + 1))
                except SQLAlchemyError as e:
                    logging.error(f"Error SQL chunk {start_idx}-{end_idx}: {e}")
                    if "Deadlock" in str(e) or "Lock wait timeout" in str(e):
                        time.sleep(5)
                        continue
                    raise

            if not chunk_success:
                error_msg = f"Fallo definitivo chunk {start_idx}-{end_idx} tras {max_retries} intentos."
                logging.error(error_msg)
                raise Exception(error_msg)

            del bound

        elapsed = max(time.perf_counter() - batch_start, 0.001)
        logging.info(
            f"[{method_name}] Inserción completada en {elapsed:.2f}s ({total_rows / elapsed:,.1f} filas/s)."
        )


# Si se desea ejecutar como script independiente
if __name__ == "__main__":
    # Aquí podrías parsear argumentos y ejecutar el proceso
    # Ejemplo:
    # config = ExtraeBiConfig(database_name="mi_db")
    # extractor = ExtraeBiExtractor(config, "20250101", "20250131")
    # extractor.run()
    pass
