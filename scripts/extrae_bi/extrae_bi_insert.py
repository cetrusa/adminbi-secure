import logging
from typing import Optional, Callable, Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import time
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
        for rec in records:
            out: dict[str, Any] = {}
            for col in valid_columns:
                meta = table_cols[col]
                v = rec.get(col)

                if isinstance(v, (pd.Timedelta, np.timedelta64, datetime.timedelta)):
                    v = self._timedelta_to_time_str(v)
                    logging.warning(f"[WARN] Conversión Timedelta->TIME aplicada: {col}")

                if v is None and not meta["is_nullable"]:
                    default_v = self._default_for_type(meta.get("data_type", ""))
                    v = default_v
                    logging.warning(
                        f"[WARN] Valor NULL normalizado por NOT NULL: {col} -> {default_v}"
                    )

                out[col] = v

            normalized_records.append(out)

        return normalized_records, valid_columns

    @staticmethod
    def _sanitize_bind_key(col: str) -> str:
        """Genera un nombre de parámetro SQLAlchemy seguro (sin espacios/acentos)."""
        safe = re.sub(r"[^0-9A-Za-z_]", "_", col or "")
        if safe and safe[0].isdigit():
            safe = f"_{safe}"
        return safe or "_col"

    def _build_bindings(self, data_list: list[dict], columnas: list[str]) -> tuple[list[dict], str]:
        """Crea diccionarios con claves saneadas y placeholder string para SQLAlchemy text()."""
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

        data_bound = [
            {bind_map[k]: v for k, v in rec.items() if k in bind_map}
            for rec in data_list
        ]
        placeholders = ", ".join(f":{bind_map[col]}" for col in columnas)
        return data_bound, placeholders

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
            logging.info("Finalizado el procedimiento de ejecución SQL.")

    def procedimiento_a_sql(self):
        for intento in range(3):
            try:
                rows_deleted = self.consulta_sql_bi()
                if rows_deleted == 0:
                    logging.warning(
                        "No se borraron filas en consulta_sql_bi, pero se continuará con la inserción de datos."
                    )
                if self.txSqlExtrae:
                    resultado_out = self.consulta_sql_out_extrae()
                    if resultado_out is not None and not resultado_out.empty:
                        self.insertar_sql(resultado_out=resultado_out)
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
                if intento >= 2:
                    logging.error(
                        "Se agotaron los intentos. No se pudo ejecutar el procedimiento."
                    )
                    break
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

    def consulta_sql_out_extrae(self, chunksize: int = 10000) -> Optional[pd.DataFrame]:
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
                    chunks = []
                    total_rows = 0
                    
                    logging.info(f"Iniciando lectura de datos en chunks de {chunksize:,} registros...")
                    
                    for chunk_num, chunk in enumerate(pd.read_sql_query(
                        sql=sqlout,
                        con=connection,
                        params={"fi": self.IdtReporteIni, "ff": self.IdtReporteFin},
                        chunksize=chunksize
                    ), start=1):
                        chunk_rows = len(chunk)
                        total_rows += chunk_rows
                        chunks.append(chunk)
                        logging.info(f"Chunk {chunk_num}: {chunk_rows:,} registros leídos (Total acumulado: {total_rows:,})")
                    
                    if chunks:
                        resultado = pd.concat(chunks, ignore_index=True)
                        logging.info(f"Consulta ejecutada con éxito en {isolation_level}. Total: {total_rows:,} registros.")
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

    def insertar_sql(self, resultado_out: pd.DataFrame):
        if resultado_out.empty:
            logging.warning(
                "Intento de insertar un DataFrame vacío. Inserción cancelada."
            )
            return
        
        # Obtener claves primarias antes de procesar
        primary_keys = self.obtener_claves_primarias()
        
        # Filtrar registros con claves primarias NULL
        if primary_keys:
            registros_antes_filtro = len(resultado_out)
            for pk_col in primary_keys:
                if pk_col in resultado_out.columns:
                    # Filtrar registros donde la clave primaria es NULL
                    registros_null = resultado_out[pk_col].isna().sum()
                    if registros_null > 0:
                        logging.warning(
                            f"Se encontraron {registros_null:,} registros con '{pk_col}' NULL. Estos registros serán excluidos."
                        )
                        resultado_out = resultado_out[resultado_out[pk_col].notna()]
            
            registros_despues_filtro = len(resultado_out)
            if registros_antes_filtro > registros_despues_filtro:
                logging.warning(
                    f"Se excluyeron {registros_antes_filtro - registros_despues_filtro:,} registros con claves primarias NULL."
                )
            
            if resultado_out.empty:
                logging.error(
                    "Después de filtrar claves primarias NULL, no quedan registros para insertar."
                )
                return
        
        # Procesamiento de columnas numéricas específicas
        numeric_columns = ["latitud_cl", "longitud_cl"]
        for col in numeric_columns:
            if col in resultado_out.columns:
                resultado_out[col] = pd.to_numeric(resultado_out[col], errors="coerce")
        
        if "macrozona_id" in resultado_out.columns:
            resultado_out["macrozona_id"] = resultado_out["macrozona_id"].fillna(0)
            resultado_out["macrozona_id"] = resultado_out["macrozona_id"].replace(
                {"": 0}
            )
        
        if "macro" in resultado_out.columns:
            resultado_out["macro"] = pd.to_numeric(
                resultado_out["macro"], errors="coerce"
            )
            resultado_out["macro"] = resultado_out["macro"].fillna(0)
            resultado_out["macro"] = resultado_out["macro"].replace({"": 0})
        
        resultado_out = resultado_out.replace({np.nan: None, "": None})
        
        # Eliminar duplicados
        if len(resultado_out) > 0:
            registros_originales = len(resultado_out)
            resultado_out = resultado_out.drop_duplicates()
            registros_sin_duplicados = len(resultado_out)
            if registros_sin_duplicados < registros_originales:
                logging.info(
                    f"Se eliminaron {registros_originales - registros_sin_duplicados:,} duplicados del dataframe antes de insertar"
                )
        CHUNK_THRESHOLD = 5000
        CHUNK_SIZE = 5000
        primary_keys = self.obtener_claves_primarias()
        if primary_keys:
            self.insertar_con_on_duplicate_key(
                resultado_out, CHUNK_THRESHOLD, CHUNK_SIZE
            )
        else:
            self.insertar_con_ignore(resultado_out, CHUNK_THRESHOLD, CHUNK_SIZE)
        # ✅ NOTA: El logging de éxito se mueve a dentro de insertar_con_on_duplicate_key/insertar_con_ignore
        # para evitar mensajes engañosos cuando la inserción fue cancelada por falta de columnas válidas.

    def obtener_claves_primarias(self):
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
                return [row[0] for row in resultado.fetchall()]
        except Exception as e:
            logging.error(f"Error obteniendo claves primarias de {self.txTabla}: {e}")
            return []

    def insertar_con_on_duplicate_key(self, df, chunk_threshold, chunk_size):
        data_list_raw = df.to_dict(orient="records")
        total_rows = len(data_list_raw)
        logging.info(
            f"Se preparan {total_rows} registros para insertar con ON DUPLICATE KEY en {self.txTabla}."
        )
        if not data_list_raw:
            logging.warning(f"No hay registros para insertar en {self.txTabla}")
            return

        data_list, columnas = self._normalize_and_filter_records_for_table(
            str(self.txTabla), data_list_raw
        )
        if not columnas:
            logging.error(
                f"No quedaron columnas válidas para insertar en {self.txTabla}. Inserción cancelada."
            )
            return

        data_bound, placeholders = self._build_bindings(data_list, columnas)
        columnas_str = ", ".join(self._quote_ident(c) for c in columnas)
        update_columns = ", ".join(
            [f"{self._quote_ident(col)}=VALUES({self._quote_ident(col)})" for col in columnas]
        )
        insert_query = (
            f"INSERT INTO {self.txTabla} ({columnas_str})\n"
            f"VALUES ({placeholders})\n"
            f"ON DUPLICATE KEY UPDATE {update_columns};"
        )
        
        self._execute_batch_insert(insert_query, data_bound, total_rows, chunk_size, "ON DUPLICATE KEY")
        logging.info(f"✓ Se insertaron {total_rows:,} registros en {self.txTabla} con ON DUPLICATE KEY.")

    def insertar_con_ignore(self, df, chunk_threshold, chunk_size):
        data_list_raw = df.to_dict(orient="records")
        total_rows = len(data_list_raw)
        logging.info(
            f"Se preparan {total_rows} registros para insertar con INSERT IGNORE en {self.txTabla}."
        )
        if not data_list_raw:
            logging.warning(f"No hay registros para insertar en {self.txTabla}")
            return

        data_list, columnas = self._normalize_and_filter_records_for_table(
            str(self.txTabla), data_list_raw
        )
        if not columnas:
            logging.error(
                f"No quedaron columnas válidas para insertar en {self.txTabla}. Inserción cancelada."
            )
            return

        data_bound, placeholders = self._build_bindings(data_list, columnas)
        columnas_str = ", ".join(self._quote_ident(c) for c in columnas)
        insert_query = (
            f"INSERT IGNORE INTO {self.txTabla} ({columnas_str})\n"
            f"VALUES ({placeholders});"
        )
        
        self._execute_batch_insert(insert_query, data_bound, total_rows, chunk_size, "INSERT IGNORE")
        logging.info(f"✓ Se insertaron {total_rows:,} registros en {self.txTabla} con INSERT IGNORE.")

    def _execute_batch_insert(self, query: str, data: list[dict], total_rows: int, chunk_size: int, method_name: str):
        """Helper para ejecutar inserciones por lotes con manejo robusto de conexiones y reintentos."""
        max_retries = 3
        
        # Iterar por chunks
        for start_idx in range(0, total_rows, chunk_size):
            end_idx = min(start_idx + chunk_size, total_rows)
            chunk = data[start_idx:end_idx]
            
            # Intentar insertar el chunk actual con reintentos
            chunk_success = False
            for attempt in range(max_retries):
                try:
                    # IMPORTANTE: Abrir transacción NUEVA para cada chunk. 
                    # Esto evita transacciones gigantes que bloquean la DB o mueren por timeout.
                    with self.engine_mysql_bi.begin() as connection:
                        connection.execute(text(query), chunk)
                    
                    logging.debug(
                        f"[{method_name}] Chunk {start_idx}-{end_idx} insertado correctamente."
                    )
                    chunk_success = True
                    break # Éxito, salir del loop de reintentos
                
                except OperationalError as e:
                    logging.warning(
                        f"Error de conexión en chunk {start_idx}-{end_idx} (Intento {attempt + 1}/{max_retries}): {e}"
                    )
                    # Forzar limpieza de conexión muerta
                    try:
                        con.clear_connection_cache() 
                    except:
                        pass
                    time.sleep(2 * (attempt + 1)) # Backoff
                
                except SQLAlchemyError as e:
                    logging.error(f"Error SQL crítico en chunk {start_idx}-{end_idx}: {e}")
                    # Errores de SQL (sintaxis, integridad fatales) no suelen arreglarse con retry
                    # Pero si es deadlock, maybe. Asumimos retry conservador.
                    if "Deadlock" in str(e) or "Lock wait timeout" in str(e):
                        time.sleep(5)
                        continue
                    raise e
            
            if not chunk_success:
                error_msg = f"Fallo definitivo al insertar chunk {start_idx}-{end_idx} tras {max_retries} intentos."
                logging.error(error_msg)
                raise Exception(error_msg)


# Si se desea ejecutar como script independiente
if __name__ == "__main__":
    # Aquí podrías parsear argumentos y ejecutar el proceso
    # Ejemplo:
    # config = ExtraeBiConfig(database_name="mi_db")
    # extractor = ExtraeBiExtractor(config, "20250101", "20250131")
    # extractor.run()
    pass
