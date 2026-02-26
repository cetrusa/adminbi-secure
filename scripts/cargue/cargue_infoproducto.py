import os
import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
from pandas import DataFrame, Series
from pandas.api import types as pdt
from sqlalchemy import text
from sqlalchemy import types as satypes
from sqlalchemy.exc import SQLAlchemyError

from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con

ProgressCallback = Optional[Callable[[int, str, Optional[Dict[str, object]]], None]]

_NON_NUMERIC_PATTERN = re.compile(r"[^0-9,.-]+")
_EMPTY_NUMERIC_TOKENS = {"", "-", "None", "nan"}


@dataclass
class ArchivoFuente:
    """Metadata mínima requerida para procesar un archivo InfoProducto."""

    path: str
    original_name: str
    fuente_id: str
    fuente_nombre: str
    sede: Optional[str] = None


class CargueInfoProducto:
    """Procesa archivos InfoProducto (XLS basado en HTML) y carga la información en BI."""

    NUMERIC_COLUMNS = [
        "Facturado",
        "Pedido",
        "Faltante",
        "Valor costo $",
        "Valor venta $",
    ]

    EXPECTED_COLUMNS = {
        "Producto",
        "Nombre",
        "Facturado",
        "Pedido",
        "Faltante",
        "Valor costo $",
        "Valor venta $",
        "Codigo pedido",
        "Cliente",
        "Asesor",
    }

    SQL_DTYPES = {
        "fecha_reporte": satypes.Date(),
        "fuente_id": satypes.String(50),
        "fuente_nombre": satypes.String(100),
        "sede": satypes.String(100),
        "producto_codigo": satypes.String(50),
        "producto_nombre": satypes.String(255),
        "cliente_codigo": satypes.String(50),
        "cliente_nombre": satypes.String(255),
        "asesor_codigo": satypes.String(50),
        "asesor_nombre": satypes.String(255),
        "asesor_contacto": satypes.String(50),
        "facturado": satypes.Numeric(18, 2),
        "pedido": satypes.Numeric(18, 2),
        "faltante": satypes.Numeric(18, 2),
        "valor_costo": satypes.Numeric(18, 2),
        "valor_venta": satypes.Numeric(18, 2),
        "codigo_pedido": satypes.String(50),
        "archivo_fuente": satypes.String(255),
    }

    def __init__(
        self,
        database_name: str,
        fecha_reporte: date,
        progress_callback: ProgressCallback = None,
    ) -> None:
        self.database_name = database_name
        self.fecha_reporte = fecha_reporte
        self.progress_callback = progress_callback

        config = ConfigBasic(database_name).config
        self.engine_mysql_bi = self._create_engine_mysql_bi(config)
        self._ensure_table_exists()

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def cargar_archivos(self, archivos: List[ArchivoFuente]) -> Dict[str, object]:
        if not archivos:
            raise ValueError("No se recibieron archivos para procesar.")

        total_archivos = len(archivos)
        resultados: Dict[str, Dict[str, object]] = {}
        total_insertados = 0
        total_filas = 0
        advertencias: List[str] = []

        start_time = time.time()
        self._emit_progress(5, "Preparando carga de InfoProducto")

        for idx, archivo in enumerate(archivos, start=1):
            porcentaje = 10 + int(((idx - 1) / total_archivos) * 80)
            etapa = f"Procesando {archivo.fuente_nombre}"
            self._emit_progress(
                porcentaje,
                etapa,
                {
                    "fuente": archivo.fuente_nombre,
                    "archivo": os.path.basename(archivo.path),
                    "indice": idx,
                    "total": total_archivos,
                },
            )

            try:
                df_original = self._leer_archivo(archivo.path)
                filas_originales = len(df_original)
                total_filas += filas_originales

                df_transformado, meta = self._transformar_dataframe(df_original, archivo)
                if df_transformado.empty:
                    resultados[archivo.fuente_id] = {
                        "status": "sin_datos",
                        "fuente": archivo.fuente_nombre,
                        "mensaje": f"{archivo.fuente_nombre}: Sin datos válidos para procesar",
                        "insertados": 0,
                        "advertencias": meta.get("warnings", []) if meta.get("warnings") else None,
                    }
                    advertencias.extend(
                        [
                            f"{archivo.fuente_nombre}: {aviso}"
                            for aviso in meta.get("warnings", [])
                        ]
                    )
                    continue

                insertados = self._insertar_registros(df_transformado, archivo)
                total_insertados += insertados

                # Mensaje resumido
                msg_resumen = f"{archivo.fuente_nombre}: {insertados:,} registros procesados"
                if meta.get("duplicados", 0) > 0:
                    msg_resumen += f" ({meta['duplicados']} actualizados)"
                if meta.get("descartados", 0) > 0:
                    msg_resumen += f" [{meta['descartados']} descartados]"

                resultados[archivo.fuente_id] = {
                    "status": "exitoso",
                    "fuente": archivo.fuente_nombre,
                    "mensaje": msg_resumen,
                    "insertados": insertados,
                    "advertencias": meta.get("warnings", []) if meta.get("warnings") else None,
                }

                advertencias.extend(
                    [
                        f"{archivo.fuente_nombre}: {aviso}"
                        for aviso in meta.get("warnings", [])
                    ]
                )
            except Exception as exc:  # pragma: no cover - registro defensivo
                resultados[archivo.fuente_id] = {
                    "status": "error",
                    "fuente": archivo.fuente_nombre,
                    "sede": archivo.sede,
                    "error": str(exc),
                }
                advertencias.append(
                    f"{archivo.fuente_nombre}: error al procesar el archivo ({exc})."
                )

        tiempo_total = time.time() - start_time
        stage_final = (
            "Completado con advertencias"
            if advertencias
            else "Carga InfoProducto finalizada"
        )
        self._emit_progress(
            95,
            "Consolidando resultados",
            {
                "total_insertados": total_insertados,
                "total_filas": total_filas,
            },
        )

        # Mensaje final conciso
        tiene_errores = any(res.get("status") == "error" for res in resultados.values())
        archivos_ok = sum(1 for res in resultados.values() if res.get("status") == "exitoso")
        
        if tiene_errores:
            mensaje = f"⚠️ Completado con errores: {archivos_ok}/{total_archivos} archivos OK"
        elif advertencias:
            mensaje = f"✓ Completado: {total_insertados:,} registros ({len(advertencias)} advertencias)"
        else:
            mensaje = f"✓ Completado: {total_insertados:,} registros en {tiempo_total:.1f}s"

        resultado_final = {
            "success": not tiene_errores,
            "message": mensaje,
            "data": resultados,
            "total_insertados": total_insertados,
            "advertencias": advertencias if advertencias else None,
        }

        self._emit_progress(100, stage_final, {"resultado": resultado_final})
        return resultado_final

    # ------------------------------------------------------------------
    # Utilidades internas
    # ------------------------------------------------------------------
    def _create_engine_mysql_bi(self, config: Dict[str, object]):
        user = config.get("nmUsrIn")
        password = config.get("txPassIn")
        host = config.get("hostServerIn")
        port = config.get("portServerIn")
        database = config.get("dbBi")

        if not all([user, password, host, port, database]):
            raise ValueError(
                "Faltan parámetros de conexión a MySQL BI en la configuración"
            )

        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def _ensure_table_exists(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS fact_infoproducto (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            fecha_reporte DATE NOT NULL,
            fuente_id VARCHAR(50) NOT NULL,
            fuente_nombre VARCHAR(100) NOT NULL,
            sede VARCHAR(100) NULL,
            producto_codigo VARCHAR(50) NOT NULL,
            producto_nombre VARCHAR(255) NULL,
            cliente_codigo VARCHAR(50) NOT NULL,
            cliente_nombre VARCHAR(255) NULL,
            asesor_codigo VARCHAR(50) NULL,
            asesor_nombre VARCHAR(255) NULL,
            asesor_contacto VARCHAR(50) NULL,
            facturado DECIMAL(18,2) DEFAULT 0,
            pedido DECIMAL(18,2) DEFAULT 0,
            faltante DECIMAL(18,2) DEFAULT 0,
            valor_costo DECIMAL(18,2) DEFAULT 0,
            valor_venta DECIMAL(18,2) DEFAULT 0,
            codigo_pedido VARCHAR(50) NULL,
            archivo_fuente VARCHAR(255) NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_infoproducto (
                fuente_id,
                codigo_pedido,
                producto_codigo
            )
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        with self.engine_mysql_bi.begin() as conn:
            conn.execute(text(ddl))

    def _emit_progress(
        self,
        percent: int,
        stage: str,
        meta: Optional[Dict[str, object]] = None,
    ) -> None:
        if self.progress_callback:
            try:
                self.progress_callback(percent, stage, meta)
            except Exception:
                # No interrumpir el flujo si el callback falla
                pass

    def _leer_archivo(self, ruta_archivo: str) -> DataFrame:
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"No se encontró el archivo: {ruta_archivo}")

        # Leer archivo como bytes primero
        with open(ruta_archivo, 'rb') as f:
            contenido_bytes = f.read()
        
        # Intentar decodificar con múltiples encodings
        contenido_str = None
        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'windows-1252']
        
        for encoding in encodings_to_try:
            try:
                contenido_str = contenido_bytes.decode(encoding)
                print(f"[INFOPRODUCTO] Archivo decodificado exitosamente con {encoding}")
                break
            except (UnicodeDecodeError, LookupError):
                continue
        
        # Si ningún encoding funcionó, usar replace para forzar la decodificación
        if contenido_str is None:
            print("[INFOPRODUCTO] Usando decodificación forzada con reemplazo de caracteres")
            contenido_str = contenido_bytes.decode('latin-1', errors='replace')
        
        # Parsear el HTML usando pandas
        from io import StringIO
        try:
            # header=0 usa la primera fila como nombres de columnas
            tablas = pd.read_html(StringIO(contenido_str), header=0)
        except ValueError as e:
            raise ValueError(
                f"No se pudo parsear el archivo {os.path.basename(ruta_archivo)} como HTML. "
                f"Error: {str(e)}"
            )

        if not tablas:
            raise ValueError(
                f"El archivo {os.path.basename(ruta_archivo)} no contiene tablas HTML legibles"
            )

        df = tablas[0]
        df = df.dropna(how="all")
        df.columns = [str(col).strip() for col in df.columns]

        if not self.EXPECTED_COLUMNS.issubset(set(df.columns)):
            faltantes = self.EXPECTED_COLUMNS.difference(set(df.columns))
            raise ValueError(
                "El archivo no tiene las columnas esperadas. Faltantes: "
                + ", ".join(sorted(faltantes))
            )

        # Algunas planillas repiten las cabeceras en la primera fila
        primera_fila = df.iloc[0].astype(str).str.lower().tolist()
        columnas_norm = [col.lower() for col in df.columns]
        if primera_fila == columnas_norm:
            df = df.iloc[1:]

        return df.reset_index(drop=True)

    def _transformar_dataframe(
        self, df: DataFrame, archivo: ArchivoFuente
    ) -> Tuple[DataFrame, Dict[str, object]]:
        df = df.copy()
        df = df.dropna(how="all")

        df["Producto"] = df["Producto"].astype(str).str.strip()
        df = df[df["Producto"].str.len() > 0]
        df = df[~df["Producto"].str.contains("TOTAL", case=False, na=False)]

        df["Cliente"] = df["Cliente"].fillna("").astype(str)
        filas_antes_cliente = len(df)
        df = df[df["Cliente"].str.strip() != ""]
        descartados = filas_antes_cliente - len(df)

        if df.empty:
            meta = {
                "warnings": ["No se encontraron filas útiles."],
                "duplicados": 0,
                "descartados": descartados,
            }
            return pd.DataFrame(columns=self.SQL_DTYPES.keys()), meta

        df.loc[:, self.NUMERIC_COLUMNS] = df.loc[:, self.NUMERIC_COLUMNS].apply(
            self._coerce_numeric_series
        )

        producto_codigo, producto_nombre = self._split_codigo_nombre_series(
            df["Producto"]
        )
        df["producto_codigo"] = producto_codigo
        # Si producto_nombre está vacío (no había guion), usar columna "Nombre"
        df["producto_nombre"] = producto_nombre.where(
            producto_nombre.str.len() > 0, 
            df["Nombre"].fillna("").astype(str).str.strip()
        )

        cliente_codigo, cliente_nombre = self._split_codigo_nombre_series(
            df["Cliente"]
        )
        df["cliente_codigo"] = cliente_codigo
        df["cliente_nombre"] = cliente_nombre

        (
            df["asesor_codigo"],
            df["asesor_nombre"],
            df["asesor_contacto"],
        ) = self._split_codigo_nombre_asesor_series(df["Asesor"])

        df["codigo_pedido"] = (
            df["Codigo pedido"].fillna("").astype(str).str.strip()
        )

        df["fuente_id"] = archivo.fuente_id
        df["fuente_nombre"] = archivo.fuente_nombre
        df["sede"] = archivo.sede
        df["fecha_reporte"] = self.fecha_reporte
        df["archivo_fuente"] = os.path.basename(archivo.path)

        subset_cols = [
            "fuente_id",
            "producto_codigo",
            "cliente_codigo",
            "codigo_pedido",
        ]
        filas_antes_dedup = len(df)
        df = df.drop_duplicates(subset=subset_cols, keep="first")
        duplicados = filas_antes_dedup - len(df)

        df = df.dropna(subset=["producto_codigo", "cliente_codigo"])
        df = df[(df["producto_codigo"] != "") & (df["cliente_codigo"] != "")]

        df["facturado"] = df["Facturado"].fillna(0).astype(float)
        df["pedido"] = df["Pedido"].fillna(0).astype(float)
        df["faltante"] = df["Faltante"].fillna(0).astype(float)
        df["valor_costo"] = df["Valor costo $"].fillna(0).astype(float)
        df["valor_venta"] = df["Valor venta $"].fillna(0).astype(float)

        columnas_finales = list(self.SQL_DTYPES.keys())
        df_final = df[columnas_finales]

        warnings = []
        if duplicados:
            warnings.append(
                "Se eliminaron {0} registros duplicados por (fuente, producto, cliente, pedido).".format(
                    duplicados
                )
            )
        if descartados:
            warnings.append(
                "Se descartaron {0} filas sin cliente asociado.".format(descartados)
            )

        meta = {
            "warnings": warnings,
            "duplicados": duplicados,
            "descartados": descartados,
        }
        return df_final.reset_index(drop=True), meta

    def _insertar_registros(self, df: DataFrame, archivo: ArchivoFuente) -> int:
        if df.empty:
            return 0

        try:
            with self.engine_mysql_bi.begin() as conn:
                # Usar INSERT ON DUPLICATE KEY UPDATE para evitar duplicados
                # y actualizar registros existentes
                self._insert_on_duplicate_update(conn, df)
        except SQLAlchemyError as exc:
            raise RuntimeError(
                f"Error insertando registros para {archivo.fuente_nombre}: {exc}"
            ) from exc

        return len(df)
    
    def _insert_on_duplicate_update(self, conn, df: DataFrame) -> None:
        """Inserta registros usando INSERT ON DUPLICATE KEY UPDATE.
        
        Esto previene duplicados basados en la clave única:
        (fecha_reporte, fuente_id, codigo_pedido, producto_codigo)
        
        Si el registro ya existe, actualiza los valores.
        """
        from sqlalchemy import Table, MetaData
        from sqlalchemy.dialects.mysql import insert
        
        metadata = MetaData()
        table = Table('fact_infoproducto', metadata, autoload_with=conn)
        
        # Convertir DataFrame a lista de diccionarios
        records = df.to_dict('records')
        
        # Procesar en lotes de 100 para evitar max_allowed_packet
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Crear INSERT statement
            stmt = insert(table).values(batch)
            
            # Definir qué columnas actualizar en caso de duplicado
            # No actualizamos: id, fecha_reporte, fuente_id, codigo_pedido, producto_codigo (son la clave)
            # Actualizamos: todos los demás campos
            update_dict = {
                'fuente_nombre': stmt.inserted.fuente_nombre,
                'sede': stmt.inserted.sede,
                'producto_nombre': stmt.inserted.producto_nombre,
                'cliente_codigo': stmt.inserted.cliente_codigo,
                'cliente_nombre': stmt.inserted.cliente_nombre,
                'asesor_codigo': stmt.inserted.asesor_codigo,
                'asesor_nombre': stmt.inserted.asesor_nombre,
                'asesor_contacto': stmt.inserted.asesor_contacto,
                'facturado': stmt.inserted.facturado,
                'pedido': stmt.inserted.pedido,
                'faltante': stmt.inserted.faltante,
                'valor_costo': stmt.inserted.valor_costo,
                'valor_venta': stmt.inserted.valor_venta,
                'archivo_fuente': stmt.inserted.archivo_fuente,
                # updated_at se actualiza automáticamente por ON UPDATE CURRENT_TIMESTAMP
            }
            
            # Agregar ON DUPLICATE KEY UPDATE
            stmt = stmt.on_duplicate_key_update(**update_dict)
            
            # Ejecutar
            conn.execute(stmt)

    # ------------------------------------------------------------------
    # Helpers de normalización
    # ------------------------------------------------------------------
    @staticmethod
    def _coerce_numeric(valor) -> float:
        if pd.isna(valor):
            return 0.0
        if isinstance(valor, (int, float)):
            return float(valor)

        texto = str(valor).strip()
        if texto in _EMPTY_NUMERIC_TOKENS:
            return 0.0

        texto = texto.replace("\xa0", "")
        texto = texto.replace("$", "")
        texto = texto.replace(" ", "")

        if "." in texto and "," in texto:
            if texto.rfind(",") > texto.rfind("."):
                texto = texto.replace(".", "")
                texto = texto.replace(",", ".")
            else:
                texto = texto.replace(",", "")
        elif texto.count(",") == 1 and texto.count(".") == 0:
            texto = texto.replace(",", ".")
        else:
            texto = texto.replace(",", "")

        texto = _NON_NUMERIC_PATTERN.sub("", texto)
        try:
            return float(texto)
        except ValueError:
            return 0.0

    @staticmethod
    def _coerce_numeric_series(serie: Series) -> Series:
        if serie.empty:
            return serie.astype(float)

        if pdt.is_numeric_dtype(serie):
            return serie.fillna(0).astype(float)

        valores = serie.astype(str).str.strip()
        mask_vacios = valores.isin(_EMPTY_NUMERIC_TOKENS)

        valores = valores.str.replace("\xa0", "", regex=False)
        valores = valores.str.replace("$", "", regex=False)
        valores = valores.str.replace(" ", "", regex=False)

        tiene_punto = valores.str.contains(".", regex=False)
        tiene_coma = valores.str.contains(",", regex=False)
        ambos = tiene_punto & tiene_coma
        if ambos.any():
            valores.loc[ambos] = valores.loc[ambos].str.replace(".", "", regex=False)
            valores.loc[ambos] = valores.loc[ambos].str.replace(",", ".", regex=False)

        solo_coma = (~tiene_punto) & tiene_coma
        if solo_coma.any():
            valores.loc[solo_coma] = valores.loc[solo_coma].str.replace(",", ".", regex=False)

        valores = valores.str.replace(",", "", regex=False)
        valores = valores.str.replace(_NON_NUMERIC_PATTERN.pattern, "", regex=True)

        numerico = pd.to_numeric(valores, errors="coerce").fillna(0.0)
        numerico.loc[mask_vacios] = 0.0
        return numerico.astype(float)

    @staticmethod
    def _split_codigo_nombre_series(series: Series) -> Tuple[Series, Series]:
        normalizado = series.fillna("").astype(str).str.strip()
        partes = normalizado.str.split("-", n=1, expand=True)
        if partes.shape[1] == 1:
            partes[1] = ""
        # Forzar explícitamente a string para evitar inferencia de tipos numéricos
        codigo = partes[0].fillna("").astype(str).str.strip()
        nombre = partes[1].fillna("").astype(str).str.strip()
        return codigo, nombre

    @classmethod
    def _split_codigo_nombre_asesor_series(
        cls, series: Series
    ) -> Tuple[Series, Series, Series]:
        codigo, resto = cls._split_codigo_nombre_series(series)
        # codigo ya viene como string del método anterior
        resto = resto.fillna("").astype(str).str.strip()
        contacto = resto.str.extract(r"(\d+)$", expand=False)
        contacto = contacto.where(resto.str.match(r".*\d+$"), None)
        # Forzar contacto a string (puede tener ceros a la izquierda)
        contacto = contacto.fillna("").astype(str)
        nombre = resto.str.replace(r"\s*\d+$", "", regex=True).str.strip()
        nombre = nombre.where(resto != "", "")
        # Retornar None para contactos vacíos (después de astype string)
        contacto = contacto.replace("", None)
        return codigo, nombre, contacto
