# scripts/extrae_bi/trazabilidad.py
import os
import time
import gc
import logging
import uuid
import pathlib

import pandas as pd
from sqlalchemy import text, create_engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from openpyxl import Workbook

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from scripts.text_cleaner import TextCleaner

logger = logging.getLogger(__name__)

SQL_FILE = pathlib.Path(__file__).resolve().parent.parent / "bimbo" / "sql" / "trazabilidad_preventa.sql"

# Columnas para cada nivel de agrupación
_AGG_COLUMNS = """
    SUM(cant_pedida_campo)       AS cant_pedida_campo,
    SUM(cant_asignada_factura)   AS cant_asignada_factura,
    SUM(cant_faltante)           AS cant_faltante,
    SUM(vl_pedido_campo)         AS vl_pedido_campo,
    SUM(vl_faltante_cantidad)    AS vl_faltante_cantidad,
    SUM(vl_brecha_total)         AS vl_brecha_total,
    SUM(bo_faltante_total)       AS agotados_total,
    SUM(bo_faltante_parcial)     AS agotados_parcial,
    COUNT(*)                     AS total_lineas
"""

_GROUPING_QUERIES = {
    "cliente": f"""
        SELECT establecimiento_id, MAX(nmPuntoVenta) AS nmPuntoVenta,
               MAX(zona_id) AS zona_id, {_AGG_COLUMNS}
        FROM trazabilidad_preventa
        WHERE dt_entrega BETWEEN :fi AND :ff
        -- FILTERS_HERE
        GROUP BY establecimiento_id
        ORDER BY establecimiento_id
    """,
    "zona": f"""
        SELECT zona_id, {_AGG_COLUMNS}
        FROM trazabilidad_preventa
        WHERE dt_entrega BETWEEN :fi AND :ff
        -- FILTERS_HERE
        GROUP BY zona_id
        ORDER BY zona_id
    """,
    "macrozona": f"""
        SELECT zona_id, {_AGG_COLUMNS}
        FROM trazabilidad_preventa
        WHERE dt_entrega BETWEEN :fi AND :ff
        -- FILTERS_HERE
        GROUP BY zona_id
        ORDER BY zona_id
    """,
    "producto": f"""
        SELECT producto_id, MAX(nmProducto) AS nmProducto, {_AGG_COLUMNS}
        FROM trazabilidad_preventa
        WHERE dt_entrega BETWEEN :fi AND :ff
        -- FILTERS_HERE
        GROUP BY producto_id
        ORDER BY producto_id
    """,
    "total": f"""
        SELECT 'TOTAL' AS nivel, {_AGG_COLUMNS}
        FROM trazabilidad_preventa
        WHERE dt_entrega BETWEEN :fi AND :ff
        -- FILTERS_HERE
    """,
}

# DDL de la tabla intermedia
_CREATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS trazabilidad_preventa (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    origen_registro VARCHAR(10) NULL,
    registro_id BIGINT NULL,
    pedido_id BIGINT NULL,
    ordcompra_id VARCHAR(50) NULL,
    establecimiento_id VARCHAR(50) NULL,
    nmPuntoVenta VARCHAR(200) NULL,
    zona_id VARCHAR(20) NULL,
    dt_pedido DATETIME NULL,
    dt_entrega DATETIME NULL,
    producto_id VARCHAR(50) NULL,
    nmProducto VARCHAR(200) NULL,
    tp_linea VARCHAR(10) NULL,
    bo_linea INT NULL,
    nbLinea INT NULL,
    nbVirtual VARCHAR(50) NULL,
    nbSustituido VARCHAR(50) NULL,
    cant_pedida_campo DECIMAL(18,4) DEFAULT 0,
    cant_cambios_campo DECIMAL(18,4) DEFAULT 0,
    cant_otros_cambios_campo DECIMAL(18,4) DEFAULT 0,
    cant_calidad_campo DECIMAL(18,4) DEFAULT 0,
    precio_campo DECIMAL(18,4) DEFAULT 0,
    vl_pedido_campo DECIMAL(18,4) DEFAULT 0,
    pct_descuento_campo DECIMAL(18,4) DEFAULT 0,
    cant_asignada_campo DECIMAL(18,4) DEFAULT 0,
    cant_solicitado DECIMAL(18,4) DEFAULT 0,
    costo_campo DECIMAL(18,4) DEFAULT 0,
    estado_tordenesmovil INT NULL,
    nro_factura_campo VARCHAR(50) NULL,
    nbPreplanilla VARCHAR(50) NULL,
    ordcompra_factura VARCHAR(50) NULL,
    nbFactura VARCHAR(50) NULL,
    dtContabilizacion DATETIME NULL,
    estado_pedido_factura INT NULL,
    nm_estado_pedido VARCHAR(100) NULL,
    nbPlanilla VARCHAR(50) NULL,
    idRuta VARCHAR(50) NULL,
    idListaPrecio VARCHAR(50) NULL,
    nbReglaNegFactura VARCHAR(100) NULL,
    vl_bruto_factura DECIMAL(18,4) DEFAULT 0,
    vl_neto_factura DECIMAL(18,4) DEFAULT 0,
    vl_total_a_pagar DECIMAL(18,4) DEFAULT 0,
    nbTotalItems INT NULL,
    txEstadoReg VARCHAR(50) NULL,
    estado_item INT NULL,
    nm_estado_item VARCHAR(100) NULL,
    cant_pedida_factura DECIMAL(18,4) DEFAULT 0,
    cant_reservada DECIMAL(18,4) DEFAULT 0,
    cant_asignada_factura DECIMAL(18,4) DEFAULT 0,
    cant_devuelta DECIMAL(18,4) DEFAULT 0,
    cant_desc_factura DECIMAL(18,4) DEFAULT 0,
    precio_factura DECIMAL(18,4) DEFAULT 0,
    costo_factura DECIMAL(18,4) DEFAULT 0,
    vl_bruto_item DECIMAL(18,4) DEFAULT 0,
    vl_descuentos_item DECIMAL(18,4) DEFAULT 0,
    vl_total_linea_item DECIMAL(18,4) DEFAULT 0,
    vl_iva_item DECIMAL(18,4) DEFAULT 0,
    vl_inc_item DECIMAL(18,4) DEFAULT 0,
    pct_descuento_item DECIMAL(18,4) DEFAULT 0,
    pct_iva_item DECIMAL(18,4) DEFAULT 0,
    nbAlmacen VARCHAR(50) NULL,
    nbMotivosFactura VARCHAR(200) NULL,
    idFuente VARCHAR(50) NULL,
    nbFuente VARCHAR(100) NULL,
    cant_faltante DECIMAL(18,4) DEFAULT 0,
    vl_faltante_cantidad DECIMAL(18,4) DEFAULT 0,
    dif_precio_unitario DECIMAL(18,4) DEFAULT 0,
    vl_diferencia_precio DECIMAL(18,4) DEFAULT 0,
    vl_brecha_total DECIMAL(18,4) DEFAULT 0,
    causa_brecha VARCHAR(50) NULL,
    bo_sin_factura TINYINT DEFAULT 0,
    bo_sin_item TINYINT DEFAULT 0,
    bo_faltante_total TINYINT DEFAULT 0,
    bo_faltante_parcial TINYINT DEFAULT 0,
    bo_dif_precio TINYINT DEFAULT 0,
    bo_tiene_nro_factura TINYINT DEFAULT 0,
    bo_es_hijo_virtual TINYINT DEFAULT 0,
    INDEX idx_trz_dt_entrega (dt_entrega),
    INDEX idx_trz_zona (zona_id),
    INDEX idx_trz_establecimiento (establecimiento_id),
    INDEX idx_trz_producto (producto_id),
    INDEX idx_trz_causa (causa_brecha)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


class TrazabilidadExtractor:
    """
    Motor ETL para el reporte de trazabilidad preventa vs facturación.

    Extrae datos de SIDIS (COSMOS) y los carga en la tabla intermedia
    `trazabilidad_preventa` en la BD BI. También genera Excel de salida.
    """

    def __init__(self, database_name, fecha_ini, fecha_fin, user_id, progress_callback=None):
        self.database_name = database_name
        self.fecha_ini = fecha_ini
        self.fecha_fin = fecha_fin
        self.user_id = user_id
        self.progress_callback = progress_callback
        self.start_time = time.time()

        self.config = {}
        self.engine_mysql_bi = None
        self.engine_mysql_out = None
        self.engine_sqlite = None
        self.sqlite_table_name = f"trz_{database_name}_{user_id}_{uuid.uuid4().hex[:8]}"
        self.file_path = None
        self.file_name = None
        self.total_records = 0

        self._update_progress("Inicializando", 1)
        self._configurar_conexiones()

    def _update_progress(self, stage, progress_percent, current_rec=None, total_rec=None):
        if self.progress_callback:
            try:
                safe_progress = max(0, min(100, int(progress_percent)))
                self.progress_callback(
                    stage, safe_progress,
                    current_rec if current_rec is not None else self.total_records,
                    total_rec if total_rec is not None else self.total_records,
                )
            except Exception as e:
                logger.warning(f"Error en progress_callback: {e}")

    def _configurar_conexiones(self):
        self._update_progress("Configurando conexiones", 2)
        config_basic = ConfigBasic(self.database_name, self.user_id)
        self.config = config_basic.config

        # Conexión a BD BI (destino)
        self.engine_mysql_bi = con.ConexionMariadb3(
            str(self.config["nmUsrIn"]),
            str(self.config["txPassIn"]),
            str(self.config["hostServerIn"]),
            int(self.config["portServerIn"]),
            str(self.config["dbBi"]),
        )

        # Conexión a BD SIDIS (fuente COSMOS)
        self.engine_mysql_out = con.ConexionMariadb3(
            str(self.config["nmUsrOut"]),
            str(self.config["txPassOut"]),
            str(self.config["hostServerOut"]),
            int(self.config["portServerOut"]),
            str(self.config["dbSidis"]),
        )
        logger.info("Conexiones BI y SIDIS configuradas.")

    def _crear_tabla_destino(self):
        self._update_progress("Creando tabla destino", 5)
        with self.engine_mysql_bi.begin() as conn:
            conn.execute(text(_CREATE_TABLE_DDL))
        logger.info("Tabla trazabilidad_preventa verificada/creada en BD BI.")

    def _limpiar_periodo(self):
        self._update_progress("Limpiando periodo anterior", 8)
        delete_sql = text(
            "DELETE FROM trazabilidad_preventa "
            "WHERE dt_entrega >= CONCAT(:fi, ' 00:00:00') "
            "  AND dt_entrega <= CONCAT(:ff, ' 23:59:59')"
        )
        with self.engine_mysql_bi.begin() as conn:
            result = conn.execute(delete_sql, {"fi": self.fecha_ini, "ff": self.fecha_fin})
            deleted = result.rowcount
        logger.info(f"Eliminados {deleted:,} registros del periodo {self.fecha_ini} - {self.fecha_fin}.")

    def _extraer_datos(self, chunksize=15000):
        self._update_progress("Extrayendo datos de SIDIS", 10)
        sql_text = SQL_FILE.read_text(encoding="utf-8")
        # Quitar ORDER BY para la extracción masiva (optimización)
        # El ORDER BY se aplica al consultar, no al cargar
        params = {"fi": self.fecha_ini, "ff": self.fecha_fin}

        total_inserted = 0
        max_retries = 3

        with self.engine_mysql_out.connect() as out_conn:
            result = out_conn.execution_options(stream_results=True).execute(
                text(sql_text), params
            )
            columns = list(result.keys())
            logger.info(f"Query ejecutada, {len(columns)} columnas. Extrayendo chunks...")

            rows = result.fetchmany(chunksize)
            if not rows:
                logger.info("La consulta no retornó datos.")
                return 0

            while rows:
                df_chunk = pd.DataFrame(rows, columns=columns)

                # NaN safety net (IEEE 754)
                records = df_chunk.astype(object).where(pd.notnull(df_chunk), None).to_dict(orient="records")
                for rec in records:
                    for k, v in rec.items():
                        if isinstance(v, float) and v != v:
                            rec[k] = None

                # Insertar en BD BI
                if records:
                    self._insertar_chunk(records, columns, max_retries)

                total_inserted += len(rows)
                self.total_records = total_inserted

                progress = min(10 + (total_inserted / max(total_inserted + chunksize, 1)) * 60, 70)
                self._update_progress(
                    f"Extrayendo... {total_inserted:,} registros",
                    progress, total_inserted,
                )

                if total_inserted % (chunksize * 10) == 0:
                    gc.collect()

                rows = result.fetchmany(chunksize)

        logger.info(f"Extracción completada: {total_inserted:,} registros.")
        return total_inserted

    def _insertar_chunk(self, records, columns, max_retries=3):
        # Excluir 'id' autoincremental si aparece en las columnas
        insert_cols = [c for c in columns if c != "id"]
        placeholders = ", ".join([f":{c}" for c in insert_cols])
        col_names = ", ".join([f"`{c}`" for c in insert_cols])
        insert_sql = f"INSERT INTO trazabilidad_preventa ({col_names}) VALUES ({placeholders})"

        # Filtrar solo las columnas válidas de cada record
        filtered = [{k: r.get(k) for k in insert_cols} for r in records]

        for attempt in range(max_retries):
            try:
                with self.engine_mysql_bi.begin() as conn:
                    conn.execute(text(insert_sql), filtered)
                return
            except OperationalError as e:
                logger.warning(f"Error inserción (intento {attempt + 1}/{max_retries}): {e}")
                try:
                    con.clear_connection_cache()
                except Exception:
                    pass
                time.sleep(2 * (attempt + 1))
            except SQLAlchemyError as e:
                logger.error(f"Error SQL insertando chunk: {e}")
                raise

        raise RuntimeError(f"No se pudo insertar chunk tras {max_retries} intentos.")

    def _generar_excel(self):
        self._update_progress("Generando archivo Excel", 75)

        self.file_name = (
            f"Trazabilidad_{self.database_name.upper()}"
            f"_de_{self.fecha_ini}_a_{self.fecha_fin}"
            f"_user_{self.user_id}.xlsx"
        )
        self.file_path = os.path.join("media", self.file_name)
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

        # Leer de BD BI
        query = text(
            "SELECT * FROM trazabilidad_preventa "
            "WHERE dt_entrega >= CONCAT(:fi, ' 00:00:00') "
            "  AND dt_entrega <= CONCAT(:ff, ' 23:59:59') "
            "ORDER BY zona_id, establecimiento_id, dt_pedido, producto_id"
        )
        params = {"fi": self.fecha_ini, "ff": self.fecha_fin}

        wb = Workbook(write_only=True)
        ws = wb.create_sheet(title="Trazabilidad")
        records_written = 0

        with self.engine_mysql_bi.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(query, params)
            headers = list(result.keys())
            ws.append(headers)

            while True:
                rows = result.fetchmany(10000)
                if not rows:
                    break
                for row in rows:
                    cleaned = tuple(
                        TextCleaner.clean_for_excel(v) if isinstance(v, str) else v
                        for v in row
                    )
                    ws.append(cleaned)
                records_written += len(rows)
                progress = 75 + (records_written / max(self.total_records, 1)) * 20
                self._update_progress(
                    f"Escribiendo Excel... {records_written:,}",
                    min(progress, 95), records_written,
                )

        wb.save(self.file_path)
        logger.info(f"Excel generado: {self.file_path} ({records_written:,} filas)")
        self._update_progress("Archivo generado", 97, records_written)

    def run(self):
        """Ejecuta el proceso ETL completo."""
        try:
            self._crear_tabla_destino()
            self._limpiar_periodo()
            total = self._extraer_datos()

            if total == 0:
                self._update_progress("Sin datos", 100, 0, 0)
                return {
                    "success": False,
                    "message": "No se encontraron datos de trazabilidad para el periodo.",
                    "file_path": None,
                    "file_name": None,
                    "execution_time": time.time() - self.start_time,
                    "metadata": {"total_records": 0},
                }

            self._generar_excel()

            execution_time = time.time() - self.start_time
            self._update_progress("Completado", 100, self.total_records)
            logger.info(f"Trazabilidad ETL completado en {execution_time:.2f}s. {self.total_records:,} registros.")

            return {
                "success": True,
                "message": f"Trazabilidad generada en {execution_time:.2f}s ({self.total_records:,} registros).",
                "file_path": self.file_path,
                "file_name": self.file_name,
                "execution_time": execution_time,
                "metadata": {"total_records": self.total_records},
            }

        except Exception as e:
            execution_time = time.time() - self.start_time
            error_msg = f"Error en TrazabilidadExtractor.run: {type(e).__name__} - {e}"
            logger.error(error_msg, exc_info=True)
            self._update_progress(f"Error: {e}", 100)
            return {
                "success": False,
                "message": error_msg,
                "file_path": None,
                "file_name": None,
                "execution_time": execution_time,
                "metadata": {"total_records": self.total_records},
            }

    # ------------------------------------------------------------------
    # Métodos estáticos para consulta desde views (AJAX)
    # ------------------------------------------------------------------

    @staticmethod
    def get_kpis(database_name, fecha_ini, fecha_fin, user_id):
        """Calcula KPIs desde la tabla trazabilidad_preventa en BD BI."""
        config = ConfigBasic(database_name, user_id).config
        engine = con.ConexionMariadb3(
            str(config["nmUsrIn"]), str(config["txPassIn"]),
            str(config["hostServerIn"]), int(config["portServerIn"]),
            str(config["dbBi"]),
        )
        kpi_sql = text("""
            SELECT
                COUNT(*)                                     AS total_registros,
                SUM(bo_faltante_total)                       AS agotados_total,
                SUM(bo_faltante_parcial)                     AS agotados_parcial,
                ROUND(SUM(bo_faltante_total) * 100.0
                      / NULLIF(COUNT(*), 0), 2)              AS pct_agotamiento,
                ROUND(SUM(vl_brecha_total), 2)               AS valor_brecha_total,
                ROUND(SUM(vl_faltante_cantidad), 2)          AS valor_faltante_cantidad,
                ROUND(SUM(vl_pedido_campo), 2)               AS valor_pedido_total,
                COUNT(DISTINCT producto_id)                   AS productos_unicos,
                COUNT(DISTINCT establecimiento_id)            AS clientes_unicos,
                COUNT(DISTINCT zona_id)                       AS zonas_unicas
            FROM trazabilidad_preventa
            WHERE dt_entrega >= CONCAT(:fi, ' 00:00:00')
              AND dt_entrega <= CONCAT(:ff, ' 23:59:59')
        """)
        with engine.connect() as conn:
            row = conn.execute(kpi_sql, {"fi": fecha_ini, "ff": fecha_fin}).fetchone()

        if not row:
            return {}

        return {
            "total_registros": row[0] or 0,
            "agotados_total": row[1] or 0,
            "agotados_parcial": row[2] or 0,
            "pct_agotamiento": float(row[3] or 0),
            "valor_brecha_total": float(row[4] or 0),
            "valor_faltante_cantidad": float(row[5] or 0),
            "valor_pedido_total": float(row[6] or 0),
            "productos_unicos": row[7] or 0,
            "clientes_unicos": row[8] or 0,
            "zonas_unicas": row[9] or 0,
        }

    @staticmethod
    def get_data(database_name, fecha_ini, fecha_fin, user_id,
                 agrupacion="detalle", start=0, length=100, search=""):
        """
        Consulta datos para DataTables server-side.

        Returns:
            dict con keys: headers, rows, total_records, filtered_records
        """
        config = ConfigBasic(database_name, user_id).config
        engine = con.ConexionMariadb3(
            str(config["nmUsrIn"]), str(config["txPassIn"]),
            str(config["hostServerIn"]), int(config["portServerIn"]),
            str(config["dbBi"]),
        )

        params = {"fi": fecha_ini, "ff": fecha_fin}

        if agrupacion == "detalle":
            base_sql = (
                "SELECT * FROM trazabilidad_preventa "
                "WHERE dt_entrega >= CONCAT(:fi, ' 00:00:00') "
                "  AND dt_entrega <= CONCAT(:ff, ' 23:59:59')"
            )
            count_sql = (
                "SELECT COUNT(*) FROM trazabilidad_preventa "
                "WHERE dt_entrega >= CONCAT(:fi, ' 00:00:00') "
                "  AND dt_entrega <= CONCAT(:ff, ' 23:59:59')"
            )
        else:
            group_query = _GROUPING_QUERIES.get(agrupacion)
            if not group_query:
                group_query = _GROUPING_QUERIES["total"]
            base_sql = group_query.replace("-- FILTERS_HERE", "")
            # Para count de agrupados, envolver en subquery
            count_sql = f"SELECT COUNT(*) FROM ({base_sql}) AS sub"

        # Search filter
        search_clause = ""
        if search and agrupacion == "detalle":
            search_clause = (
                " AND (nmPuntoVenta LIKE :search "
                "OR producto_id LIKE :search "
                "OR nmProducto LIKE :search "
                "OR causa_brecha LIKE :search "
                "OR zona_id LIKE :search)"
            )
            params["search"] = f"%{search}%"

        # Count total (sin filtro de search)
        with engine.connect() as conn:
            total_records = conn.execute(text(count_sql), {"fi": fecha_ini, "ff": fecha_fin}).scalar() or 0

        # Count filtrado (con search)
        if search_clause:
            filtered_count_sql = count_sql + search_clause
            with engine.connect() as conn:
                filtered_records = conn.execute(text(filtered_count_sql), params).scalar() or 0
        else:
            filtered_records = total_records

        # Data query con paginación
        data_sql = base_sql + search_clause + f" LIMIT {int(length)} OFFSET {int(start)}"

        with engine.connect() as conn:
            result = conn.execute(text(data_sql), params)
            headers = list(result.keys())
            rows = [dict(zip(headers, row)) for row in result.fetchall()]

        return {
            "headers": headers,
            "rows": rows,
            "total_records": total_records,
            "filtered_records": filtered_records,
        }
