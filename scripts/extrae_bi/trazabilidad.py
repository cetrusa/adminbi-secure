# scripts/extrae_bi/trazabilidad.py
import os
import time
import logging

from sqlalchemy import text
from openpyxl import Workbook

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from scripts.text_cleaner import TextCleaner

logger = logging.getLogger(__name__)

# Columnas para cada nivel de agrupacion
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
        SELECT z.macrozona_id, MAX(z.macro) AS nmMacrozona, {_AGG_COLUMNS}
        FROM trazabilidad_preventa tp
        INNER JOIN zona z ON tp.zona_id = z.zona_id
        WHERE tp.dt_entrega BETWEEN :fi AND :ff
        -- FILTERS_HERE
        GROUP BY z.macrozona_id
        ORDER BY z.macrozona_id
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


class TrazabilidadExtractor:
    """
    Reporteador de trazabilidad preventa vs facturacion.

    Lee datos de la tabla `trazabilidad_preventa` en la BD BI
    (poblada por el ETL de preventa) y genera Excel / KPIs / datos AJAX.
    """

    def __init__(self, database_name, fecha_ini, fecha_fin, user_id, progress_callback=None, batch_size=15000):
        self.database_name = database_name
        self.fecha_ini = fecha_ini
        self.fecha_fin = fecha_fin
        self.user_id = user_id
        self.progress_callback = progress_callback
        self.batch_size = max(1000, int(batch_size))
        self.start_time = time.time()

        self.config = {}
        self.engine_mysql_bi = None
        self.file_path = None
        self.file_name = None
        self.total_records = 0

        self._update_progress("Inicializando", 1)
        self._configurar_conexion()

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

    def _configurar_conexion(self):
        self._update_progress("Configurando conexion", 2)
        config_basic = ConfigBasic(self.database_name, self.user_id)
        self.config = config_basic.config

        # Conexion a BD BI (donde vive trazabilidad_preventa)
        self.engine_mysql_bi = con.ConexionMariadb3(
            str(self.config["nmUsrIn"]),
            str(self.config["txPassIn"]),
            str(self.config["hostServerIn"]),
            int(self.config["portServerIn"]),
            str(self.config["dbBi"]),
        )
        logger.info("Conexion BI configurada para trazabilidad.")

    def _contar_registros(self):
        """Cuenta registros existentes en trazabilidad_preventa para el periodo."""
        self._update_progress("Verificando datos", 10)
        count_sql = text(
            "SELECT COUNT(*) FROM trazabilidad_preventa "
            "WHERE dt_entrega BETWEEN :fi AND :ff"
        )
        with self.engine_mysql_bi.connect() as conn:
            total = conn.execute(
                count_sql, {"fi": self.fecha_ini, "ff": self.fecha_fin}
            ).scalar() or 0
        self.total_records = total
        logger.info(f"Registros encontrados en trazabilidad_preventa: {total:,}")
        return total

    def _generar_excel(self):
        self._update_progress("Generando archivo Excel", 30)

        self.file_name = (
            f"Trazabilidad_{self.database_name.upper()}"
            f"_de_{self.fecha_ini}_a_{self.fecha_fin}"
            f"_user_{self.user_id}.xlsx"
        )
        self.file_path = os.path.join("media", self.file_name)
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

        query = text(
            "SELECT * FROM trazabilidad_preventa "
            "WHERE dt_entrega BETWEEN :fi AND :ff "
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
                rows = result.fetchmany(self.batch_size)
                if not rows:
                    break
                for row in rows:
                    cleaned = tuple(
                        TextCleaner.clean_for_excel(v) if isinstance(v, str) else v
                        for v in row
                    )
                    ws.append(cleaned)
                records_written += len(rows)
                progress = 30 + (records_written / max(self.total_records, 1)) * 65
                self._update_progress(
                    f"Escribiendo Excel... {records_written:,}",
                    min(progress, 95), records_written,
                )

        wb.save(self.file_path)
        logger.info(f"Excel generado: {self.file_path} ({records_written:,} filas)")
        self._update_progress("Archivo generado", 97, records_written)

    def run(self):
        """Genera reporte Excel desde la tabla trazabilidad_preventa."""
        try:
            total = self._contar_registros()

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
            logger.info(f"Trazabilidad completada en {execution_time:.2f}s. {self.total_records:,} registros.")

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
    # Metodos estaticos para consulta desde views (AJAX)
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

        # Data query con paginacion
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
