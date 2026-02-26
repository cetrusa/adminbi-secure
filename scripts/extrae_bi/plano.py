import os
import pandas as pd
from sqlalchemy import create_engine, text
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
import logging
from scripts.StaticPage import StaticPage
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import ast
import xlsxwriter
import zipfile
from zipfile import ZipFile
import psutil
import time
import gc

# Configuración del logging
logging.basicConfig(
    filename="logInterface.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)


class InterfacePlano:
    """
    Clase para la generación de archivos planos comprimidos en ZIP, similar en estructura a Interface.
    """

    def __init__(
        self,
        database_name,
        IdtReporteIni,
        IdtReporteFin,
        user_id=None,
        reporte_id=None,
        progress_callback=None,
    ):
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.user_id = user_id
        self.reporte_id = reporte_id
        self.config = None
        self.engine_mysql = None
        self.engine_sqlite = None
        self.file_path = None
        self.archivo_plano = None
        self.progress_callback = progress_callback
        self._setup()

    def _setup(self):
        config_basic = ConfigBasic(self.database_name)
        self.config = config_basic.config
        self.engine_mysql = con.ConexionMariadb3(
            str(self.config.get("nmUsrIn")),
            str(self.config.get("txPassIn")),
            str(self.config.get("hostServerIn")),
            int(self.config.get("portServerIn")),
            str(self.config.get("dbBi")),
        )
        import os

        db_path = os.path.join("media", "mydata.db")
        self.engine_sqlite = con.ConexionSqlite(db_path)

    def _generate_sql(self, hoja, proc_key):
        sql = self.config[proc_key]
        if self.config["dbBi"] == "powerbi_tym_eje":
            return text(
                f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','','{str(hoja)}',0,0,0);"
            )
        return text(
            f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','','{str(hoja)}');"
        )

    def _guardar_datos_csv(
        self,
        table_name,
        buffer,
        sep="|",
        float_fmt="%.2f",
        header=True,
        hoja=None,
        total_records=None,
    ):
        chunksize = 50000
        processed = 0
        start_export_time = time.time()
        last_progress = 0
        for chunk in pd.read_sql_query(
            f"SELECT * FROM {table_name}", self.engine_sqlite, chunksize=chunksize
        ):
            if not chunk.empty:
                chunk.to_csv(
                    buffer, sep=sep, index=False, float_format=float_fmt, header=header
                )
                processed += len(chunk)
                if self.progress_callback and total_records:
                    percent = min(99, int((processed / total_records) * 100))
                    # Asegura que la barra de progreso nunca retroceda
                    if percent < last_progress:
                        percent = last_progress
                    else:
                        last_progress = percent
                    logger.info(
                        f"[PLANO] CSV: {processed} registros escritos, progreso: {percent}%, memoria: {psutil.Process(os.getpid()).memory_info().rss / (1024*1024):.1f} MB, tiempo: {time.time() - start_export_time:.2f}s"
                    )
                    self.progress_callback(
                        f"Procesando hoja {hoja}", percent, processed, total_records
                    )
                gc.collect()

    def _ejecutar_query_mysql_chunked(self, query, table_name, chunksize=50000):
        # Elimina la tabla si existe
        with self.engine_sqlite.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            # Desactivar restricciones y optimizar PRAGMAs para inserción masiva
            conn.execute(text("PRAGMA journal_mode = MEMORY;"))
            conn.execute(text("PRAGMA synchronous = OFF;"))
            conn.execute(text("PRAGMA cache_size = -100000;"))
            conn.execute(text("PRAGMA temp_store = MEMORY;"))
            # Inserción masiva SIN transacción explícita (evita error de transacción anidada)
            with self.engine_mysql.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")
                for chunk in pd.read_sql_query(query, con=cursor, chunksize=chunksize):
                    chunk.to_sql(
                        name=table_name,
                        con=conn,
                        if_exists="append",
                        index=False,
                        method="multi",  # Inserta múltiples filas por statement
                        chunksize=1000,   # Ajusta según memoria
                    )
        # Crear índice solo si existe una columna de fecha relevante
        with self.engine_sqlite.connect() as conn:
            try:
                columns_info = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
                column_names = [col[1] for col in columns_info]
                posibles_fechas = ["dtContabilizacion", "fecha", "date", "dtFecha", "created_at"]
                fecha_col = next((col for col in posibles_fechas if col in column_names), None)
                if fecha_col:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{fecha_col} ON {table_name}({fecha_col});"))
                    logger.info(f"Índice creado en {table_name} sobre {fecha_col}")
                else:
                    logger.info(f"No se encontró columna de fecha para indexar en {table_name}")
            except Exception as e:
                logger.warning(f"No se pudo crear índice en {table_name}: {e}")
            total_records = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).fetchone()[0]
        return total_records

    def _call_progress(
        self,
        stage,
        percent,
        current_rec=None,
        total_rec=None,
        hoja_idx=None,
        total_hojas=None,
        status=None,
        meta=None,
        **kwargs,
    ):
        if self.progress_callback:
            cb_kwargs = {}
            if hoja_idx is not None:
                cb_kwargs["hoja_idx"] = hoja_idx
            if total_hojas is not None:
                cb_kwargs["total_hojas"] = total_hojas
            if status is not None:
                cb_kwargs["status"] = status
            if meta is not None:
                cb_kwargs["meta"] = meta
            cb_kwargs.update(kwargs)
            self.progress_callback(stage, percent, current_rec, total_rec, **cb_kwargs)

    def _procesar_hoja(
        self,
        hoja,
        buffer,
        proc_key,
        sep,
        float_fmt,
        header,
        hoja_idx=None,
        total_hojas=None,
    ):
        try:
            print(f"Procesando hoja: {hoja}")
            # Progreso inicial > 0 para mejor UX
            self._call_progress(
                f"Iniciando hoja {hoja}", 5, hoja_idx=hoja_idx, total_hojas=total_hojas
            )
            sqlout = self._generate_sql(hoja, proc_key)
            print(f"SQL generado para hoja {hoja}: {sqlout}")
            table_name = f"my_table_{self.database_name}_{hoja}"
            total_records = self._ejecutar_query_mysql_chunked(sqlout, table_name)
            print(f"Total records para hoja {hoja}: {total_records}")
            if total_records == 0:
                print(f"Hoja {hoja} sin datos")
                self._call_progress(
                    f"Hoja {hoja} sin datos",
                    100,
                    0,
                    0,
                    hoja_idx=hoja_idx,
                    total_hojas=total_hojas,
                    status="no_data",
                )
                return {
                    "success": False,
                    "error_message": f"No hay datos para la hoja {hoja}",
                }
            # Progreso intermedio antes de guardar CSV
            self._call_progress(
                f"Preparando exportación hoja {hoja}",
                50,
                hoja_idx=hoja_idx,
                total_hojas=total_hojas,
            )
            self._guardar_datos_csv(
                table_name,
                buffer,
                sep=sep,
                float_fmt=float_fmt,
                header=header,
                hoja=hoja,
                total_records=total_records,
            )
            with self.engine_sqlite.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f"Finalizada hoja {hoja}")
            self._call_progress(
                f"Finalizada hoja {hoja}",
                100,
                total_records,
                total_records,
                hoja_idx=hoja_idx,
                total_hojas=total_hojas,
                status="success",
            )
            return True
        except Exception as e:
            print(f"Error al procesar la hoja {hoja}: {e}")
            logging.error(f"Error al procesar la hoja {hoja}: {e}")
            self._call_progress(
                f"Error en hoja {hoja}",
                100,
                0,
                0,
                hoja_idx=hoja_idx,
                total_hojas=total_hojas,
                status="failed",
                meta={"error_message": str(e)},
            )
            return {
                "success": False,
                "error_message": f"Error al procesar la hoja {hoja}: {e}",
            }

    def run(self):
        hojas1 = self._obtener_lista_hojas("txProcedureCsv")
        hojas2 = self._obtener_lista_hojas("txProcedureCsv2")
        total_hojas = len(hojas1) if hojas1 else len(hojas2)
        self._call_progress(
            "Iniciando generación de plano", 0, hoja_idx=0, total_hojas=total_hojas
        )
        if not hojas1 and not hojas2:
            self._call_progress(
                "No hay hojas configuradas",
                100,
                hoja_idx=0,
                total_hojas=0,
                status="failed",
                meta={"error_message": "La empresa no maneja planos"},
            )
            return {
                "success": False,
                "error_message": "La empresa no maneja planos",
                "metadata": {"total_hojas": 0, "hojas_con_datos": 0},
            }
        if hojas1:
            result, hojas_con_datos = self._procesar(
                hojas1,
                "nmProcedureCsv",
                sep="|",
                float_fmt="%.2f",
                header=True,
                total_hojas=total_hojas,
            )
        else:
            result, hojas_con_datos = self._procesar(
                hojas2,
                "nmProcedureCsv2",
                sep=",",
                float_fmt="%.0f",
                header=False,
                total_hojas=total_hojas,
            )
        self._call_progress(
            "Plano finalizado",
            100,
            hoja_idx=total_hojas,
            total_hojas=total_hojas,
            status="completed",
        )
        # Agregar metadata relevante
        if isinstance(result, dict):
            result["metadata"] = {
                "total_hojas": total_hojas,
                "hojas_con_datos": hojas_con_datos,
            }
        return result

    def _procesar(self, hojas, proc_key, sep, float_fmt, header, total_hojas):
        self._generar_nombre_archivo()
        hoja_idx = 0
        hojas_con_datos = 0
        with zipfile.ZipFile(self.file_path, "w") as zf:
            for hoja in hojas:
                hoja_idx += 1
                with zf.open(hoja + ".txt", "w") as buffer:
                    result = self._procesar_hoja(
                        hoja,
                        buffer,
                        proc_key,
                        sep,
                        float_fmt,
                        header,
                        hoja_idx=hoja_idx,
                        total_hojas=total_hojas,
                    )
                    if result is not True:
                        if isinstance(result, dict) and "No hay datos" in result.get(
                            "error_message", ""
                        ):
                            continue
                        return result, hojas_con_datos
                    else:
                        hojas_con_datos += 1
                self._call_progress(
                    f"Progreso global: {hoja_idx}/{total_hojas} hojas",
                    int((hoja_idx / total_hojas) * 100),
                    hoja_idx=hoja_idx,
                    total_hojas=total_hojas,
                )
        if hojas_con_datos == 0:
            self._call_progress(
                "No se encontraron datos en ninguna hoja",
                100,
                0,
                0,
                status="failed",
                meta={
                    "stage": "No se encontraron datos en ninguna hoja",
                    "error_message": "No hay datos para mostrar en ninguna hoja.",
                },
                hoja_idx=hoja_idx,
                total_hojas=total_hojas,
            )
            return {
                "success": False,
                "error_message": "No hay datos para mostrar en ninguna hoja.",
                "file_path": None,
                "file_name": None,
            }, hojas_con_datos
        return {
            "success": True,
            "file_path": self.file_path,
            "file_name": self.archivo_plano,
        }, hojas_con_datos

    def _obtener_lista_hojas(self, key):
        """
        Devuelve la lista de hojas a procesar a partir de la configuración,
        manejando tanto listas como strings serializadas.
        """
        hojas = self.config.get(key)
        if not hojas:
            return []
        if isinstance(hojas, list):
            return [str(h).strip() for h in hojas if str(h).strip()]
        if isinstance(hojas, str):
            try:
                # Intentar evaluar como lista
                hojas_eval = ast.literal_eval(hojas)
                if isinstance(hojas_eval, list):
                    return [str(h).strip() for h in hojas_eval if str(h).strip()]
                # Si no es lista, tratar como string separada por comas
                return [s.strip() for s in hojas.split(",") if s.strip()]
            except Exception:
                # Si falla, tratar como string separada por comas
                return [s.strip() for s in hojas.split(",") if s.strip()]
        return []

    def _generar_nombre_archivo(self):
        """
        Genera el nombre y la ruta del archivo plano ZIP de salida y los asigna a self.file_path y self.archivo_plano.
        Incluye user_id y reporte_id si están disponibles para mayor unicidad.
        """
        ext = ".zip"
        user_part = f"_user_{self.user_id}" if self.user_id is not None else ""
        reporte_part = (
            f"_reporte_{self.reporte_id}" if self.reporte_id is not None else ""
        )
        nombre = f"Plano_{self.database_name}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}{user_part}{reporte_part}{ext}"
        ruta = os.path.join("media", nombre)
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        self.file_path = ruta
        self.archivo_plano = nombre
