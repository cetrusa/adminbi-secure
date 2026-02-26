import os
import pandas as pd
import time
import gc
import logging
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from openpyxl import Workbook
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import ast
import psutil

logger = logging.getLogger(__name__)


class InterfaceContable:
    """
    Clase para manejar la generación de la Interface Contable, con soporte para tareas asíncronas y progreso.
    """

    def __init__(
        self,
        database_name,
        IdtReporteIni,
        IdtReporteFin,
        user_id,
        reporte_id,
        progress_callback=None,  # Añadido callback
    ):
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.user_id = user_id
        self.progress_callback = progress_callback
        self.start_time = time.time()
        self.config = {}
        self.engine_mysql = None
        self.engine_sqlite = None
        self.sqlite_table_name = f"interface_{self.database_name}_{self.user_id or 'nouser'}_{uuid.uuid4().hex[:8]}"
        self.file_path = None
        self.file_name = None
        self.total_records_processed = 0
        self.total_records_estimate = 0
        self._update_progress("Inicializando", 1)
        try:
            self._configurar_conexiones()
            self._create_sqlite_engine()
        except Exception as e:
            logger.error(
                f"Error crítico durante la inicialización de InterfaceContable: {e}",
                exc_info=True,
            )
            self._update_progress(f"Error inicialización: {e}", 100)
            raise

    def _update_progress(
        self, stage, progress_percent, current_rec=None, total_rec=None
    ):
        if self.progress_callback:
            try:
                total = (
                    total_rec if total_rec is not None else self.total_records_estimate
                )
                current = (
                    current_rec
                    if current_rec is not None
                    else self.total_records_processed
                )
                safe_progress = max(0, min(100, int(progress_percent)))
                self.progress_callback(stage, safe_progress, current, total)
                logger.debug(
                    f"Progreso: {stage} - {safe_progress}% ({current:,}/{total:,})"
                )
            except Exception as e:
                logger.warning(f"Error al llamar progress_callback: {e}", exc_info=True)
        else:
            logger.debug(f"Progreso (sin callback): {stage} - {progress_percent}%")

    def _configurar_conexiones(self):
        self._update_progress("Configurando conexiones", 2)
        try:
            config_basic = ConfigBasic(self.database_name, self.user_id)
            self.config = config_basic.config
            required_keys = [
                "nmUsrIn",
                "txPassIn",
                "hostServerIn",
                "portServerIn",
                "dbBi",
            ]
            if not all(self.config.get(key) for key in required_keys):
                raise ValueError(
                    "Configuración de conexión a MySQL/MariaDB incompleta."
                )
            self.engine_mysql = con.ConexionMariadb3(
                str(self.config["nmUsrIn"]),
                str(self.config["txPassIn"]),
                str(self.config["hostServerIn"]),
                int(self.config["portServerIn"]),
                str(self.config["dbBi"]),
            )
        except Exception as e:
            logger.error(f"Error al configurar conexiones: {e}", exc_info=True)
            raise

    def _create_sqlite_engine(self):
        self._update_progress("Configurando BD temporal", 3)
        try:
            # Usar archivo temporal único en 'media' para depuración de permisos
            import uuid

            sqlite_table_name = getattr(
                self, "sqlite_table_name", f"interface_{uuid.uuid4().hex[:8]}"
            )
            sqlite_path = os.path.join("media", f"temp_{sqlite_table_name}.db")
            os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
            self.engine_sqlite = create_engine(f"sqlite:///{sqlite_path}")
            with self.engine_sqlite.connect() as conn:
                conn.execute(text("PRAGMA journal_mode = MEMORY;"))
                conn.execute(text("PRAGMA synchronous = OFF;"))
                conn.execute(text("PRAGMA cache_size = -100000;"))
                conn.execute(text("PRAGMA temp_store = MEMORY;"))
        except Exception as e:
            logger.error(f"Error creando motor SQLite: {e}", exc_info=True)
            raise

    def _generate_sqlout(self, hoja):
        sql = self.config.get("nmProcedureInterface")
        if not sql:
            raise ValueError("Esta empresa no maneja Interface")
        if self.config["dbBi"] == "powerbi_tym_eje":
            return text(
                f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','','{str(hoja)}',0,0,0);"
            )
        return text(
            f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','','{str(hoja)}');"
        )

    def _execute_query_to_sqlite(self, query, table_name, chunksize=50000):
        stage_name = "Extrayendo datos de MySQL"
        self._update_progress(stage_name, 10)
        total_processed = 0
        start_extract_time = time.time()
        first_chunk = True
        try:
            with self.engine_mysql.connect() as mysql_conn:
                # Primero verificar si la consulta devuelve resultados
                print(
                    f"[InterfaceContable] Ejecutando consulta MySQL para tabla: {table_name}"
                )
                result_proxy = mysql_conn.execute(query)
                columns = result_proxy.keys()

                # Obtener el primer chunk para verificar si hay datos
                rows = result_proxy.fetchmany(chunksize)
                if not rows:
                    print(
                        f"[InterfaceContable] No hay datos para la tabla: {table_name}"
                    )
                    logger.warning(f"No hay datos para la tabla: {table_name}")
                    self.total_records_processed = 0
                    return

                # Si hay datos, proceder con la creación de la tabla SQLite
                with self.engine_sqlite.connect() as sqlite_conn, sqlite_conn.begin():
                    df_chunk = pd.DataFrame(rows, columns=columns)
                    print(
                        f"[InterfaceContable] Creando tabla SQLite '{table_name}' con {len(columns)} columnas y {len(rows)} filas iniciales"
                    )

                    # Crear tabla con el primer chunk
                    df_chunk.to_sql(
                        name=table_name,
                        con=sqlite_conn,
                        if_exists="replace",
                        index=False,
                        method=None,
                    )

                    # Actualizar contadores
                    total_processed = len(rows)
                    self.total_records_processed = total_processed

                    # Procesar resto de chunks si hay más datos
                    while True:
                        rows = result_proxy.fetchmany(chunksize)
                        if not rows:
                            break
                        df_chunk = pd.DataFrame(rows, columns=columns)
                        df_chunk.to_sql(
                            name=table_name,
                            con=sqlite_conn,
                            if_exists="append",
                            index=False,
                            method="multi",
                            chunksize=1000,
                        )
                        total_processed += len(rows)
                        self.total_records_processed = total_processed
                        elapsed_total = time.time() - start_extract_time
                        progress_percent = (
                            10 + (elapsed_total / (elapsed_total + 1)) * 70
                        )
                        self._update_progress(
                            stage_name, progress_percent, total_processed
                        )
                        del df_chunk
                        if total_processed % (chunksize * 5) == 0:
                            gc.collect()

                # Verificar que la tabla se ha creado y confirmar el recuento final
                with self.engine_sqlite.connect() as sqlite_conn:
                    # Verificar que la tabla existe
                    table_exists = sqlite_conn.execute(
                        text(
                            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                        )
                    ).fetchone()

                    if not table_exists:
                        raise ValueError(
                            f"La tabla {table_name} no se creó correctamente en SQLite"
                        )

                    # Confirmar recuento de registros
                    final_count = sqlite_conn.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    ).scalar()

                    print(
                        f"[InterfaceContable] Tabla {table_name} creada con {final_count} registros"
                    )

                    if final_count != total_processed:
                        logger.warning(
                            f"Discrepancia en conteo para {table_name}: procesados {total_processed}, SQLite tiene {final_count}"
                        )
                        self.total_records_processed = final_count

                self._update_progress(
                    "Datos extraídos a BD temporal", 80, self.total_records_processed
                )
        except SQLAlchemyError as e:
            logger.error(
                f"Error de base de datos durante la extracción para {table_name}: {e}",
                exc_info=True,
            )
            print(f"[InterfaceContable] Error de BD para {table_name}: {e}")
            self._update_progress(f"Error BD en {table_name}: {e}", 100)
            # Asegurar que la tabla se elimina si hay error
            try:
                with self.engine_sqlite.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            except:
                pass
            raise
        except Exception as e:
            logger.error(
                f"Error inesperado durante la extracción para {table_name}: {e}",
                exc_info=True,
            )
            print(f"[InterfaceContable] Error extraer {table_name}: {e}")
            self._update_progress(f"Error en {table_name}: {e}", 100)
            # Asegurar que la tabla se elimina si hay error
            try:
                with self.engine_sqlite.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            except:
                pass
            raise
        except SQLAlchemyError as e:
            logger.error(
                f"Error de base de datos durante la extracción: {e}", exc_info=True
            )
            self._update_progress(f"Error BD: {e}", 100)
            raise
        except Exception as e:
            logger.error(f"Error inesperado durante la extracción: {e}", exc_info=True)
            self._update_progress(f"Error extracción: {e}", 100)
            raise

    def _generate_output_file(self, hoja_nombre, chunksize=50000):
        stage_name = "Generando archivo de salida"
        self._update_progress(stage_name, 85, self.total_records_processed)
        use_csv = self.total_records_processed > 1000000
        ext = ".csv" if use_csv else ".xlsx"
        # Incluir user_id y reporte_id para unicidad y trazabilidad
        reporte_id_str = (
            f"_reporte_{self.reporte_id}"
            if hasattr(self, "reporte_id") and self.reporte_id
            else ""
        )
        user_id_str = f"_user_{self.user_id}" if self.user_id else ""
        self.file_name = f"Interface_Contable_{self.database_name}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}{user_id_str}{reporte_id_str}{ext}"
        self.file_path = os.path.join("media", self.file_name)
        # Mejora: Validar que la ruta no sea vacía y crear carpeta destino
        output_dir = os.path.dirname(self.file_path)
        if not output_dir:
            raise ValueError("La ruta de destino para el archivo está vacía.")
        os.makedirs(output_dir, exist_ok=True)
        temp_path = self.file_path + ".tmp"
        start_export_time = time.time()
        records_written = 0
        try:
            with self.engine_sqlite.connect() as sqlite_conn:
                headers_result = sqlite_conn.execute(
                    text(f"PRAGMA table_info({self.sqlite_table_name})")
                ).fetchall()
                header_names = [col[1] for col in headers_result]
                if use_csv:
                    pd.DataFrame(columns=header_names).to_csv(
                        temp_path, index=False, encoding="utf-8-sig"
                    )
                    for chunk_df in pd.read_sql_query(
                        f"SELECT * FROM {self.sqlite_table_name}",
                        sqlite_conn,
                        chunksize=chunksize,
                    ):
                        chunk_df.to_csv(
                            temp_path,
                            mode="a",
                            header=False,
                            index=False,
                            encoding="utf-8-sig",
                        )
                        records_written += len(chunk_df)
                        progress_percent = (
                            85 + (records_written / self.total_records_processed * 14)
                            if self.total_records_processed > 0
                            else 99
                        )
                        logger.info(f"[INTERFACE] CSV: {records_written} registros escritos, progreso: {progress_percent:.2f}%, memoria: {psutil.Process(os.getpid()).memory_info().rss / (1024*1024):.1f} MB, tiempo: {time.time() - start_export_time:.2f}s")
                        self._update_progress(
                            f"{stage_name} (CSV)", progress_percent, records_written
                        )
                        del chunk_df
                        gc.collect()
                else:
                    wb = Workbook(write_only=True)
                    ws = wb.create_sheet(title=hoja_nombre)
                    ws.append(header_names)
                    cursor = sqlite_conn.execute(
                        text(f"SELECT * FROM {self.sqlite_table_name}")
                    )
                    while True:
                        rows = cursor.fetchmany(chunksize)
                        if not rows:
                            break
                        for row in rows:
                            ws.append(tuple(row))
                        records_written += len(rows)
                        progress_percent = (
                            85 + (records_written / self.total_records_processed * 14)
                            if self.total_records_processed > 0
                            else 99
                        )
                        logger.info(f"[INTERFACE] Excel: {records_written} registros escritos, progreso: {progress_percent:.2f}%, memoria: {psutil.Process(os.getpid()).memory_info().rss / (1024*1024):.1f} MB, tiempo: {time.time() - start_export_time:.2f}s")
                        self._update_progress(
                            f"{stage_name} (Excel)", progress_percent, records_written
                        )
                    wb.save(temp_path)
            # Atomicidad: renombrar archivo temporal solo si todo fue exitoso
            os.replace(temp_path, self.file_path)
            self._update_progress("Archivo generado", 99, records_written)
        except Exception as e:
            logger.error(f"Error generando archivo de salida: {e}", exc_info=True)
            self._update_progress(f"Error archivo: {e}", 100)
            # Eliminar archivo temporal si existe
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            self.file_path = None
            raise

    def _cleanup(self):
        try:
            if self.engine_sqlite:
                with self.engine_sqlite.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self.sqlite_table_name}"))
        except Exception as e:
            logger.warning(f"Error durante la limpieza de SQLite: {e}", exc_info=True)

    def run(self):
        print(
            "[InterfaceContable] INICIO del proceso de generación de interface contable"
        )
        logger.info(
            "[InterfaceContable] INICIO del proceso de generación de interface contable"
        )
        try:
            print(f"[InterfaceContable] Configuración cargada: {self.config}")
            logger.info(f"[InterfaceContable] Configuración cargada: {self.config}")
            txProcedureInterface_str = self.config.get("txProcedureInterface")
            print(
                f"[InterfaceContable] txProcedureInterface_str: {txProcedureInterface_str}"
            )
            logger.info(
                f"[InterfaceContable] txProcedureInterface_str: {txProcedureInterface_str}"
            )
            if isinstance(txProcedureInterface_str, str):
                try:
                    self.config["txProcedureInterface"] = ast.literal_eval(
                        txProcedureInterface_str
                    )
                except ValueError as e:
                    print(
                        f"[InterfaceContable] Error al convertir txProcedureInterface a lista: {e}"
                    )
                    logger.error(
                        f"[InterfaceContable] Error al convertir txProcedureInterface a lista: {e}"
                    )
                    self.config["txProcedureInterface"] = []
            print(
                f"[InterfaceContable] txProcedureInterface (list): {self.config['txProcedureInterface']}"
            )
            logger.info(
                f"[InterfaceContable] txProcedureInterface (list): {self.config['txProcedureInterface']}"
            )
            if not self.config["txProcedureInterface"] or not any(
                str(x).strip() for x in self.config["txProcedureInterface"]
            ):
                print("[InterfaceContable] No hay datos para procesar")
                logger.warning("[InterfaceContable] No hay datos para procesar")
                return {"success": False, "error_message": "No hay datos para procesar"}

            # Generar nombre de archivo de salida
            ext = ".xlsx"
            # Incluir user_id y reporte_id para unicidad y trazabilidad
            reporte_id_str = (
                f"_reporte_{self.reporte_id}"
                if hasattr(self, "reporte_id") and self.reporte_id
                else ""
            )
            user_id_str = f"_user_{self.user_id}" if self.user_id else ""
            self.file_name = f"Interface_Contable_{self.database_name}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}{user_id_str}{reporte_id_str}{ext}"
            self.file_path = os.path.join("media", self.file_name)
            output_dir = os.path.dirname(self.file_path)
            os.makedirs(output_dir, exist_ok=True)

            # Crear un ExcelWriter para todas las hojas
            with pd.ExcelWriter(self.file_path, engine="xlsxwriter") as writer:
                total_global_records = 0
                for idx, hoja in enumerate(
                    self.config["txProcedureInterface"], start=1
                ):
                    hoja = str(hoja).strip()
                    if not hoja:
                        continue
                    print(
                        f"[InterfaceContable] Ejecutando query para extraer datos de la hoja: {hoja}"
                    )
                    logger.info(
                        f"[InterfaceContable] Ejecutando query para extraer datos de la hoja: {hoja}"
                    )
                    try:
                        query = self._generate_sqlout(hoja)
                        print(f"[InterfaceContable] Query generado: {query}")
                        logger.info(f"[InterfaceContable] Query generado: {query}")
                        table_name = f"{self.sqlite_table_name}_{hoja}"

                        # Ejecutar consulta y verificar si se creó la tabla
                        self._execute_query_to_sqlite(query, table_name)

                        # Verificar que la tabla exista antes de continuar
                        with self.engine_sqlite.connect() as conn:
                            table_exists = conn.execute(
                                text(
                                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                                )
                            ).fetchone()

                            if not table_exists:
                                logger.warning(
                                    f"[InterfaceContable] La tabla {table_name} no se creó correctamente. Omitiendo..."
                                )
                                print(
                                    f"[InterfaceContable] La tabla {table_name} no se creó correctamente. Omitiendo..."
                                )
                                continue

                            # Verificar que haya datos en la tabla
                            row_count = conn.execute(
                                text(f"SELECT COUNT(*) FROM {table_name}")
                            ).scalar()
                            if row_count == 0:
                                logger.warning(
                                    f"[InterfaceContable] La tabla {table_name} está vacía. Omitiendo..."
                                )
                                print(
                                    f"[InterfaceContable] La tabla {table_name} está vacía. Omitiendo..."
                                )
                                self._cleanup_table(table_name)
                                continue

                        print(
                            f"[InterfaceContable] Extracción de datos completada para hoja {hoja}. Total registros procesados: {self.total_records_processed}"
                        )
                        logger.info(
                            f"[InterfaceContable] Extracción de datos completada para hoja {hoja}. Total registros procesados: {self.total_records_processed}"
                        )

                        # Guardar datos en la hoja correspondiente
                        self._guardar_datos_excel_xlsxwriter(table_name, hoja, writer)
                        total_global_records += self.total_records_processed

                        # Limpiar tabla temporal
                        self._cleanup_table(table_name)
                    except Exception as e:
                        logger.error(
                            f"[InterfaceContable] Error al procesar hoja {hoja}: {str(e)}",
                            exc_info=True,
                        )
                        print(
                            f"[InterfaceContable] Error al procesar hoja {hoja}: {str(e)}"
                        )
                        # Intentar limpiar tabla en caso de error
                        try:
                            self._cleanup_table(f"{self.sqlite_table_name}_{hoja}")
                        except:
                            pass
                        # Continuar con la siguiente hoja
                        continue
                    # Progreso global por hoja
                    if self.progress_callback:
                        percent = int(
                            (idx / len(self.config["txProcedureInterface"])) * 100
                        )
                        self.progress_callback(
                            f"Progreso global: {idx}/{len(self.config['txProcedureInterface'])} hojas",
                            percent,
                            hoja_idx=idx,
                            total_hojas=len(self.config["txProcedureInterface"]),
                        )

            execution_time = time.time() - self.start_time
            if total_global_records == 0:
                print("[InterfaceContable] No hay datos para mostrar en ninguna hoja.")
                logger.warning(
                    "[InterfaceContable] No hay datos para mostrar en ninguna hoja."
                )
                return {
                    "success": False,
                    "error_message": "No hay datos para mostrar en ninguna hoja.",
                    "file_path": None,
                    "file_name": None,
                    "execution_time": execution_time,
                    "metadata": {"total_records": 0},
                }
            print(f"[InterfaceContable] Archivo generado en: {self.file_path}")
            logger.info(f"[InterfaceContable] Archivo generado en: {self.file_path}")
            print(
                f"[InterfaceContable] Proceso completado correctamente en {execution_time:.2f} segundos."
            )
            logger.info(
                f"[InterfaceContable] Proceso completado correctamente en {execution_time:.2f} segundos."
            )
            self._update_progress("Completado", 100, total_global_records)
            logger.info(
                f"InterfaceContable completada en {execution_time:.2f} segundos. Archivo: {self.file_path}"
            )
            return {
                "success": True,
                "message": f"Interface contable generada exitosamente en {execution_time:.2f} segundos.",
                "file_path": self.file_path,
                "file_name": self.file_name,
                "execution_time": execution_time,
                "metadata": {
                    "total_records": total_global_records,
                },
            }
        except Exception as e:
            execution_time = time.time() - self.start_time
            error_msg = (
                f"Error fatal en InterfaceContable.run: {type(e).__name__} - {e}"
            )
            print(f"[InterfaceContable] ERROR: {error_msg}")
            logger.error(f"[InterfaceContable] ERROR: {error_msg}", exc_info=True)
            self._update_progress(f"Error fatal: {e}", 100)
            return {
                "success": False,
                "message": f"Error al generar la interface: {error_msg}",
                "error": error_msg,
                "file_path": None,
                "file_name": None,
                "execution_time": execution_time,
                "metadata": {},
            }

    def _guardar_datos_excel_xlsxwriter(self, table_name, hoja, writer):
        # Definir el tamaño de cada bloque de datos (chunk)
        chunksize = 50000
        with self.engine_sqlite.connect() as connection:
            startrow = 0
            table_exists = connection.execute(
                text(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                )
            ).fetchone()
            if not table_exists:
                logger.warning(f"La tabla {table_name} no existe en SQLite")
                return
            for chunk in pd.read_sql_query(
                f"SELECT * FROM {table_name}", self.engine_sqlite, chunksize=chunksize
            ):
                print(f"Procesando chunk de tamaño: {len(chunk)} para hoja {hoja}")
                chunk.to_excel(
                    writer,
                    sheet_name=hoja,
                    startrow=startrow,
                    index=False,
                    header=not bool(startrow),
                )
                startrow += len(chunk)
                print(f"Próximo startrow será: {startrow} para hoja {hoja}")
            writer.sheets[hoja].sheet_state = "visible"

    def _cleanup_table(self, table_name):
        try:
            if self.engine_sqlite:
                with self.engine_sqlite.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        except Exception as e:
            logger.warning(
                f"Error durante la limpieza de la tabla {table_name}: {e}",
                exc_info=True,
            )

    def get_data(self, start_row=0, chunk_size=10000):
        if not self.engine_sqlite:
            return {
                "headers": [],
                "rows": [],
                "metadata": {"error": "Engine not ready"},
            }
        try:
            with self.engine_sqlite.connect() as connection:
                total_count = connection.execute(
                    text(f"SELECT COUNT(*) FROM {self.sqlite_table_name}")
                ).scalar()
                headers_result = connection.execute(
                    text(f"PRAGMA table_info({self.sqlite_table_name})")
                ).fetchall()
                headers = [col[1] for col in headers_result]
                query = text(
                    f"SELECT * FROM {self.sqlite_table_name} LIMIT :limit OFFSET :offset"
                )
                result = connection.execute(
                    query, {"limit": chunk_size, "offset": start_row}
                )
                rows = [tuple(row) for row in result]
            return {
                "headers": headers,
                "rows": rows,
                "metadata": {
                    "total_records": total_count,
                    "current_page": start_row // chunk_size + 1,
                    "total_pages": (total_count + chunk_size - 1) // chunk_size,
                    "start_row": start_row,
                    "chunk_size": chunk_size,
                    "has_more": start_row + len(rows) < total_count,
                },
            }
        except Exception as e:
            return {"headers": [], "rows": [], "metadata": {"error": str(e)}}
