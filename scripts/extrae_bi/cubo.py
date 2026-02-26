# scripts/extrae_bi/cubo.py
import os
import pandas as pd
import time
import gc
import logging
import uuid
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.exc import SQLAlchemyError
from openpyxl import Workbook
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from apps.home.models import Reporte
import psutil
from scripts.text_cleaner import TextCleaner

logger = logging.getLogger(__name__)


class CuboVentas:
    """
    Clase para manejar el proceso de creación de cubos de ventas.

    Atributos:
        database_name (str): Nombre de la base de datos.
        IdtReporteIni (str): Fecha de inicio del reporte.
        IdtReporteFin (str): Fecha de fin del reporte.
        user_id (int): ID del usuario.
        reporte_id (int): ID del reporte.
        config (dict): Configuración para las conexiones a las bases de datos.
        proveedores (list): Lista de proveedores.
        macrozonas (list): Lista de macrozonas.
        engine_sqlite (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos SQLite.
        engine_mysql (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos MySQL.
        file_path (str): Ruta del archivo generado.
        archivo_cubo_ventas (str): Nombre del archivo generado.
        progress_callback (callable): Función para reportar el progreso.
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
        """
        Inicializa la instancia de CuboVentas.

        Args:
            database_name (str): Nombre de la base de datos MySQL/MariaDB.
            IdtReporteIni (str): Fecha de inicio del reporte (YYYY-MM-DD).
            IdtReporteFin (str): Fecha de fin del reporte (YYYY-MM-DD).
            user_id (int): ID del usuario que solicita el reporte.
            reporte_id (int): ID del objeto Reporte que contiene la consulta base.
            progress_callback (callable, opcional): Función para reportar progreso.
                                                   Debe aceptar (stage, progress_percent, current_rec, total_rec).
        """
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.user_id = user_id
        self.reporte_id = reporte_id
        self.progress_callback = progress_callback
        self.start_time = time.time()  # Para calcular tiempo total

        # Estado interno
        self.config = {}
        self.proveedores = []
        self.macrozonas = []
        self.engine_mysql = None
        self.engine_sqlite = None
        self.sqlite_table_name = f"cubo_{self.database_name}_{self.user_id}_{uuid.uuid4().hex[:8]}"  # Tabla temporal única
        self.file_path = None
        self.file_name = None
        self.total_records_processed = 0
        self.total_records_estimate = 0

        logger.info(
            f"Inicializando CuboVentas: DB={database_name}, ReporteID={reporte_id}, UserID={user_id}"
        )
        self._update_progress("Inicializando", 1)

        try:
            self._configurar_conexiones()
            self._create_sqlite_engine()
        except Exception as e:
            logger.error(
                f"Error crítico durante la inicialización de CuboVentas: {e}",
                exc_info=True,
            )
            self._update_progress(f"Error inicialización: {e}", 100)
            # Propagar la excepción para que la tarea Celery falle correctamente
            raise

    def _update_progress(
        self, stage, progress_percent, current_rec=None, total_rec=None
    ):
        """Llama al callback de progreso si está definido."""
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
                # Asegurar que el progreso esté entre 0 y 100
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
        """Configura las conexiones a las bases de datos."""
        self._update_progress("Configurando conexiones", 2)
        logger.info("Configurando conexiones...")
        try:
            config_basic = ConfigBasic(self.database_name, self.user_id)
            self.config = config_basic.config
            self.proveedores = self.config.get("proveedores", [])
            self.macrozonas = self.config.get("macrozonas", [])

            # Validar configuración de conexión MySQL/MariaDB
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
            logger.info("Conexión a MySQL/MariaDB configurada.")
        except Exception as e:
            logger.error(f"Error al configurar conexiones: {e}", exc_info=True)
            raise  # Propagar para fallo claro

    def _create_sqlite_engine(self):
        """Crea y optimiza el motor SQLAlchemy para SQLite."""
        self._update_progress("Configurando BD temporal", 3)
        logger.info("Creando y optimizando motor SQLite...")
        try:
            # Usar archivo temporal único en 'media' para depuración de permisos
            sqlite_path = os.path.join("media", f"temp_{self.sqlite_table_name}.db")
            os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
            self.engine_sqlite = create_engine(f"sqlite:///{sqlite_path}")

            # Aplicar optimizaciones PRAGMA (con precaución)
            with self.engine_sqlite.connect() as conn:
                conn.execute(text("PRAGMA journal_mode = MEMORY;"))
                conn.execute(text("PRAGMA synchronous = OFF;"))
                conn.execute(text("PRAGMA cache_size = -100000;"))
                conn.execute(text("PRAGMA temp_store = MEMORY;"))
            logger.info(f"Motor SQLite creado y optimizado en {sqlite_path}.")
        except Exception as e:
            logger.error(f"Error creando motor SQLite: {e}", exc_info=True)
            raise

    def _generate_sql_query(self):
        """
        Genera la consulta SQL principal usando parámetros seguros.

        Returns:
            tuple: (sqlalchemy.sql.elements.TextClause, dict) Consulta SQL y parámetros.

        Raises:
            ValueError: Si el reporte no se encuentra o la consulta base es inválida.
        """
        self._update_progress("Generando consulta SQL", 5)
        logger.info(f"Generando consulta SQL para Reporte ID: {self.reporte_id}")
        try:
            reporte = Reporte.objects.get(pk=self.reporte_id)
            base_sql = reporte.sql_text
            if not base_sql:
                raise ValueError(
                    f"El Reporte ID {self.reporte_id} no tiene texto SQL definido."
                )

            logger.debug(f"Consulta base obtenida:\n{base_sql}")

            params = {
                "fi": self.IdtReporteIni,
                "ff": self.IdtReporteFin,
                "empresa": self.database_name.upper(),  # Asumiendo que :empresa es un valor
            }
            if self.reporte_id != 2:
                # Si es el reporte es diferente de 2, no hay filtros adicionales
                final_sql_text = text(base_sql)
                return final_sql_text, params
            else:
                # Construir cláusulas WHERE adicionales para filtros
                where_clauses = []
                if self.proveedores:
                    # Usar IN con parámetros múltiples si SQLAlchemy lo soporta bien, o construir dinámicamente
                    # Forma segura con parámetros individuales para evitar límites:
                    prov_params = {}
                    prov_placeholders = []
                    for i, prov in enumerate(self.proveedores):
                        param_name = f"prov_{i}"
                        prov_params[param_name] = prov
                        prov_placeholders.append(f":{param_name}")
                    if prov_placeholders:
                        where_clauses.append(
                            f"idProveedor IN ({', '.join(prov_placeholders)})"
                        )
                        params.update(prov_params)
                    logger.info(f"Aplicando filtro de proveedores: {self.proveedores}")

                if self.macrozonas:
                    # Similar para macrozonas
                    macro_params = {}
                    macro_placeholders = []
                    for i, macro in enumerate(self.macrozonas):
                        param_name = f"macro_{i}"
                        macro_params[param_name] = macro
                        macro_placeholders.append(f":{param_name}")
                    if macro_placeholders:
                        where_clauses.append(
                            f"macrozona_id IN ({', '.join(macro_placeholders)})"
                        )  # Ajustar nombre de columna si es diferente
                        params.update(macro_params)
                    logger.info(f"Aplicando filtro de macrozonas: {self.macrozonas}")

                # Integrar cláusulas WHERE adicionales de forma segura
                # Esto asume que la consulta base NO termina con WHERE o GROUP BY/ORDER BY/LIMIT
                # Se necesita una forma robusta de insertar los filtros.
                # Opción 1: Marcador específico en la consulta base (ej: -- FILTERS_HERE)
                if "-- FILTERS_HERE" in base_sql and where_clauses:
                    filter_sql = " AND " + " AND ".join(where_clauses)
                    final_sql = base_sql.replace("-- FILTERS_HERE", filter_sql)
                                # Opción 2: Añadir siempre al final (menos robusto si hay GROUP BY/ORDER BY)
                elif where_clauses:
                    # Intentar añadir antes de GROUP BY, ORDER BY, LIMIT si existen
                    # Esta parte es compleja y depende de la estructura de base_sql
                    # Solución simple (puede fallar): añadir antes del ';' si existe
                    parts = base_sql.split(";")
                    if len(parts) > 1:
                        parts[0] += " AND " + " AND ".join(where_clauses)
                        final_sql = ";".join(parts)
                    else:
                        final_sql = base_sql + " AND " + " AND ".join(where_clauses)
                else:
                    # Si no hay where_clauses, usar la consulta base tal cual
                    final_sql = base_sql
            # Asegurar que la consulta final use los parámetros :fi, :ff, :empresa, etc.
            # Asegurar que la consulta final use los parámetros :fi, :ff, :empresa, etc.
            # No hacer replace aquí, pasar `params` a execute.
            final_sql_text = text(final_sql)
            print(f"Consulta SQL generada:\n{final_sql_text}")

            logger.info("Consulta SQL final generada.")
            logger.debug(f"SQL Final (sin parámetros reemplazados):\n{final_sql_text}")
            logger.debug(f"Parámetros: {params}")

            return final_sql_text, params

        except Reporte.DoesNotExist:
            logger.error(f"Reporte con ID {self.reporte_id} no encontrado.")
            raise ValueError(f"Reporte con ID {self.reporte_id} no encontrado.")
        except Exception as e:
            logger.error(f"Error generando consulta SQL: {e}", exc_info=True)
            raise

    def _estimate_total_records(self, query, params):
        """Estima el número total de registros ejecutando un COUNT(*)."""
        # Esta función es opcional y puede ser costosa.
        # Se podría intentar parsear la query original y reemplazar SELECT ... FROM por SELECT COUNT(*) FROM
        # O simplemente ejecutarla si no es demasiado lenta.
        # Por ahora, la omitimos para simplificar y evitar sobrecarga.
        self.total_records_estimate = 0  # Indicar que no hay estimación
        logger.info("Estimación de registros totales omitida.")
        return 0

    def _execute_query_to_sqlite(self, query, params, chunksize=10000):
        """Ejecuta la consulta MySQL y guarda los resultados en SQLite en chunks."""
        stage_name = "Extrayendo datos de MySQL"
        self._update_progress(stage_name, 10)
        logger.info(
            f"Iniciando extracción de datos a SQLite ({self.sqlite_table_name}). Chunksize={chunksize}"
        )

        total_processed = 0
        first_chunk = True
        columns = None
        start_extract_time = time.time()
        try:
            with self.engine_mysql.connect() as mysql_conn, self.engine_sqlite.connect() as sqlite_conn:
                result = mysql_conn.execution_options(stream_results=True).execute(query, params)
                columns = result.keys()
                rows = result.fetchmany(chunksize)
                if not rows:
                    logger.info("La consulta no retornó datos. No se generará archivo.")
                    self.total_records_processed = 0
                    return False  # Indica que no hay datos
                while rows:
                    df_chunk = pd.DataFrame(rows, columns=columns)
                    if first_chunk:
                        logger.info(
                            f"Creando tabla SQLite '{self.sqlite_table_name}' con {len(columns)} columnas."
                        )
                        df_chunk.to_sql(
                            name=self.sqlite_table_name,
                            con=sqlite_conn,
                            if_exists="replace",
                            index=False,
                            method=None,
                        )
                        first_chunk = False
                    else:
                        df_chunk.to_sql(
                            name=self.sqlite_table_name,
                            con=sqlite_conn,
                            if_exists="append",
                            index=False,
                            method="multi",
                            chunksize=1000,
                        )
                    total_processed += len(rows)
                    self.total_records_processed = total_processed
                    rows = result.fetchmany(chunksize)
            # Verificar recuento final en SQLite
            with self.engine_sqlite.connect() as sqlite_conn:
                final_count = sqlite_conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.sqlite_table_name}")
                ).scalar()
                if final_count != total_processed:
                    logger.warning(
                        f"Discrepancia en conteo: MySQL procesó {total_processed}, SQLite tiene {final_count}"
                    )
                    self.total_records_processed = final_count
            logger.info(
                f"Extracción a SQLite completada. Total: {self.total_records_processed:,} registros."
            )
            self._update_progress(
                "Datos extraídos a BD temporal", 80, self.total_records_processed
            )
            return True
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

    def _generate_output_file(self, hoja_nombre, chunksize=10000):
        """Genera el archivo de salida (Excel o CSV) desde la tabla SQLite."""
        stage_name = "Generando archivo de salida"
        self._update_progress(stage_name, 85, self.total_records_processed)
        logger.info(
            f"Iniciando generación de archivo para {self.total_records_processed:,} registros."
        )

        # Decidir formato y generar nombre/ruta
        use_csv = self.total_records_processed > 1000000  # Umbral para CSV
        ext = ".csv" if use_csv else ".xlsx"
        self.file_name = f"{hoja_nombre}_{self.database_name.upper()}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}_user_{self.user_id}{ext}"
        self.file_path = os.path.join("media", self.file_name)
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        logger.info(f"Archivo de salida: {self.file_path}")

        start_export_time = time.time()
        records_written = 0

        try:
            with self.engine_sqlite.connect() as sqlite_conn:
                # Obtener encabezados
                headers_result = sqlite_conn.execute(
                    text(f"PRAGMA table_info({self.sqlite_table_name})")
                ).fetchall()
                header_names = [col[1] for col in headers_result]

                if use_csv:
                    logger.info("Exportando a CSV...")
                    # Escribir encabezado
                    pd.DataFrame(columns=header_names).to_csv(
                        self.file_path, index=False, encoding="utf-8-sig"
                    )  # utf-8-sig para Excel
                    # Escribir datos en chunks
                    for chunk_df in pd.read_sql_query(
                        f"SELECT * FROM {self.sqlite_table_name}",
                        sqlite_conn,
                        chunksize=chunksize,
                    ):
                        chunk_df.to_csv(
                            self.file_path,
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
                        self._update_progress(
                            f"{stage_name} (CSV)", progress_percent, records_written
                        )
                        # Log extra para diagnóstico
                        if records_written % (chunksize * 2) == 0:
                            logger.info(
                                f"Exportación parcial: {records_written:,} registros escritos en {time.time() - start_export_time:.2f}s. Memoria usada: {psutil.Process(os.getpid()).memory_info().rss / (1024*1024):.1f} MB"
                            )
                        del chunk_df
                        gc.collect()
                else:
                    logger.info("Exportando a Excel (optimizad)...")
                    # Usar openpyxl write_only para memoria baja
                    wb = Workbook(write_only=True)
                    ws = wb.create_sheet(title=hoja_nombre)
                    ws.append(header_names)  # Escribir encabezados

                    # Leer y escribir en chunks
                    cursor = sqlite_conn.execute(
                        text(f"SELECT * FROM {self.sqlite_table_name}")
                    )
                    while True:
                        rows = cursor.fetchmany(chunksize)
                        if not rows:
                            break
                        for row in rows:
                            # Convertir RowProxy a lista/tupla simple
                            # Limpiar valores string para evitar caracteres ilegales en openpyxl
                            cleaned_row = tuple(
                                TextCleaner.clean_for_excel(v) if isinstance(v, str) else v
                                for v in row
                            )
                            ws.append(cleaned_row)
                        records_written += len(rows)
                        progress_percent = (
                            85 + (records_written / self.total_records_processed * 14)
                            if self.total_records_processed > 0
                            else 99
                        )
                        self._update_progress(
                            f"{stage_name} (Excel)", progress_percent, records_written
                        )
                        if records_written % (chunksize * 2) == 0:
                            logger.info(
                                f"Exportación parcial: {records_written:,} registros escritos en {time.time() - start_export_time:.2f}s. Memoria usada: {psutil.Process(os.getpid()).memory_info().rss / (1024*1024):.1f} MB"
                            )
                        # No es necesario borrar rows aquí, fetchmany devuelve copias

                    logger.info(f"Guardando archivo Excel en {self.file_path}...")
                    save_start = time.time()
                    wb.save(self.file_path)
                    logger.info(
                        f"Archivo Excel guardado en {time.time() - save_start:.2f}s"
                    )

            export_time = time.time() - start_export_time
            logger.info(
                f"Generación de archivo completada en {export_time:.2f}s. Registros escritos: {records_written:,}"
            )
            if records_written != self.total_records_processed:
                logger.warning(
                    f"Discrepancia en escritura: SQLite tenía {self.total_records_processed}, archivo tiene {records_written}"
                )

            self._update_progress("Archivo generado", 99, records_written)

        except Exception as e:
            logger.error(f"Error generando archivo de salida: {e}", exc_info=True)
            self._update_progress(f"Error archivo: {e}", 100)
            # Intentar limpiar archivo parcial si existe
            if self.file_path and os.path.exists(self.file_path):
                try:
                    os.remove(self.file_path)
                    logger.info(f"Archivo parcial eliminado: {self.file_path}")
                except OSError as oe:
                    logger.warning(
                        f"No se pudo eliminar archivo parcial {self.file_path}: {oe}"
                    )
            self.file_path = None  # Indicar que no hay archivo válido
            raise

    def _cleanup(self):
        """Limpia recursos como la tabla SQLite temporal."""
        logger.info(f"Limpiando tabla temporal SQLite: {self.sqlite_table_name}")
        try:
            if self.engine_sqlite:
                with self.engine_sqlite.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self.sqlite_table_name}"))
                logger.info("Tabla temporal eliminada.")
                # Si SQLite era un archivo, eliminarlo:
                # if "///" in str(self.engine_sqlite.url): # Es archivo
                #     db_path = str(self.engine_sqlite.url).split("///")[1]
                #     if os.path.exists(db_path):
                #         os.remove(db_path)
                #         logger.info(f"Archivo DB temporal eliminado: {db_path}")
        except Exception as e:
            logger.warning(f"Error durante la limpieza de SQLite: {e}", exc_info=True)

    def _generate_performance_report(self, execution_time):
        """Genera un reporte de rendimiento."""
        try:
            report = ["=== REPORTE DE RENDIMIENTO ==="]
            report.append(f"Tiempo total de ejecución: {execution_time:.2f} segundos")
            report.append(f"Registros procesados: {self.total_records_processed:,}")
            if execution_time > 0 and self.total_records_processed > 0:
                report.append(
                    f"Velocidad promedio: {self.total_records_processed / execution_time:.1f} reg/seg"
                )

            if self.file_path and os.path.exists(self.file_path):
                file_size_mb = os.path.getsize(self.file_path) / (1024 * 1024)
                report.append(
                    f"Archivo generado: {self.file_name} ({file_size_mb:.2f} MB)"
                )
            else:
                report.append("Archivo generado: No disponible")

            report.append(
                f"\nParámetros: DB={self.database_name}, Periodo={self.IdtReporteIni}-{self.IdtReporteFin}"
            )
            if self.proveedores:
                report.append(f"Filtro Proveedores: {len(self.proveedores)} aplicados")
            if self.macrozonas:
                report.append(f"Filtro Macrozonas: {len(self.macrozonas)} aplicados")

            # Memoria
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            report.append(
                f"\nUso máximo de memoria (RSS): {memory_info.rss / (1024 * 1024):.1f} MB"
            )

            return "\n".join(report)
        except Exception as e:
            logger.warning(f"Error generando reporte de rendimiento: {e}")
            return "No se pudo generar el reporte de rendimiento."

    # --- Método Principal ---
    def run(self):
        """
        Orquesta el proceso completo de generación del cubo de ventas.

        Returns:
            dict: Diccionario con el resultado del proceso:
                  {
                      'success': bool,
                      'message': str,
                      'file_path': str or None,
                      'file_name': str or None,
                      'execution_time': float,
                      'metadata': dict
                  }
        """
        try:
            # 1. Generar Consulta SQL
            query, params = self._generate_sql_query()
            print(f"Consulta SQL generada:\n{query}\nParámetros: {params}")

            # 2. (Opcional) Estimar Registros
            self._estimate_total_records(query, params)

            # 3. Ejecutar Consulta y volcar a SQLite
            datos_ok = self._execute_query_to_sqlite(query, params)
            if datos_ok is False or self.total_records_processed == 0:
                self._update_progress("Sin datos para mostrar", 100, 0, 0)
                return {
                    "success": False,
                    "message": "No hay datos para mostrar en el cubo de ventas.",
                    "file_path": None,
                    "file_name": None,
                    "execution_time": time.time() - self.start_time,
                    "metadata": {"total_records": 0},
                }

            # 4. Generar Archivo de Salida (Excel/CSV) desde SQLite
            reporte = Reporte.objects.get(pk=self.reporte_id)  # Obtener nombre de hoja
            hoja_nombre = reporte.nombre or "CuboVentas"
            self._generate_output_file(hoja_nombre)

            # 5. Limpieza
            self._cleanup()

            # 6. Finalizar y Reportar
            execution_time = time.time() - self.start_time
            performance_report = self._generate_performance_report(execution_time)
            logger.info(
                f"Proceso CuboVentas completado exitosamente en {execution_time:.2f}s."
            )
            logger.info("INFORME DE RENDIMIENTO:\n" + performance_report)
            self._update_progress("Completado", 100, self.total_records_processed)

            return {
                "success": True,
                "message": f"Cubo de ventas generado exitosamente en {execution_time:.2f} segundos.",
                "file_path": self.file_path,
                "file_name": self.file_name,
                "execution_time": execution_time,
                "metadata": {
                    "total_records": self.total_records_processed,
                    "performance_report": performance_report,
                },
            }

        except Exception as e:
            execution_time = time.time() - self.start_time
            error_msg = f"Error fatal en CuboVentas.run: {type(e).__name__} - {e}"
            logger.error(error_msg, exc_info=True)
            self._update_progress(
                f"Error fatal: {e}", 100, self.total_records_processed
            )
            return {
                "success": False,
                "message": error_msg,
                "file_path": None,
                "file_name": None,
                "execution_time": execution_time,
                "metadata": {"total_records": self.total_records_processed},
            }

    # --- Métodos Adicionales (Mantenidos de la versión original si son necesarios) ---

    def get_data(self, start_row=0, chunk_size=10000, search=None):
        """
        Obtiene datos paginados desde la tabla SQLite temporal (para previsualización).
        ADVERTENCIA: Llama a este método ANTES de que run() complete la limpieza.
        Permite búsqueda simple si se pasa un string en 'search'.
        """
        logger.info(
            f"Obteniendo datos paginados: start={start_row}, size={chunk_size} from {self.sqlite_table_name}"
        )
        if not self.engine_sqlite:
            logger.error("Intento de obtener datos sin motor SQLite inicializado.")
            return {
                "headers": [],
                "rows": [],
                "metadata": {"error": "Engine not ready"},
            }

        try:
            with self.engine_sqlite.connect() as connection:
                # Verificar si la tabla existe
                from sqlalchemy import inspect
                inspector = inspect(self.engine_sqlite)
                if not inspector.has_table(self.sqlite_table_name):
                    logger.warning(
                        f"Tabla {self.sqlite_table_name} no encontrada para get_data."
                    )
                    return {
                        "headers": [],
                        "rows": [],
                        "metadata": {
                            "error": "Temporary data not found or already cleaned."
                        },
                    }

                total_count = connection.execute(
                    text(f"SELECT COUNT(*) FROM {self.sqlite_table_name}")
                ).scalar()

                headers_result = connection.execute(
                    text(f"PRAGMA table_info({self.sqlite_table_name})")
                ).fetchall()
                headers = [col[1] for col in headers_result]

                # Búsqueda simple (solo para texto)
                where_clause = ""
                params = {"limit": chunk_size, "offset": start_row}
                if search and search.strip():
                    search_terms = search.strip().split()
                    like_clauses = [
                        f"({ ' OR '.join([f'{h} LIKE :search_{i}_{j}' for h in headers]) })"
                        for j, term in enumerate(search_terms)
                        for i, h in enumerate(headers)
                    ]
                    if like_clauses:
                        where_clause = "WHERE " + " AND ".join(like_clauses)
                        for j, term in enumerate(search_terms):
                            for i, h in enumerate(headers):
                                params[f"search_{i}_{j}"] = f"%{term}%"

                query = text(
                    f"SELECT * FROM {self.sqlite_table_name} {where_clause} LIMIT :limit OFFSET :offset"
                )
                result = connection.execute(query, params)
                rows = [tuple(row) for row in result]  # Convertir a tuplas simples

            logger.debug(f"get_data: {len(rows)} filas recuperadas.")
            return {
                "headers": headers,
                "rows": rows,
                "total_records": total_count,
                "filtered_records": total_count if not search else len(rows),
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
            logger.error(f"Error en get_data: {e}", exc_info=True)
            return {"headers": [], "rows": [], "metadata": {"error": str(e)}}
