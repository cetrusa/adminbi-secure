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
    Esta versión escribe directamente a Excel en chunks, sin usar SQLite.
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
        self.reporte_id = reporte_id
        self.progress_callback = progress_callback
        self.start_time = time.time()
        self.config = {}
        self.engine_mysql = None
        self.file_path = None
        self.file_name = None
        self.total_records_processed = 0
        self.total_records_estimate = 0
        self._update_progress("Inicializando", 1)
        try:
            self._configurar_conexiones()
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

    def _write_query_to_excel(self, query, hoja, writer, chunksize=10000):
        """
        Escritura robusta a Excel por chunks, compatible con cualquier consulta/procedimiento.
        """
        stage_name = f"Extrayendo y escribiendo datos de MySQL para hoja {hoja} (SIN CHUNKS)"
        self._update_progress(stage_name, 10)
        total_processed = 0
        start_extract_time = time.time()
        try:
            # Ejecutar el query (puede ser CALL o SELECT)
            with self.engine_mysql.connect() as conn:
                result = conn.execute(query)
                columns = list(result.keys())
                # Acumular todos los resultados en un solo DataFrame
                all_rows = [
                    dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
                    for row in result
                ]
                df_all = pd.DataFrame(all_rows)
                if not df_all.empty:
                    df_all = df_all.astype(str)
                    # Diagnóstico: buscar fila específica antes de escribir
                    if (
                        "Código de Cuenta" in df_all.columns
                        and "Documento" in df_all.columns
                    ):
                        target_row = df_all[
                            (df_all["Código de Cuenta"] == "13109501")
                            & (df_all["Documento"] == "5727337")
                        ]
                        if not target_row.empty:
                            print(
                                f"[DEBUG][NO CHUNKS] ¡Fila 13109501 encontrada en DataFrame completo!"
                            )
                            print(target_row)
                    # Justo antes de escribir, imprime todas las filas con Documento == '5727337'
                    print(
                        f"[DEBUG][NO CHUNKS] Todas las filas con Documento == '5727337' ANTES DE ESCRIBIR:"
                    )
                    print(df_all[df_all["Documento"] == "5727337"])
                    df_all.to_excel(
                        writer,
                        sheet_name=hoja,
                        startrow=0,
                        index=False,
                        header=True,
                    )
                    total_processed = len(df_all)
                else:
                    print(f"[DEBUG][NO CHUNKS] No se encontraron datos para la hoja {hoja}")
                    empty_df = pd.DataFrame(columns=columns)
                    empty_df.to_excel(writer, sheet_name=hoja, index=False)
        except SQLAlchemyError as e:
            logger.error(
                f"Error de base de datos durante la extracción para {hoja}: {e}",
                exc_info=True,
            )
            self._update_progress(f"Error BD en {hoja}: {e}", 100)
            raise
        except Exception as e:
            logger.error(
                f"Error inesperado durante la extracción para {hoja}: {e}",
                exc_info=True,
            )
            self._update_progress(f"Error en {hoja}: {e}", 100)
            raise
        self.total_records_processed = total_processed
        self._update_progress(
            f"Datos extraídos y escritos a Excel para hoja {hoja} (SIN CHUNKS)",
            80,
            total_processed,
        )

    def diagnostico_excel_completo(self, query, hoja, output_path):
        """
        Diagnóstico: ejecuta el query, carga todo el resultado en un DataFrame y lo exporta a Excel de una sola vez.
        """
        print(f"[DIAGNÓSTICO] Ejecutando query completo para hoja {hoja}")
        with self.engine_mysql.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(
                [
                    dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
                    for row in result
                ]
            )
        print(f"[DIAGNÓSTICO] DataFrame completo shape: {df.shape}")
        if "Código de Cuenta" in df.columns and "Documento" in df.columns:
            fila_diag = df[
                (df["Código de Cuenta"].astype(str) == "13109501")
                & (df["Documento"].astype(str) == "5727337")
            ]
            print("[DIAGNÓSTICO] Fila 13109501 en DataFrame completo:")
            print(fila_diag)
        # Guardar a Excel
        df = df.astype(str)
        df.to_excel(output_path, sheet_name=hoja, index=False)
        print(f"[DIAGNÓSTICO] Archivo Excel completo guardado en: {output_path}")
        # Leer de vuelta y verificar
        try:
            df_verif = pd.read_excel(output_path, sheet_name=hoja)
            fila_verif = df_verif[
                (df_verif["Código de Cuenta"].astype(str) == "13109501")
                & (df_verif["Documento"].astype(str) == "5727337")
            ]
            print("[DIAGNÓSTICO] Fila 13109501 en Excel completo generado:")
            print(fila_verif)
        except Exception as e:
            print(f"[DIAGNÓSTICO] Error al verificar Excel completo: {e}")

    def run(self):
        print(
            "[InterfaceContable] INICIO del proceso de generación de interface contable (directo a Excel)"
        )
        logger.info(
            "[InterfaceContable] INICIO del proceso de generación de interface contable (directo a Excel)"
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

            # Usar openpyxl como engine para ExcelWriter
            with pd.ExcelWriter(self.file_path, engine="openpyxl") as writer:
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
                        self._write_query_to_excel(query, hoja, writer)
                        total_global_records += self.total_records_processed
                    except Exception as e:
                        logger.error(
                            f"[InterfaceContable] Error al procesar hoja {hoja}: {str(e)}",
                            exc_info=True,
                        )
                        print(
                            f"[InterfaceContable] Error al procesar hoja {hoja}: {str(e)}"
                        )
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
            # Verificación automática: ¿la fila buscada está en el Excel generado?
            try:
                df_verif = pd.read_excel(self.file_path, sheet_name=hoja)
                fila_buscada = df_verif[df_verif["Documento"] == "5731603"]
                print("\n[VERIFICACIÓN FINAL] Fila buscada en Excel generado:")
                print(fila_buscada)
                print(f"[VERIFICACIÓN FINAL] Total filas en Excel: {df_verif.shape[0]}")
                # Diagnóstico: buscar fila 13109501
                fila_13109501 = df_verif[
                    (df_verif["Código de Cuenta"].astype(str) == "13109501")
                    & (df_verif["Documento"].astype(str) == "5727337")
                ]
                print("[VERIFICACIÓN FINAL] Fila 13109501 en Excel generado:")
                print(fila_13109501)
                # Después de terminar la escritura, imprime todas las filas con Documento == '5727337' en el Excel generado
                print("[DEBUG] Todas las filas con Documento == '5727337' EN EXCEL FINAL:")
                print(df_verif[df_verif["Documento"] == "5727337"])
            except Exception as e:
                print(f"[VERIFICACIÓN FINAL] Error al verificar Excel generado: {e}")
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
