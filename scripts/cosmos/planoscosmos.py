import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import ast
from datetime import datetime, timedelta
import zipfile
import shutil
import json
import subprocess


# Configuración del logging (solo para ejecución standalone)
logging.basicConfig(
    filename="logCosmos.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)


def get_secret(secret_name, secrets_file="secret.json"):
    try:
        with open(secrets_file) as f:
            secrets = json.loads(f.read())
        return secrets[secret_name]
    except KeyError:
        raise KeyError(f"La variable '{secret_name}' no existe en el archivo de configuración.")
    except FileNotFoundError:
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {secrets_file}.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error al leer el archivo de configuración '{secrets_file}': {e}")


class DataBaseConnection:
    def __init__(self, config, output_dir="./output", mysql_engine=None, sqlite_engine=None):
        self.config = config
        self.engine_mysql = mysql_engine if mysql_engine else self.create_engine_mysql()
        self.db_sqlite = os.path.join(output_dir, 'mydata.db')
        self.engine_sqlite = (
            sqlite_engine if sqlite_engine else create_engine(f"sqlite:///{self.db_sqlite}")
        )

    def create_engine_mysql(self):
        # Importar aquí para permitir uso desde Django y standalone
        scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        from conexion import Conexion as con

        user, password, host, port, database = (
            self.config.get("nmUsrOut"),
            self.config.get("txPassOut"),
            self.config.get("hostServerOut"),
            self.config.get("portServerOut"),
            self.config.get("dbSidis"),
        )
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def execute_query_mysql_chunked(self, archivo, query, table_name, parametros, chunksize=50000):
        try:
            self.eliminar_tabla_sqlite(table_name)
            with self.engine_mysql.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")
                for chunk in pd.read_sql_query(query, con=cursor, chunksize=chunksize, params=parametros):
                    chunk.to_sql(
                        name=table_name,
                        con=self.engine_sqlite,
                        if_exists="append",
                        index=False,
                    )
                with self.engine_sqlite.connect() as connection:
                    total_records = connection.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    ).fetchone()[0]
                return total_records
        except Exception as e:
            logging.error(f"Error al ejecutar el query: {e}")
            raise

    def eliminar_tabla_sqlite(self, table_name):
        sql = text(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine_sqlite.connect() as connection:
            connection.execute(sql)

    def eliminar_base_datos_sqllite(self):
        # Cerrar conexiones SQLite
        self.engine_sqlite.dispose()
        try:
            if os.path.exists(self.db_sqlite):
                os.remove(self.db_sqlite)
                logging.info(f"Base de datos SQLite eliminada: {self.db_sqlite}")
        except PermissionError as e:
            logging.error(f"No se pudo eliminar el archivo SQLite: {e}")
            raise


class PlanosCosmos:
    def __init__(
        self,
        database_name=None,
        nmDt=None,
        *,
        empresa_id_cosmos=None,
        fecha_ini=None,
        fecha_fin=None,
        tx_cosmos=None,
        ftps_config=None,
        enviar_ftps=True,
        base_output_dir=None,
    ):
        """
        Inicializa PlanosCosmos.

        Modo web (desde task Django):
            PlanosCosmos(
                database_name="jyc",
                empresa_id_cosmos="CO-CBIA-DTR-0093",
                fecha_ini="2026-01-01",
                fecha_fin="2026-03-19",
                tx_cosmos=[1, 2, 3],
                ftps_config={"host": ..., "port": ..., ...},
                enviar_ftps=True,
                base_output_dir="/path/to/media/cosmos/jyc",
            )

        Modo standalone (compatibilidad con main_cosmos.py):
            PlanosCosmos(id_or_name, nmDt)
        """
        self._log_buffer = []
        self.enviar_ftps = enviar_ftps

        # Configurar rutas de salida
        if base_output_dir:
            self.output_dir = os.path.join(base_output_dir, "output")
            self.historical_dir = os.path.join(base_output_dir, "historico")
        else:
            self.output_dir = "./output"
            self.historical_dir = "./historico"

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.historical_dir, exist_ok=True)

        # Configurar empresa
        self.configurar(database_name, nmDt)

        # Empresa ID Cosmos
        if empresa_id_cosmos:
            self.empresa_id = empresa_id_cosmos
        else:
            # Fallback: intentar leer de config o usar hardcoded
            self.empresa_id = self.config.get("cosmos_empresa_id", "CO-CBIA-DTR-0093")

        self.fecha_actual = datetime.now().strftime('%Y%m%d')

        # Fechas
        if fecha_ini and fecha_fin:
            self.IdtReporteIni = fecha_ini
            self.IdtReporteFin = fecha_fin
        else:
            self.IdtReporteIni, self.IdtReporteFin = self.calculate_dates()

        # Lista de IDs SQL Cosmos
        if tx_cosmos is not None:
            self._tx_cosmos = tx_cosmos if isinstance(tx_cosmos, list) else list(tx_cosmos)
        else:
            self._tx_cosmos = None  # Se lee de config en procesar_datos

        # Credenciales FTPS
        self._ftps_config = ftps_config

    def _log(self, msg):
        """Agrega mensaje al buffer de log interno y al logging estándar."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] {msg}"
        self._log_buffer.append(entry)
        logging.info(msg)

    def get_log(self):
        """Retorna el log completo como string."""
        return "\n".join(self._log_buffer)

    def limpiar_directorio(self):
        """Limpia todos los archivos de la carpeta de salida."""
        try:
            for archivo in os.listdir(self.output_dir):
                archivo_path = os.path.join(self.output_dir, archivo)
                if os.path.isfile(archivo_path):
                    os.remove(archivo_path)
                    self._log(f"Archivo eliminado de la carpeta output: {archivo_path}")
        except Exception as e:
            self._log(f"Error al limpiar la carpeta de salida: {e}")
            raise

    def configurar(self, database_name, nmDt):
        scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        from config import ConfigBasic

        self.config_basic = ConfigBasic(database_name, nmDt)
        self.config = self.config_basic.config
        self.db_connection = DataBaseConnection(
            config=self.config, output_dir=self.output_dir
        )

    def calculate_dates(self):
        fecha_final = datetime.now()
        fecha_inicial = fecha_final - timedelta(days=45)
        self._log("Calculando fechas: 45 días hacia atrás")
        return fecha_inicial.strftime("%Y-%m-%d"), fecha_final.strftime("%Y-%m-%d")

    def generar_nombre_archivo(self, archivo, ext=".csv"):
        archivo_nombre = f"{self.empresa_id}_{archivo}_{self.fecha_actual}{ext}"
        archivo_ruta = os.path.join(self.output_dir, archivo_nombre)
        self._log(f"Generando archivo: {archivo_nombre}")
        return archivo_nombre, archivo_ruta

    def guardar_datos_csv(self, table_name, file_path):
        with self.db_connection.engine_sqlite.connect() as connection:
            for chunk in pd.read_sql_query(
                f"SELECT * FROM {table_name}", connection, chunksize=50000
            ):
                chunk.to_csv(file_path, mode='a', index=False, sep='|', header=False, lineterminator='\r\n')
        self._log(f"Datos guardados en: {file_path}")

    def generate_sqlout_extrae(self, reporte):
        import time
        intento = 0
        while intento < 3:
            try:
                sql = text("SELECT * FROM powerbi_adm.conf_sql_cosmos WHERE nmReporte = :reporte")
                result = self.config_basic.execute_sql_query(sql, params={"reporte": reporte})
                df = result

                if not df.empty:
                    self.txSqlExtrae = df["txSqlExtrae"].iloc[0]
                    self._log(f"Consulta SQL obtenida para reporte: {reporte}")
                    return text(self.txSqlExtrae)
            except Exception as e:
                logging.error(f'Error en la construcción de la consulta SQL: {str(e)}')
                intento += 1
                if intento < 3:
                    logging.info(f'Reintentando construir la consulta (Intento {intento}/3)...')
                    time.sleep(5)
                else:
                    logging.error('Se agotaron los intentos para construir la consulta SQL.')
                    break
        return None

    def procesar_archivo(self, archivo, reporte):
        sqlout = self.generate_sqlout_extrae(reporte)
        table_name = f"my_table_{archivo}"
        parametros = {"fi": self.IdtReporteIni, "ff": self.IdtReporteFin, "IdDs": self.empresa_id}
        total_records = self.db_connection.execute_query_mysql_chunked(
            archivo, sqlout, table_name, parametros
        )
        archivo_nombre, archivo_ruta = self.generar_nombre_archivo(archivo)
        self.guardar_datos_csv(table_name, archivo_ruta)
        self.db_connection.eliminar_tabla_sqlite(table_name)
        self._log(f"Archivo procesado: {archivo_nombre} ({total_records} registros)")
        return archivo_ruta, total_records

    def procesar_datos(self):
        """
        Procesa todos los reportes Cosmos configurados.
        Retorna dict con resultados estructurados.
        """
        try:
            self.limpiar_directorio()

            archivos_generados = []
            archivos_nombres = []
            total_registros = 0

            # Obtener lista de IDs SQL
            if self._tx_cosmos is not None:
                tx_cosmos_list = self._tx_cosmos
            else:
                txCosmos_str = self.config.get("txCosmos")
                if isinstance(txCosmos_str, str):
                    try:
                        tx_cosmos_list = ast.literal_eval(txCosmos_str)
                    except (ValueError, SyntaxError) as e:
                        self._log(f"Error al convertir txCosmos a lista: {e}")
                        tx_cosmos_list = []
                else:
                    tx_cosmos_list = txCosmos_str or []

            self._log(f"Reportes a procesar: {tx_cosmos_list}")

            for nbSql in tx_cosmos_list:
                archivo_query = text(
                    "SELECT nmReporte, txDescripcion FROM powerbi_adm.conf_sql_cosmos WHERE nbSql = :nbSql"
                )
                result = self.config_basic.execute_sql_query(archivo_query, params={"nbSql": nbSql})
                if not result.empty:
                    archivo = result.iloc[0]["txDescripcion"]
                    reporte = result.iloc[0]["nmReporte"]
                    archivo_ruta, records = self.procesar_archivo(archivo, reporte)
                    archivos_generados.append(archivo_ruta)
                    archivos_nombres.append(os.path.basename(archivo_ruta))
                    total_registros += records

            self._log(f"Archivos generados: {archivos_nombres}")

            # Crear ZIP
            zip_path = os.path.join(self.output_dir, f"{self.empresa_id}_{self.fecha_actual}.zip")
            self.crear_y_añadir_a_zip(zip_path, archivos_generados)
            self._log(f"ZIP creado: {zip_path}")

            # Enviar por FTPS
            enviado_ftps = False
            if self.enviar_ftps:
                try:
                    ftps_cfg = self._get_ftps_config()
                    self._log("Iniciando envío FTPS...")
                    self.send_files_via_ftps(
                        zip_path,
                        ftps_cfg["host"],
                        ftps_cfg["port"],
                        ftps_cfg["user"],
                        ftps_cfg["pass"],
                        ftps_cfg["certificate"],
                        ftps_cfg["remote_dir"],
                    )
                    enviado_ftps = True
                    self._log("Envío FTPS completado exitosamente")
                except Exception as e:
                    self._log(f"Error durante el envío por FTPS: {e}")

            # Mover ZIP al histórico
            zip_historico = os.path.join(self.historical_dir, os.path.basename(zip_path))
            self.mover_archivo(zip_path, zip_historico)

            # Limpiar
            self.limpiar_directorio()
            self.db_connection.eliminar_base_datos_sqllite()

            return {
                "success": True,
                "total_registros": total_registros,
                "archivos": archivos_nombres,
                "zip_path": zip_historico,
                "enviado_ftps": enviado_ftps,
                "log": self.get_log(),
            }

        except Exception as e:
            self._log(f"Error procesando datos: {e}")
            return {
                "success": False,
                "total_registros": 0,
                "archivos": [],
                "zip_path": "",
                "enviado_ftps": False,
                "log": self.get_log(),
                "error_message": str(e),
            }

    def _get_ftps_config(self):
        """Obtiene config FTPS desde parámetros o secret.json (fallback)."""
        if self._ftps_config:
            return self._ftps_config
        # Fallback: leer de secret.json (modo standalone)
        return {
            "host": get_secret("FTPS_HOST"),
            "port": int(get_secret("FTPS_PORT")),
            "user": get_secret("FTPS_USER"),
            "pass": get_secret("FTPS_PASS"),
            "remote_dir": get_secret("FTPS_REMOTE_DIR"),
            "certificate": get_secret("FTPS_CERTIFICATE_FINGERPRINT"),
        }

    def crear_y_añadir_a_zip(self, zip_path, archivos):
        """Crea un archivo ZIP con los archivos especificados."""
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for archivo in archivos:
                zipf.write(archivo, arcname=os.path.basename(archivo))
                self._log(f"Archivo añadido al ZIP: {os.path.basename(archivo)}")

    def send_files_via_ftps(self, zip_path, hostname, port, username, password, certificate_fingerprint, remote_dir=""):
        """Enviar archivos utilizando WinSCP CLI."""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            winscp_path = os.path.join(script_dir, "WinSCP", "winscp.com")

            if not os.path.isfile(winscp_path):
                raise FileNotFoundError(f"No se encontró WinSCP en la ruta especificada: {winscp_path}")
            if not os.path.isfile(zip_path):
                raise FileNotFoundError(f"No se encontró el archivo ZIP especificado: {zip_path}")

            winscp_command = [
                winscp_path,
                "/command",
                f"open ftps://{username}:{password}@{hostname}:{port} -implicit -certificate=\"{certificate_fingerprint}\"",
                f"cd {remote_dir}" if remote_dir else "",
                f"put {zip_path}",
                "exit"
            ]

            winscp_command = [cmd for cmd in winscp_command if cmd]

            self._log(f"Ejecutando WinSCP hacia {hostname}:{port}")
            result = subprocess.run(winscp_command, capture_output=True, text=True)

            if result.returncode != 0:
                self._log(f"Error WinSCP: {result.stderr}")
                raise Exception(f"WinSCP falló con el siguiente error:\n{result.stderr}")

            self._log(f"Transferencia FTPS completada")

        except Exception as e:
            self._log(f"Error durante el envío por WinSCP: {e}")
            raise

    def mover_archivo(self, origen, destino):
        shutil.move(origen, destino)
