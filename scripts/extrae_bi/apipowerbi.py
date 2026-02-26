import os, sys
import pandas as pd
from os import path, system
from time import time
from distutils.log import error
from sqlalchemy import create_engine, text
import sqlalchemy
import pymysql
import csv
import zipfile
from zipfile import ZipFile
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import json
import msal
import requests
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE
import ast
from typing import Optional, Callable

with open("secret.json") as f:
    secret = json.loads(f.read())

def get_secret(secret_name, secrets_file="secret.json"):
    try:
        with open(secrets_file) as f:
            secrets = json.loads(f.read())
        return secrets[secret_name]
    except KeyError:
        raise ValueError(f"La variable {secret_name} no existe.")
    except FileNotFoundError:
        raise ValueError(
            f"No se encontró el archivo de configuración {secrets_file}."
        )


####################################################################
import logging

logging.basicConfig(
    filename="log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
####################################################################
logging.info("Inciando Proceso")


class DataBaseConnection:
    def __init__(self, config, mysql_engine=None, sqlite_engine=None):
        self.config = config
        # Asegurarse de que los engines son instancias de conexión válidas y no cadenas
        self.engine_mysql_bi = (
            mysql_engine if mysql_engine else self.create_engine_mysql_bi()
        )
        self.engine_mysql_out = (
            mysql_engine if mysql_engine else self.create_engine_mysql_out()
        )
        self.engine_sqlite = (
            sqlite_engine if sqlite_engine else create_engine("sqlite:///mydata.db")
        )
        # print(self.engine_sqlite)

    def create_engine_mysql_bi(self):
        # Simplificación en la obtención de los parámetros de configuración
        user, password, host, port, database = (
            self.config.get("nmUsrIn"),
            self.config.get("txPassIn"),
            self.config.get("hostServerIn"),
            self.config.get("portServerIn"),
            self.config.get("dbBi"),
        )
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def create_engine_mysql_out(self):
        # Simplificación en la obtención de los parámetros de configuración
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

    def execute_query_mysql(self, query, chunksize=None):
        # Usar el engine correctamente
        with self.create_engine_mysql.connect() as connection:
            cursor = connection.execution_options(isolation_level="READ COMMITTED")
            return pd.read_sql_query(query, cursor, chunksize=chunksize)

    def execute_sql_sqlite(self, sql, params=None):
        # Usar el engine correctamente
        with self.engine_sqlite.connect() as connection:
            return connection.execute(sql, params)

    def execute_query_mysql_chunked(self, query, table_name, chunksize=50000):
        try:
            # Primero, elimina la tabla si ya existe
            self.eliminar_tabla_sqlite(table_name)
            # Luego, realiza la consulta y almacena los resultados en SQLite
            with self.engine_mysql.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")

                for chunk in pd.read_sql_query(query, con=cursor, chunksize=chunksize):
                    chunk.to_sql(
                        name=table_name,
                        con=self.engine_sqlite,
                        if_exists="append",
                        index=False,
                    )

                # Retorna el total de registros almacenados en la tabla SQLite
                with self.engine_sqlite.connect() as connection:
                    # Modificar aquí para usar fetchone correctamente
                    total_records = connection.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    ).fetchone()[0]
                return total_records

        except Exception as e:
            logging.error(f"Error al ejecutar el query: {e}")
            print(f"Error al ejecutar el query: {e}")
            raise
        print("terminamos de ejecutar el query")

    def eliminar_tabla_sqlite(self, table_name):
        sql = text(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine_sqlite.connect() as connection:
            connection.execute(sql)
class Api_PowerBi_Config:
    """Clase para manejar la configuración y conexiones a bases de datos."""

    def __init__(self, database_name: str):
        self.config_basic = ConfigBasic(database_name)
        self.config = self.config_basic.config
        self.engine_mysql_bi = self._create_engine_mysql_bi()
        self.engine_mysql_out = self._create_engine_mysql_out()
        self.engine_sqlite = con.ConexionSqlite()

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


class Api_PowerBi:
    def __init__(
        self,
        config: Api_PowerBi_Config,
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

    def request_access_token_refresh(self):
        """
        Obtiene un token de acceso para las APIs de Power BI.
        Mejorado con mejor registro de errores para diagnosticar problemas de conexión.
        """
        try:
            print("Obteniendo token de acceso para Power BI...")

            # Obtener credenciales
            app_id = get_secret("CLIENT_ID")
            tenant_id = get_secret("TENANT_ID")

            # Primero intentar obtener de la configuración
            username = self.config.get("nmUsrPowerbi")
            password = self.config.get("txPassPowerbi")

            # Si no están en la configuración, intentar obtenerlos de secret.json
            if not username:
                print(
                    "nmUsrPowerbi no encontrado en config, intentando obtener de secret.json"
                )
                try:
                    username = get_secret("POWER_BI_USER")
                    print(f"Usuario obtenido de secret.json: {username}")
                except Exception as e:
                    print(f"Error al obtener POWER_BI_USER de secret.json: {e}")

            if not password:
                print(
                    "txPassPowerbi no encontrado en config, intentando obtener de secret.json"
                )
                try:
                    password = get_secret("POWER_BI_PASS")
                    print("Contraseña obtenida de secret.json")
                except Exception as e:
                    print(f"Error al obtener POWER_BI_PASS de secret.json: {e}")

            # Validar que las credenciales existan
            if not username:
                error_message = "Error: No se encontró el usuario de Power BI en la configuración (nmUsrPowerbi) ni en secret.json (POWER_BI_USER)"
                print(error_message)
                logging.error(error_message)
                raise ValueError(error_message)

            if not password:
                error_message = "Error: No se encontró la contraseña de Power BI en la configuración (txPassPowerbi) ni en secret.json (POWER_BI_PASS)"
                print(error_message)
                logging.error(error_message)
                raise ValueError(error_message)

            # Registro de información de diagnóstico (sin mostrar contraseña)
            print(
                f"Datos de conexión - App ID: {app_id}, Tenant ID: {tenant_id}, Usuario: {username}"
            )
            print(f"Dataset ID configurado: {self.config.get('dataset_id_powerbi')}")

            # Configurar URL de autoridad y ámbitos
            authority_url = "https://login.microsoftonline.com/" + tenant_id
            scopes = ["https://analysis.windows.net/powerbi/api/.default"]

            # Inicializar aplicación cliente MSAL
            client = msal.PublicClientApplication(app_id, authority=authority_url)

            # Obtener token con usuario y contraseña
            print("Solicitando token con usuario y contraseña...")
            token_response = client.acquire_token_by_username_password(
                username=username,
                password=password,
                scopes=scopes,
            )

            # Verificar si la respuesta contiene un token de acceso
            if "access_token" in token_response:
                token_length = len(token_response["access_token"])
                print(
                    f"Token obtenido exitosamente. Longitud del token: {token_length} caracteres"
                )
                return token_response.get("access_token")
            else:
                # Si no hay token, registrar el error detallado
                error_code = token_response.get("error", "Error desconocido")
                error_description = token_response.get(
                    "error_description", "Sin descripción"
                )
                error_message = (
                    f"Error al obtener token: {error_code} - {error_description}"
                )
                print(error_message)
                logging.error(error_message)
                raise Exception(error_message)

        except Exception as e:
            error_message = f"Excepción en request_access_token_refresh: {str(e)}"
            print(error_message)
            logging.error(error_message)
            raise Exception(error_message)

    def run_datasetrefresh(self):
        """
        Inicia el proceso de refresh del dataset y espera a que termine.
        Mejorado con mejor registro de errores.
        """
        try:
            print("Iniciando refresh de Power BI...")

            # Obtener token de acceso
            access_id = self.request_access_token_refresh()
            if not access_id:
                error_message = "No se pudo obtener un token de acceso válido"
                print(error_message)
                return {"success": False, "error_message": error_message}

            # Obtener ID del dataset
            dataset_id = self.config.get("dataset_id_powerbi")
            if not dataset_id:
                error_message = "No se encontró el ID del dataset en la configuración"
                print(error_message)
                return {"success": False, "error_message": error_message}

            print(f"Dataset ID a actualizar: {dataset_id}")

            # Preparar endpoint y headers
            endpoint = (
                f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
            )
            headers = {"Authorization": f"Bearer {access_id}"}

            # Hacer la solicitud para iniciar el refresh
            print(f"Enviando solicitud POST a {endpoint}")
            response = requests.post(endpoint, headers=headers)

            # Registrar información completa de la respuesta
            print(f"Respuesta: Código {response.status_code} - {response.reason}")

            if response.status_code == 202:
                print("Refresh iniciado correctamente. Verificando estado...")
                result = self.get_status_history()
                print(f"Resultado final del refresh: {result}")
                return result
            else:
                # Intentar obtener más detalles del error
                error_detail = "Sin detalles adicionales"
                try:
                    if response.content:
                        error_detail = response.json()
                except:
                    error_detail = response.text

                error_message = f"Error al iniciar refresh. Código: {response.status_code}, Razón: {response.reason}, Detalles: {error_detail}"
                print(error_message)
                return {"success": False, "error_message": error_message}

        except Exception as e:
            error_message = f"Excepción en run_datasetrefresh: {str(e)}"
            print(error_message)
            logging.error(error_message)
            return {"success": False, "error_message": error_message}

    def run_datasetrefresh_solo_inicio(self):
        access_id = self.request_access_token_refresh()

        dataset_id = self.config.get("dataset_id_powerbi")
        endpoint = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
        headers = {"Authorization": f"Bearer " + access_id}

        response = requests.post(endpoint, headers=headers)
        if response.status_code == 202:
            print("Dataset refreshed")
        else:
            print(response.reason)
            print(response.json())

    def get_report_id(self):
        report_id = self.config.get("report_id_powerbi")
        return report_id

    def generate_embed_token(self):
        """
        Genera el token de incrustación usando usuario y contraseña,
        igual que se hace para refresh, pero en vez de llamar al endpoint de refresh,
        llama al endpoint GenerateToken.
        """
        try:
            # 1. Obtener el token de Azure AD con usuario y contraseña
            username = self.config.get("nmUsrPowerbi")  # tu usuario
            password = self.config.get("txPassPowerbi")  # tu contraseña
            tenant_id = get_secret("TENANT_ID")
            client_id = get_secret("CLIENT_ID")

            authority_url = f"https://login.microsoftonline.com/{tenant_id}"
            scopes = ["https://analysis.windows.net/powerbi/api/.default"]

            public_app = msal.PublicClientApplication(
                client_id, authority=authority_url
            )
            token_response = public_app.acquire_token_by_username_password(
                username=username, password=password, scopes=scopes
            )

            if "access_token" not in token_response:
                raise Exception(
                    f"Error en token_response: {token_response.get('error_description')}"
                )

            user_access_token = token_response["access_token"]

            # 2. Llamar a la API para GenerateToken
            workspace_id = self.config.get("group_id_powerbi")
            report_id = self.config.get("report_id_powerbi")
            endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/GenerateToken"

            headers = {
                "Authorization": f"Bearer {user_access_token}",
                "Content-Type": "application/json",
            }
            payload = {"accessLevel": "View"}

            response = requests.post(endpoint, headers=headers, json=payload)
            if response.status_code == 200:
                return response.json().get("token")
            else:
                msg = (
                    f"Error al generar token: {response.status_code} - {response.text}"
                )
                print(msg)
                return {"error": msg}

        except Exception as e:
            print(f"Excepción en generate_embed_token: {e}")
            return {"error": str(e)}

    # def generate_embed_token(self):
    #     try:
    #         access_id = self.request_access_token_refresh()
    #         if not access_id:
    #             logging.error("No se pudo obtener el token de acceso.")
    #             return {"error": "No se pudo obtener el token de acceso."}

    #         workspace_id = self.config.get("group_id_powerbi")
    #         report_id = self.get_report_id()
    #         endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/GenerateToken"

    #         headers = {
    #             "Authorization": f"Bearer {access_id}",
    #             "Content-Type": "application/json",
    #         }
    #         payload = {"accessLevel": "View"}

    #         response = requests.post(endpoint, headers=headers, json=payload)

    #         if response.status_code == 200:
    #             return response.json().get("token", None)
    #         else:
    #             logging.error(f"Error al generar token: {response.status_code} - {response.text}")
    #             return {"error": f"Error al generar token: {response.text}"}
    #     except Exception as e:
    #         logging.error(f"Excepción en generate_embed_token: {e}")
    #         return {"error": f"Excepción en generate_embed_token: {str(e)}"}

    def get_status_history(self):
        """
        Verifica el estado del refresh más reciente.
        Mejorado con mejor registro de errores.
        """
        try:
            print("Verificando estado del refresh...")

            # Obtener nuevo token para cada verificación
            access_id = self.request_access_token_refresh()
            dataset_id = self.config.get("dataset_id_powerbi")
            endpoint = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes?$top=1"
            headers = {"Authorization": f"Bearer {access_id}"}

            max_attempts = 15
            refresh_interval = 240  # 4 minutos
            attempt = 1
            refresh_status = None  # Estado inicial desconocido

            print(
                f"Iniciando verificación de estado. Máximo {max_attempts} intentos, intervalo {refresh_interval} segundos."
            )

            while attempt <= max_attempts:
                print(f"Intento {attempt}/{max_attempts} de verificación de estado...")

                response = requests.get(endpoint, headers=headers)
                print(f"Respuesta: Código {response.status_code} - {response.reason}")

                if response.status_code == 200:
                    try:
                        # Registrar la respuesta completa para diagnóstico
                        response_data = response.json()
                        print(f"Datos de respuesta: {json.dumps(response_data)}")

                        # Verificar si hay datos en la respuesta
                        if "value" in response_data and len(response_data["value"]) > 0:
                            refresh_item = response_data["value"][0]
                            refresh_status = refresh_item.get("status")
                            request_id = refresh_item.get("requestId", "desconocido")

                            print(
                                f"Estado actual del refresh: {refresh_status}, ID: {request_id}"
                            )

                            if refresh_status == "Completed":
                                print("Refresh completado exitosamente.")
                                return {"success": True, "request_id": request_id}
                            elif refresh_status == "Failed":
                                error_message = refresh_item.get(
                                    "error", "Error desconocido"
                                )
                                print(
                                    f"El refresh falló. Mensaje de error: {error_message}"
                                )
                                return {
                                    "success": False,
                                    "error_message": error_message,
                                    "request_id": request_id,
                                }
                        else:
                            print("No se encontraron datos de refresh en la respuesta.")
                            if attempt == max_attempts:
                                return {
                                    "success": False,
                                    "error_message": "No se encontraron datos de refresh en la respuesta.",
                                }

                    except (json.decoder.JSONDecodeError, IndexError) as e:
                        error_message = (
                            f"Error al procesar respuesta de verificación: {str(e)}"
                        )
                        print(error_message)
                        if attempt == max_attempts:
                            return {"success": False, "error_message": error_message}
                else:
                    error_message = f"Respuesta inesperada al verificar estado: {response.status_code} - {response.reason}"
                    print(error_message)
                    if attempt == max_attempts:
                        return {"success": False, "error_message": error_message}

                # Solo esperar si no es el último intento
                if attempt < max_attempts:
                    print(
                        f"Esperando {refresh_interval} segundos antes del siguiente intento..."
                    )
                    time.sleep(refresh_interval)

                attempt += 1

            # Si se agotan los intentos sin completarse
            error_message = (
                "El refresh no se completó en el número de intentos especificado."
            )
            print(error_message)
            return {"success": False, "error_message": error_message}

        except Exception as e:
            error_message = f"Excepción en get_status_history: {str(e)}"
            print(error_message)
            logging.error(error_message)
            return {"success": False, "error_message": error_message}

    def check_refresh_status(self, refresh_id):
        """
        Verifica el estado de un refresh específico usando su ID.

        Args:
            refresh_id: ID del refresh a verificar

        Returns:
            Diccionario con el estado del refresh y detalles adicionales
        """
        try:
            access_id = self.request_access_token_refresh()
            dataset_id = self.config.get("dataset_id_powerbi")
            endpoint = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes/{refresh_id}"
            headers = {"Authorization": f"Bearer {access_id}"}

            response = requests.get(endpoint, headers=headers)

            if response.status_code == 200:
                refresh_data = response.json()
                status = refresh_data.get("status")

                result = {
                    "status": status,
                    "request_id": refresh_id,
                    "start_time": refresh_data.get("startTime"),
                    "end_time": (
                        refresh_data.get("endTime")
                        if status in ["Completed", "Failed"]
                        else None
                    ),
                }

                # Si falló, agregar detalles del error
                if status == "Failed" and "error" in refresh_data:
                    result["error"] = {
                        "code": refresh_data.get("error", {}).get("code"),
                        "message": refresh_data.get("error", {}).get(
                            "message", "Error desconocido"
                        ),
                    }

                return result
            else:
                print(
                    f"Error al verificar estado del refresh {refresh_id}: {response.status_code} - {response.reason}"
                )
                return {"status": "Unknown", "error": response.reason}

        except Exception as e:
            print(f"Excepción al verificar estado del refresh {refresh_id}: {str(e)}")
            return {"status": "Error", "error": str(e)}

    def send_email(self, error_message):
        host = "smtp.gmail.com"
        port = 587
        username = "torredecontrolamovil@gmail.com"
        password = "dldaqtceiesyybje"

        from_addr = "torredecontrolamovil@gmail.com"
        to_addr = ["cesar.trujillo@amovil.co", "soporte@amovil.co"]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Subject"] = f"Error Bi {self.config.get('nmEmpresa')}"

        body = f"Error en el Bi de {self.config.get('nmEmpresa')}, {error_message}"

        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(username, password)
            server.sendmail(from_addr, to_addr, msg.as_string())
            server.quit()

    def run(self):
        """
        Método principal para ejecutar la extracción BI.
        Puedes personalizar este método para llamar a la lógica principal de extracción.
        Por defecto, llama a run_datasetrefresh.
        """
        return self.run_datasetrefresh()
