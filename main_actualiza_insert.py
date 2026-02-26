import os, sys

import pandas
import sqlalchemy
import mariadb
import pymysql
from charset_normalizer import md__mypyc
from scripts.conexion import Conexion as con
import json
import pandas as pd

from scripts.extrae_bi.extrae_bi_insert import Extrae_Bi
from scripts.extrae_bi.apipowerbi import Api_PowerBi
from scripts.extrae_bi.uau import CompiUpdate

from unipath import Path
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE
from email.mime.base import MIMEBase
from email import encoders
from sqlalchemy import create_engine, text
from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con
import win32com.client
import logging


def get_secret(secret_name, secrets_file="secret.json"):
    try:
        with open(secrets_file) as f:
            secrets = json.loads(f.read())
        return secrets[secret_name]
    except KeyError:
        raise ValueError(f"La variable {secret_name} no existe.")
    except FileNotFoundError:
        raise ValueError(f"No se encontró el archivo de configuración {secrets_file}.")


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


class Inicio:
    def __init__(self):
        if getattr(sys, "frozen", False):
            self.dir_actual = os.path.dirname(sys.executable)
            self.name = os.path.basename(self.dir_actual).lower()
            self.name = str(
                os.path.split(os.path.dirname(Path(sys.executable).ancestor(1)))[-1]
            )
            self.nmDt = str(
                os.path.split(os.path.dirname(Path(sys.executable).ancestor(0)))[-1]
            )
        elif __file__:
            self.dir_actual = os.path.dirname(__file__)
            self.name = str("distritiendas_aws")
            self.dir_actual = str("puentemes")
            self.nmDt = self.dir_actual

        self.configurar(self.name)

    def configurar(self, database_name):
        try:
            self.config_basic = ConfigBasic(database_name)
            self.config = self.config_basic.config
            # config_basic.print_configuration()
            # print(self.config.get("txProcedureExtrae", []))
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_out = self.db_connection.engine_mysql_out
            self.correo_config()
            # print("Configuraciones preliminares de actualización terminadas")
        except Exception as e:
            logging.error(f"Error al inicializar Actualización: {e}")
            raise

    def correo_config(self):
        sql = text("SELECT * FROM powerbi_adm.conf_tipo WHERE nbTipo = '6';")
        # print(sql)
        df = self.config_basic.execute_sql_query(sql)
        # print(df)
        if not df.empty:
            # Corrige la asignación aquí
            self.config["nmUsrCorreo"] = df["nmUsr"].iloc[0]
            self.config["txPassCorreo"] = df["txPass"].iloc[0]
        else:
            # Considera si necesitas manejar el caso de un DataFrame vacío de manera diferente
            print("No se encontraron configuraciones de Correo.")

    def setup_date_config(self):
        date_config = self.fetch_date_config(self.config["nmDt"])
        if not date_config:
            date_config = self.fetch_date_config(str("puentemes"))  # Valor por defecto

        # Actualiza el diccionario de configuración con los valores de date_config
        # print(date_config)
        self.config.update(date_config)

    def fetch_date_config(self, nmDt):
        sql = f"SELECT * FROM powerbi_adm.conf_dt WHERE nmDt = '{nmDt}';"
        # print(sql)
        df = self.config_basic.execute_sql_query(sql)
        if not df.empty:
            # Obtener los valores de IdtReporteIni y IdtReporteFin en una sola consulta
            txDtIni = df["txDtIni"].iloc[0]
            txDtFin = df["txDtFin"].iloc[0]

            # La consulta SQL para obtener IdtReporteIni y IdtReporteFin
            sql_report_date_ini = text(txDtIni)
            sql_report_date_fin = text(txDtFin)
            report_date_df_ini = self.config_basic.execute_sql_query(
                sql_report_date_ini
            )
            report_date_df_fin = self.config_basic.execute_sql_query(
                sql_report_date_fin
            )
            if not report_date_df_ini.empty and not report_date_df_fin.empty:
                self.IdtReporteIni = report_date_df_ini["IdtReporteIni"].iloc[0]
                self.IdtReporteFin = report_date_df_fin["IdtReporteFin"].iloc[0]

    def send_email_notification(self, error_message):
        logging.info("Inicia envío de correos")
        # Indica que vas a usar las variables globales

        host = "smtp.gmail.com"
        port = 587
        username = self.config["nmUsrCorreo"]
        password = self.config["txPassCorreo"]

        from_addr = "torredecontrolamovil@gmail.com"
        to_addr = [
            "cesar.trujillo@amovil.co",
        ]
        with open("difusionerror.txt", "r") as file:
            cc_addr = [line.strip() for line in file]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Cc"] = COMMASPACE.join(cc_addr)
        msg["Subject"] = f"Error {self.name}"

        html_message = f"""
        <html>
            <head>
            <style>
            body {{ font-family: Arial; }}
            p {{ font-size: 12px; }}
            </style>
            </head>
            <body>
                <h3>Verifica el log,</h3>
                <p>Adjunto encontrará el log de ejecución observe detalladamente cual proceso no se completo</p>
                <br>
            </body>
        </html>
        """

        msg.attach(MIMEText(html_message, "html"))

        filename = "log.txt"
        # Adjuntar el archivo
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {filename}")

        msg.attach(part)

        # Intentar enviar el correo hasta tres veces en caso de error
        max_retries = 3

        for _ in range(max_retries):
            try:
                with smtplib.SMTP(host, port) as server:
                    server.starttls()
                    server.login(username, password)
                    server.sendmail(from_addr, to_addr + cc_addr, msg.as_string())
                # Si el correo se envió con éxito, salir del bucle
                break
            except Exception as e:
                logging.error(f"Error al enviar el correo: {e}")
                # Puedes agregar un retraso aquí si lo deseas antes de reintentar

        logging.info("Proceso de envío de correo completado")

    def extrae_bi(self):
        try:
            extrae = Extrae_Bi(
                database_name=self.name,
                IdtReporteIni=self.IdtReporteIni,
                IdtReporteFin=self.IdtReporteFin,
            )
            extrae.extractor()
            logging.info("Termina proceso de extracción")
        except Exception as e:
            error_message = "Error en el proceso de extracción: " + str(e)
            logging.error(error_message)
            self.send_email_notification(error_message)

    def actualiza_bi(self):
        try:
            actualizabi = Api_PowerBi(
                database_name=self.name,
                IdtReporteIni=self.IdtReporteIni,
                IdtReporteFin=self.IdtReporteFin,
            )
            time.sleep(5)
            actualizabi.run_datasetrefresh_solo_inicio()
            # actualizabi.run_datasetrefresh()
            logging.info("Proceso de actualización de Power BI completado")
        except Exception as e:
            error_message = "Error en el proceso de actualización de Power BI: " + str(
                e
            )
            logging.error(error_message)
            self.send_email_notification(error_message)

    def refresh_excel(self):
        try:
            compi = CompiUpdate(database_name=self.name)
            compi.refresh_excel()
            logging.info("Proceso de actualización de Excel completado")
        except Exception as e:
            error_message = "Error en el proceso de actualización de Excel: " + str(e)
            logging.error(error_message)
            self.send_email_notification(error_message)

    def run(self):
        try:
            print(self.name)
            self.fetch_date_config(self.nmDt)
            self.extrae_bi()  # Llama al primer proceso
            # self.actualiza_bi()  # Llama al segundo proceso
            # self.refresh_excel()  # Llama al tercer proceso
            logging.info("Fin del proceso")
        except Exception as e:
            error_message = (
                "Se ha producido un error en la ejecución de Inicio: " + str(e)
            )
            logging.error(error_message)
            self.send_email_notification(error_message)


if __name__ == "__main__":
    inicio = Inicio()
    inicio.run()
