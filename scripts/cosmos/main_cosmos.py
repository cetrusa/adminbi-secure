import json
import os
import sys
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE
from email.mime.base import MIMEBase
from email import encoders
from unipath import Path
from planoscosmos import PlanosCosmos

# Configuración de logging
logging.basicConfig(
    filename="logCosmos.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)

class Inicio:
    def __init__(self):
        if getattr(sys, "frozen", False):
            self.dir = os.path.dirname(sys.executable)
            self.name = os.path.basename(self.dir).lower()
            self.id = str(
                os.path.split(os.path.dirname(Path(sys.executable).ancestor(1)))[-1]
            )
            self.nmDt = str(
                os.path.split(os.path.dirname(Path(sys.executable).ancestor(0)))[-1]
            )
        elif __file__:
            self.dir_base = os.path.dirname(__file__)
            self.name = "jyc"
            self.id = "57"
            self.dir_actual = "puentemes"
            self.nmDt = self.dir_actual

    @staticmethod
    def _load_secret(key):
        """Carga un secreto desde secret.json."""
        secret_path = os.path.join(os.path.dirname(__file__), "..", "..", "secret.json")
        if not os.path.exists(secret_path):
            secret_path = "secret.json"
        with open(secret_path) as f:
            secrets = json.loads(f.read())
        return secrets[key]

    def send_email_notification(self, error_message):
        logging.info("Inicia envío de correos")

        host = "smtp.gmail.com"
        port = 587
        username = self._load_secret("SMTP_NOTIFICATION_USER")
        password = self._load_secret("SMTP_NOTIFICATION_PASS")

        from_addr = username
        to_addr = ["cesar.trujillo@amovil.co"]
        
        if not os.path.exists("difusionerror.txt"):
            logging.error("Archivo 'difusionerror.txt' no encontrado.")
            cc_addr = []
        else:
            with open("difusionerror.txt", "r") as file:
                cc_addr = [line.strip() for line in file]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Cc"] = COMMASPACE.join(cc_addr)
        msg["Subject"] = f"Error {self.name}"

        html_message = f"""
        <html>
            <body>
                <h3>Error en la Ejecución</h3>
                <p>{error_message}</p>
                <br>
                <p>Revisar el archivo adjunto para más detalles.</p>
            </body>
        </html>
        """
        msg.attach(MIMEText(html_message, "html"))

        log_file = "logCosmos.txt"
        if os.path.exists(log_file):
            with open(log_file, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={log_file}")
            msg.attach(part)
        else:
            logging.warning(f"El archivo de log '{log_file}' no se encontró para adjuntar.")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                with smtplib.SMTP(host, port) as server:
                    server.starttls()
                    server.login(username, password)
                    server.sendmail(from_addr, to_addr + cc_addr, msg.as_string())
                logging.info("Correo enviado exitosamente.")
                break
            except Exception as e:
                logging.error(f"Error al enviar el correo en intento {attempt + 1}: {e}")
                time.sleep(5)  # Pausa entre reintentos
        else:
            logging.error("No se pudo enviar el correo después de varios intentos.")

    def planos_cosmos(self):
        try:
            planos = PlanosCosmos(self.id, self.nmDt)
            resultado = planos.procesar_datos()
            if resultado.get("success"):
                logging.info("Proceso de extracción completado exitosamente.")
            else:
                raise Exception(resultado.get("error_message", "Error desconocido"))
        except Exception as e:
            error_message = f"Error en el proceso de extracción: {e}"
            logging.error(error_message)
            self.send_email_notification(error_message)

    def run(self):
        try:
            logging.info(f"Inicio de ejecución: {self.name}")
            self.planos_cosmos()
            logging.info("Fin del proceso.")
        except Exception as e:
            error_message = f"Error crítico en la ejecución de Inicio: {e}"
            logging.error(error_message)
            self.send_email_notification(error_message)

if __name__ == "__main__":
    inicio = Inicio()
    inicio.run()
