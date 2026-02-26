import zipfile
import os
import pandas as pd
import logging

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from sqlalchemy import create_engine, text
from django.contrib import sessions
import re
import ast
from django.core.exceptions import ImproperlyConfigured
import json

logging.basicConfig(
    filename="log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
logging.info("Iniciando Proceso CargueZip")


def get_secret(secret_name, secrets_file="secret.json"):
    try:
        with open(secrets_file) as f:
            secrets = json.loads(f.read())
        return secrets[secret_name]
    except KeyError:
        raise ImproperlyConfigured(f"La variable {secret_name} no existe")
    except FileNotFoundError:
        raise ImproperlyConfigured(
            f"No se encontró el archivo de configuración {secrets_file}"
        )


class DataBaseConnection:
    """
    Clase para manejar las conexiones a bases de datos MySQL y SQLite.

    Esta clase facilita la conexión a las bases de datos y la ejecución de consultas SQL,
    incluyendo la ejecución de consultas grandes en fragmentos (chunks).

    Attributes:
        config (dict): Configuración utilizada para las conexiones a las bases de datos.
        engine_mysql (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos MySQL.
        engine_sqlite (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos SQLite.
    """

    def __init__(self, config):
        """
        Inicializa la instancia de DataBaseConnection con la configuración proporcionada.

        Args:
            config (dict): Configuración para las conexiones a las bases de datos.
            mysql_engine (sqlalchemy.engine.base.Engine, opcional): Motor SQLAlchemy para la base de datos MySQL.
            sqlite_engine (sqlalchemy.engine.base.Engine, opcional): Motor SQLAlchemy para la base de datos SQLite.
        """
        self.config = config
        # Establecer o crear el motor para MySQL
        self.engine_mysql_bi = self.create_engine_mysql_bi()
        self.engine_mysql_conf = self.create_engine_mysql_conf()
        print(self.engine_mysql_bi)
        print(self.engine_mysql_conf)
        # Establecer o crear el motor para SQLite
        self.engine_sqlite = create_engine("sqlite:///mydata.db")
        print(self.engine_sqlite)

    def create_engine_mysql_bi(self):
        """
        Crea un motor SQLAlchemy para la conexión a la base de datos MySQL.

        Utiliza la configuración proporcionada para establecer la conexión.

        Returns:
            sqlalchemy.engine.base.Engine: Motor SQLAlchemy para la base de datos MySQL.
        """
        # Extraer los parámetros de configuración para la conexión MySQL
        user, password, host, port, database = (
            self.config.get("nmUsrIn"),
            self.config.get("txPassIn"),
            self.config.get("hostServerIn"),
            self.config.get("portServerIn"),
            self.config.get("dbBi"),
        )
        # Crear y retornar el motor de conexión
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def create_engine_mysql_conf(self):
        """
        Crea un motor SQLAlchemy para la conexión a la base de datos MySQL.

        Utiliza la configuración proporcionada para establecer la conexión.

        Returns:
            sqlalchemy.engine.base.Engine: Motor SQLAlchemy para la base de datos MySQL.
        """
        # Extraer los parámetros de configuración para la conexión MySQL
        user, password, host, port, database = (
            get_secret("DB_USERNAME"),
            get_secret("DB_PASS"),
            get_secret("DB_HOST"),
            int(get_secret("DB_PORT")),
            get_secret("DB_NAME"),
        )
        # Crear y retornar el motor de conexión
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def execute_query_mysql(self, query, chunksize=None):
        """
        Ejecuta una consulta SQL en la base de datos MySQL.

        Args:
            query (str): La consulta SQL a ejecutar.
            chunksize (int, opcional): El tamaño del fragmento para la ejecución de la consulta.

        Returns:
            DataFrame: Un DataFrame de pandas con los resultados de la consulta.
        """
        # Conectar a la base de datos y ejecutar la consulta
        with self.create_engine_mysql_bi.connect() as connection:
            cursor = connection.execution_options(isolation_level="READ COMMITTED")
            return pd.read_sql_query(query, cursor, chunksize=chunksize)

    def execute_sql_sqlite(self, sql, params=None):
        """
        Ejecuta una sentencia SQL en la base de datos SQLite.

        Args:
            sql (str): La sentencia SQL a ejecutar.
            params (dict, opcional): Parámetros para la sentencia SQL.

        Returns:
            Resultado de la ejecución de la sentencia.
        """
        # Conectar a la base de datos SQLite y ejecutar la sentencia
        with self.engine_sqlite.connect() as connection:
            return connection.execute(sql, params)

    def execute_query_mysql_chunked(self, query, table_name, chunksize=50000):
        """
        Ejecuta una consulta SQL en la base de datos MySQL y almacena los resultados en SQLite,
        procesando la consulta en fragmentos (chunks).

        Args:
            query (str): La consulta SQL a ejecutar en MySQL.
            table_name (str): El nombre de la tabla en SQLite donde se almacenarán los resultados.
            chunksize (int, opcional): El tamaño del fragmento para la ejecución de la consulta.

        Returns:
            int: El número total de registros almacenados en la tabla SQLite.
        """
        try:
            # Eliminar la tabla en SQLite si ya existe
            self.eliminar_tabla_sqlite(table_name)
            # Conectar a MySQL y ejecutar la consulta en fragmentos
            with self.engine_mysql_bi.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")
                for chunk in pd.read_sql_query(query, con=cursor, chunksize=chunksize):
                    # Almacenar cada fragmento en la tabla SQLite
                    chunk.to_sql(
                        name=table_name,
                        con=self.engine_sqlite,
                        if_exists="append",
                        index=False,
                    )

            # Contar y retornar el total de registros en la tabla SQLite
            with self.engine_sqlite.connect() as connection:
                total_records = connection.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).fetchone()[0]
            return total_records

        except Exception as e:
            # Registrar y propagar cualquier excepción que ocurra
            logging.error(f"Error al ejecutar el query: {e}")
            print(f"Error al ejecutar el query: {e}")
            raise

    def eliminar_tabla_sqlite(self, table_name):
        """
        Elimina una tabla específica en la base de datos SQLite.

        Args:
            table_name (str): El nombre de la tabla a eliminar.
        """
        # Ejecutar la sentencia para eliminar la tabla si existe
        sql = text(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine_sqlite.connect() as connection:
            connection.execute(sql)


class CargueZip:
    def __init__(self, database_name, zip_file_path):
        print("listo iniciando aqui en la clase de zip")
        """
        Inicializa la instancia de InterfaceContable.

        Args:
            database_name (str): Nombre de la base de datos.
            IdtReporteIni (str): Identificador del inicio del rango de reportes.
            IdtReporteFin (str): Identificador del fin del rango de reportes.
        """
        self.database_name = database_name
        self.configurar(database_name)
        self.zip_file_path = zip_file_path  # Establecer la ruta al archivo ZIP

    def configurar(self, database_name):
        print("listo iniciando aqui en la configuracion de zip")
        """
        Configura la conexión a las bases de datos y establece las variables de entorno necesarias.

        Esta función crea una configuración básica y establece las conexiones a las bases de datos MySQL y SQLite
        utilizando los parámetros de configuración.

        Args:
            database_name (str): Nombre de la base de datos para configurar las conexiones.

        Raises:
            Exception: Propaga cualquier excepción que ocurra durante la configuración.
        """
        try:
            # Crear un objeto de configuración básica
            config_basic = ConfigBasic(database_name)
            self.config = config_basic.config
            # Establecer conexiones a las bases de datos
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_conf = self.db_connection.engine_mysql_conf
        except Exception as e:
            # Registrar y propagar excepciones que ocurran durante la configuración
            logging.error(f"Error al inicializar Interface: {e}")
            raise

    def obtener_nombres_archivos_esperados(self):
        nombres_archivos = []
        try:
            with self.engine_mysql_conf.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")
                if not self.config["txProcedureCargue"]:
                    logging.error(
                        "Esta empresa no maneja proceso de cargue de archivos."
                    )
                    return nombres_archivos

                for nb_sql in self.config["txProcedureCargue"]:
                    result = cursor.execute(
                        text(
                            f"SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = {nb_sql}"
                        )
                    )
                    for row in result:
                        nombres_archivos.append(row["txDescripcion"])
        except Exception as e:
            logging.error(f"Error al obtener nombres de archivos: {e}")
        return nombres_archivos

    def validar_zip(self, expected_files):
        if not os.path.isfile(self.zip_file_path):
            return False, "Archivo ZIP no encontrado en la ruta proporcionada."

        try:
            with zipfile.ZipFile(self.zip_file_path, "r") as zip_ref:
                archivos_zip = zip_ref.namelist()
                missing_files = [f for f in expected_files if f not in archivos_zip]
                if missing_files:
                    return (
                        False,
                        f"Faltan archivos esperados en el ZIP: {', '.join(missing_files)}",
                    )
                return True, "Todos los archivos esperados están presentes."
        except zipfile.BadZipFile:
            return False, "El archivo ZIP está corrupto."

    def cargue(self):
        print("listo iniciando el cargue de zip a la base")
        txProcedureCargue_str = self.config["txProcedureCargue"]
        if isinstance(txProcedureCargue_str, str):
            try:
                txProcedureCargue = ast.literal_eval(txProcedureCargue_str)
            except ValueError as e:
                logging.error(f"Error al convertir txProcedureCargue a lista: {e}")
                # Manejar el error o asignar un valor predeterminado
                txProcedureCargue = []

        if not txProcedureCargue:
            return {"success": False, "error_message": "No hay datos para procesar"}
        ItpReporte = txProcedureCargue
        print(ItpReporte)
        for nb_sql in ItpReporte:
            try:
                with self.engine_mysql_conf.connect() as connection:
                    cursor = connection.execution_options(
                        isolation_level="READ COMMITTED"
                    )
                    print(nb_sql)
                    sql = text(
                        f"SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = {nb_sql}"
                    )
                    result = cursor.execute(sql)
                    df = pd.DataFrame(result)
                    self.actualizar_static_page(df)
                    logging.info(f"Se va a procesar {self.config['nmReporte']}")
                    self.procedimiento_a_sql(
                        IdtReporteIni=self.config["IdtReporteIni"],
                        IdtReporteFin=self.config["IdtReporteFin"],
                        nmReporte=self.config["nmReporte"],
                        nmProcedure_in=self.config["nmProcedure_in"],
                        nmProcedure_out=self.config["nmProcedure_out"],
                        txTabla=self.config["txTabla"],
                    )
                    logging.info(
                        f"La información se generó con éxito de {self.config['nmReporte']}"
                    )
            except Exception as e:
                logging.info(
                    f"No fue posible extraer la información de {self.config['nmReporte']} por {e}"
                )

    def actualizar_static_page(self, df):
        self.config["txTabla"] = str(df["txTabla"].values[0])
        self.config["nmReporte"] = str(df["nmReporte"].values[0])
        self.config["nmProcedure_out"] = str(df["nmProcedure_out"].values[0])
        self.config["nmProcedure_in"] = str(df["nmProcedure_in"].values[0])
        self.config["txDescripcion"] = str(df["txDescripcion"].values[0])
        self.config["txSql"] = str(df["txSql"].values[0])

    def insertar_sql(self, resultado_out, txTabla):
        try:
            with self.engine_mysql_bi.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")
                resultado_out.to_sql(
                    name=txTabla,
                    con=cursor,
                    if_exists="append",
                    index=False,
                    index_label=None,
                )
                # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
                return logging.info("los datos se han insertado correctamente")
        except Exception as e:
            logging.error(f"Error al insertar datos en {txTabla}: {e}")

    def mapeo_de_caracteres(self):
        try:
            with self.engine_mysql_bi.connect() as connection:
                sql = text(f"SELECT * FROM {self.config['dbBi']}.mapeocaracteres")
                result = connection.execute(sql)
                return {
                    row["caracter_original"]: row["caracter_reemplazo"]
                    for row in result.mappings()
                }
        except Exception as e:
            logging.error(f"Error al obtener mapeo de caracteres: {e}")
            return {}

    # def consulta_txt_out(self):
    #     """
    #     Lee y procesa un archivo de texto, intentando varias codificaciones.
    #     Realiza limpieza de texto y luego carga los datos en un DataFrame.

    #     Returns:
    #         DataFrame: Un DataFrame con los datos procesados del archivo.
    #                 Devuelve un DataFrame vacío si hay un error.
    #     """

    #     # Construye la ruta completa al archivo
    #     file_path = os.path.join(self.extract_path, self.config["txDescripcion"])

    #     # Obtiene el mapeo de caracteres especiales (debería estar definido en la clase)
    #     # mapeo_caracteres = self.mapeo_de_caracteres()

    #     # Lista de codificaciones a probar
    #     codificaciones = ["utf-8", "ISO-8859-1", "windows-1252"]

    #     # Variable para almacenar el contenido del archivo
    #     contenido = None

    #     # Intenta leer el archivo con cada codificación
    #     for codificacion in codificaciones:
    #         try:
    #             with open(file_path, "r", encoding=codificacion) as file:
    #                 contenido = file.read()
    #                 print("estamos probando la coficacion; ", codificacion)
    #             break  # Si la lectura es exitosa, sale del bucle
    #         except UnicodeDecodeError as e:
    #             # Registra un aviso si falla una codificación
    #             logging.warning(
    #                 f"Fallo al intentar con la codificación {codificacion}: {e}"
    #             )

    #     # Verifica si se pudo leer el archivo
    #     if contenido is None:
    #         # Registra un error si no se pudo leer el archivo con ninguna codificación
    #         logging.error(
    #             f"No se pudo leer el archivo {file_path} con ninguna de las codificaciones probadas."
    #         )
    #         return pd.DataFrame()

    #     try:
    #         # Procesa el contenido del archivo
    #         for caracter_especial, reemplazo in mapeo_caracteres.items():
    #             contenido = contenido.replace(caracter_especial, reemplazo)

    #         contenido = re.sub(
    #             r"\s+", " ", contenido
    #         )  # Simplifica espacios múltiples a uno solo
    #         contenido = contenido.replace('"', "")  # Elimina comillas dobles

    #         # Reescribe el archivo con la codificación correcta
    #         with open(file_path, "w", encoding=codificacion) as file:
    #             file.write(contenido)

    #         # Carga el contenido en un DataFrame
    #         if self.config["txDescripcion"] == "interinfototal":
    #             df = pd.read_csv(file_path, delimiter="{", encoding=codificacion)
    #         else:
    #             df = pd.read_csv(file_path, delimiter=";", encoding=codificacion)

    #         return df
    #     except Exception as e:
    #         # Registra un error si falla el procesamiento del archivo
    #         logging.error(f"Error al procesar archivo {file_path}: {e}")
    #         return pd.DataFrame()

    def consulta_txt_out(self):
        """
        Lee y procesa un archivo de texto, intentando varias codificaciones.
        Carga los datos en un DataFrame.

        Returns:
            DataFrame: Un DataFrame con los datos procesados del archivo.
                    Devuelve un DataFrame vacío si hay un error.
        """

        # Construye la ruta completa al archivo
        file_path = os.path.join(self.extract_path, self.config["txDescripcion"])

        # Lista de codificaciones a probar
        codificaciones = ["utf-8", "ISO-8859-1", "windows-1252"]

        # Variable para almacenar el contenido del archivo
        contenido = None

        # Intenta leer el archivo con cada codificación
        for codificacion in codificaciones:
            try:
                with open(file_path, "r", encoding=codificacion) as file:
                    contenido = file.read()
                    print("estamos probando la codificacion; ", codificacion)
                break  # Si la lectura es exitosa, sale del bucle
            except UnicodeDecodeError as e:
                # Registra un aviso si falla una codificación
                logging.warning(
                    f"Fallo al intentar con la codificación {codificacion}: {e}"
                )

        # Verifica si se pudo leer el archivo
        if contenido is None:
            # Registra un error si no se pudo leer el archivo con ninguna codificación
            logging.error(
                f"No se pudo leer el archivo {file_path} con ninguna de las codificaciones probadas."
            )
            # return pd.DataFrame()

        try:
            # Simplificar espacios y eliminar comillas dobles
            # contenido = re.sub(r"\s+", " ", contenido)  # Simplifica espacios múltiples a uno solo
            # contenido = contenido.replace('"', "")  # Elimina comillas dobles

            # Reescribe el archivo con la codificación correcta
            with open(file_path, "w", encoding=codificacion) as file:
                file.write(contenido)

            # Carga el contenido en un DataFrame
            if self.config["txDescripcion"] == "interinfototal":
                df = pd.read_csv(file_path, delimiter="{", encoding=codificacion)
            else:
                df = pd.read_csv(file_path, delimiter=";", encoding=codificacion)

            return df
        except Exception as e:
            # Registra un error si falla el procesamiento del archivo
            logging.error(f"Error al procesar archivo {file_path}: {e}")
            # return pd.DataFrame()

    def consulta_sql_bi(self, IdtReporteIni, IdtReporteFin):
        with self.engine_mysql_bi.connect() as connection:
            cursor = connection.execution_options(isolation_level="READ COMMITTED")
            sqldelete = text(self.config["txSql"])
            cursor.execute(sqldelete, {"fi": IdtReporteIni, "ff": IdtReporteFin})
        return logging.info("Datos fueron borrados")

    def procedimiento_a_sql(
        self,
        IdtReporteIni,
        IdtReporteFin,
        nmReporte,
        nmProcedure_in,
        nmProcedure_out,
        txTabla,
    ):
        # Iniciamos las conexiones
        EsVacio = True
        if (
            nmReporte == "update_cubo_bi"
            or nmReporte == "borra_impactos_bi"
            or nmReporte == "impactos_bi"
        ):
            try:
                self.consulta_sql_bi(IdtReporteIni, IdtReporteFin)
                logging.info(f"Se ha realizado el proceso de {txTabla}")
                logging.info(
                    f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
                )
                EsVacio = True
            except Exception as e:
                logging.error(e)
        else:
            self.config["resultado_out"] = self.consulta_txt_out()
            logging.info(
                f"{nmReporte} contiene {len(self.config['resultado_out.index'])}"
            )
            EsVacio = False
            # self.consulta_sql_bi(nmProcedure_in=nmProcedure_in,IdtReporteIni=IdtReporteIni,IdtReporteFin=IdtReporteFin,nmReporte=nmReporte)
            logging.info(f"el procemiento {txTabla} si funciono")

        # Evaluamos si esta vacio el dataFrame
        if EsVacio == False and self.config["resultado_out"].empty == False:
            logging.info(f"Si hay datos para {txTabla} ")
            self.consulta_sql_bi(
                IdtReporteIni=IdtReporteIni, IdtReporteFin=IdtReporteFin
            )
            logging.info(f"Se ha realizado el proceso de {txTabla}")
            # resultado_out.to_csv(nmReporte.lower()+'.txt',sep='|',index=False,header=False,float_format='%.0f')
            self.insertar_sql(
                txTabla=txTabla, resultado_out=self.config["resultado_out"]
            )
            logging.info(
                f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
            )
        else:
            logging.info(f"No hay datos para {txTabla} ")

        logging.info("Se ejecutó el procedimiento")

    def obtener_identificador_empresa(self):
        try:
            # Dividir el nombre del archivo por '/'
            nombre_archivo = self.zip_file_path.split("/")[-1]
            # Dividir el nombre del archivo por '_' y obtener la segunda parte (índice 1)
            nit = nombre_archivo.split("_")[2]
            return nit
        except IndexError:
            logging.error(
                f"Formato de nombre de archivo incorrecto: {self.zip_file_path}"
            )
            return "default"

    def procesar_zip(self):
        print("listo iniciando aqui en la clase de zip")
        print(self.zip_file_path)
        if not self.zip_file_path or not os.path.isfile(self.zip_file_path):
            logging.error(
                "La ruta del archivo ZIP no es válida o el archivo no existe."
            )
            return False

        expected_files = self.obtener_nombres_archivos_esperados()
        valid, missing_files = self.validar_zip(expected_files)
        if not valid:
            logging.error(
                f"Faltan archivos esperados en el ZIP: {', '.join(missing_files)}"
            )
            return False

        try:
            # Crear la ruta de la carpeta con el nombre de la base de datos
            folder_path = os.path.join("media", self.database_name)
            os.makedirs(folder_path, exist_ok=True)
            self.extract_path = folder_path

            nit = self.obtener_identificador_empresa()
            print("nit", nit)
            if nit == self.config["id_tsol"]:
                os.makedirs(self.extract_path, exist_ok=True)
                with zipfile.ZipFile(self.zip_file_path, "r") as zip_ref:
                    zip_ref.extractall(self.extract_path)
                self.cargue()

                logging.info(
                    f"Archivo ZIP extraído y cargado exitosamente en {self.extract_path}."
                )
                return {
                    "success": True,
                    "message": "Archivo ZIP extraído y cargado exitosamente.",
                }

            else:
                error_message = f"El NIT de la empresa no coincide con el nombre del archivo ZIP: {nit}"
                logging.error(error_message)
                return {
                    "success": False,
                    "error_message": f"Error al procesar el cargue: {error_message}",
                }
        except Exception as e:
            logging.error(f"Error al extraer el archivo ZIP: {e}")
            return {
                "success": False,
                "error_message": f"Error al procesar el cargue: {e}",
            }


# Ejemplo de uso
# cargador = CargueZip(
#     "distrijass",
#     "D:\\Descargas\\Distrijass\\DISTRIJASS_210653_901164665_202361103 (1).zip",
# )
# if cargador.procesar_zip():
#     # Continuar con el procesamiento después de la extracción
# else:
#     # Manejar el error
