import zipfile
import os
import pandas as pd
import logging

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from django.contrib import sessions
import re
import ast
from django.core.exceptions import ImproperlyConfigured
import json
import unicodedata

# Configura el logging de SQLAlchemy
logging.basicConfig(
    filename="log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
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
        import uuid

        sqlite_table_name = getattr(
            self, "sqlite_table_name", f"carguezip_{uuid.uuid4().hex[:8]}"
        )
        sqlite_path = os.path.join("media", f"temp_{sqlite_table_name}.db")
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        self.engine_sqlite = create_engine(f"sqlite:///{sqlite_path}")
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
                txProcedureCargue_str = self.config["txProcedureCargue"]
                if isinstance(txProcedureCargue_str, str):
                    try:
                        txProcedureCargue = ast.literal_eval(txProcedureCargue_str)
                        # Asegúrate de que txProcedureCargue es una lista después de evaluar
                        if not isinstance(txProcedureCargue, list):
                            raise ValueError(
                                "txProcedureCargue no se convirtió en una lista."
                            )
                    except ValueError as e:
                        logging.error(
                            f"Error al convertir txProcedureCargue a lista: {e}"
                        )
                        txProcedureCargue = []

                if not txProcedureCargue:
                    logging.error("No hay datos para procesar")
                    return {
                        "success": False,
                        "error_message": "No hay datos para procesar",
                    }

                for nb_sql in txProcedureCargue:
                    try:
                        # Asegúrate de que nb_sql es un valor escalar válido
                        if not isinstance(nb_sql, (int, str)) or not nb_sql:
                            logging.error(
                                "El valor de nb_sql está vacío o es inválido."
                            )
                            continue

                        sql = text(
                            "SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = :nb_sql"
                        )
                        result = pd.read_sql_query(
                            sql, con=cursor, params={"nb_sql": nb_sql}
                        )
                        for row in result.itertuples():
                            nombres_archivos.append(row.txDescripcion)
                    except Exception as e:
                        logging.error(
                            f"Error al ejecutar la consulta SQL para nb_sql={nb_sql}: {e}"
                        )
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
                    if isinstance(nb_sql, list):
                        # Si nb_sql es una lista, toma el primer elemento o maneja el caso de lista vacía
                        nb_sql = nb_sql[0] if nb_sql else None

                    if not nb_sql:
                        logging.error("El valor de nb_sql está vacío o es inválido.")
                        continue
                    print(nb_sql)
                    # Si nb_sql no está vacío, ejecuta la consulta
                    try:
                        sql = text(
                            "SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = :nb_sql"
                        )
                        print(sql)
                        result = pd.read_sql_query(
                            sql, con=cursor, params={"nb_sql": nb_sql}
                        )
                        self.actualizar_static_page(result)
                        logging.info(f"Se va a procesar {self.config['nmReporte']}")
                        self.procedimiento_a_sql(
                            IdtReporteIni=self.config["IdtReporteIni"],
                            IdtReporteFin=self.config["IdtReporteFin"],
                            nmReporte=self.config["nmReporte"],
                            nmProcedure_in=self.config["nmProcedure_in"],
                            nmProcedure_out=self.config["nmProcedure_out"],
                            txTabla=self.config["txTabla"],
                        )

                    except Exception as e:
                        logging.error(f"Error al ejecutar la consulta SQL: {e}")

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

    def obtener_claves_primarias(self, txTabla):
        claves_primarias = []
        try:
            inspector = inspect(self.engine_mysql_bi)

            # Obtener la información de la clave primaria para la tabla
            pk_constraint = inspector.get_pk_constraint(txTabla)
            claves_primarias = pk_constraint["constrained_columns"]

            # Si no hay claves primarias definidas, registra un mensaje informativo en lugar de un error
            if not claves_primarias:
                logging.info(f"La tabla {txTabla} no tiene claves primarias definidas.")

        except Exception as e:
            # Cambia el mensaje de error para reflejar que puede ser un problema de acceso a la base de datos o que la tabla no exista
            logging.error(
                f"No se pudo verificar las claves primarias de {txTabla}: {e}"
            )

        return claves_primarias

    def obtener_nombres_columnas(self, txTabla):
        info_columnas = {}
        try:
            with self.engine_mysql_bi.connect() as connection:
                inspector = inspect(self.engine_mysql_bi)
                columnas = inspector.get_columns(txTabla)
                for columna in columnas:
                    nombre = columna["name"]
                    tipo = str(columna["type"])
                    # Ajustar la lógica para mapear tipos de SQL a tipos de Python/Pandas aquí
                    pandas_tipo = "str"  # Por defecto, trata todo como cadena
                    if "int" in tipo.lower():
                        pandas_tipo = "int"
                    elif (
                        "float" in tipo.lower()
                        or "decimal" in tipo.lower()
                        or "double" in tipo.lower()
                    ):
                        pandas_tipo = "float"
                    info_columnas[nombre] = pandas_tipo
        except Exception as e:
            logging.error(f"Error al obtener información de columnas de {txTabla}: {e}")
        print(info_columnas)
        return info_columnas

    def obtener_nombres_columnas_texto(self, txTabla):
        """
        Obtiene los nombres de las columnas de texto de una tabla dada.

        Args:
            txTabla (str): Nombre de la tabla de la cual obtener los nombres de columnas de texto.

        Returns:
            dict: Un diccionario con los nombres de las columnas de texto y su tipo de dato como 'str'.
        """
        columnas_texto = {}
        try:
            with self.engine_mysql_bi.connect() as connection:
                inspector = inspect(self.engine_mysql_bi)
                columnas = inspector.get_columns(txTabla)
                for columna in columnas:
                    nombre = columna["name"]
                    tipo = str(columna["type"]).lower()
                    if any(t in tipo for t in ["char", "text", "varchar"]):
                        columnas_texto[nombre] = "str"
        except Exception as e:
            logging.error(
                f"Error al obtener nombres de columnas de texto de {txTabla}: {e}"
            )
        return columnas_texto

    def limpiar_datos_intercliente(self, df):
        """
        Limpia y prepara los datos específicos de la tabla 'tmp_intercliente'.

        Args:
            df (DataFrame): DataFrame original con los datos de 'tmp_intercliente'.

        Returns:
            DataFrame: DataFrame limpio con los datos preparados para 'tmp_intercliente'.
        """
        # Establece un valor por defecto para fechas vacías.
        df["Fecha Ingreso"] = df["Fecha Ingreso"].apply(
            lambda x: x if x else "2020-01-01"
        )

        # Define la columna 'Cod. Cliente' para limpieza y preparación.
        col = "Cod. Cliente"

        # Convierte a texto y elimina espacios al principio y final.
        df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) else x)

        # Elimina saltos de línea, retornos de carro y comillas.
        df[col] = df[col].replace({"\\n": "", "\\r": "", '"': "", "'": ""}, regex=True)

        # Reduce espacios múltiples a uno solo y convierte todo a mayúsculas para consistencia.
        df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.upper()

        # Establece longitudes máximas permitidas según la configuración de la base de datos.
        MAX_LENGTH = 30  # Para 'Cod. Cliente', 'Barrio' y 'Telefono'.
        MAX_LENGTH_DIR = 150  # Para 'Direccion' y 'Nom. Cliente'.

        # Recorta los valores a la longitud máxima y maneja los valores 'NAN'.
        for field, max_len in [
            ("Cod. Cliente", MAX_LENGTH),
            ("Barrio", MAX_LENGTH),
            ("Telefono", MAX_LENGTH),
            ("Direccion", MAX_LENGTH_DIR),
            ("Nom. Cliente", MAX_LENGTH_DIR),
        ]:
            df[field] = df[field].apply(
                lambda x: str(x)[:max_len] if x != "NAN" else ""
            )

        # Filtra las filas donde 'Cod. Cliente' no es nulo, vacío o igual a 'NAN'.
        df_filtrado = df[(df[col] != "NAN") & (df[col].str.strip() != "")]

        # Elimina duplicados basados en las claves primarias de la tabla.
        df_limpio = self.eliminar_duplicados_df(df_filtrado, self.config["txTabla"])

        return df_limpio

    def limpiar_espacios_y_caracteres(self, df, columnas_de_texto):
        """
        Limpia los espacios y caracteres especiales de las columnas especificadas en un DataFrame.

        Args:
            df (DataFrame): El DataFrame original que se limpiará.
            columnas_de_texto (dict): Un diccionario de las columnas y sus tipos de datos esperados.

        Returns:
            DataFrame: El DataFrame con las columnas limpias.
        """
        for col, tipo in columnas_de_texto.items():
            if col in df:  # Verifica que la columna exista en el DataFrame.
                # Limpieza y normalización de texto en la columna.
                df[col] = (
                    df[col]
                    .astype(str)  # Convierte a cadena para manipulación de texto.
                    .replace(
                        {"\\n": "", "\\r": ""}, regex=True
                    )  # Elimina saltos de línea y retornos de carro.
                    .str.replace(
                        r"\s+", " ", regex=True
                    )  # Reduce espacios múltiples a uno solo.
                    .str.strip()  # Elimina espacios al principio y al final.
                    .str.upper()  # Convierte a mayúsculas para consistencia.
                )

                # Elimina comillas y aplica función para remover acentos.
                for quote in ['"', "'"]:
                    df[col] = df[col].str.replace(quote, "")
                df[col] = df[col].apply(lambda x: self.remove_accents(x) if x else x)

                # Elimina duplicados en el DataFrame basado en la configuración de la tabla actual.
                df = self.eliminar_duplicados_df(df, self.config["txTabla"])

        return df

    def remove_accents(self, input_str):
        # Define los caracteres que deseas preservar
        preserved_chars = "Ññ@#"

        # Normaliza y descompone el texto en sus componentes básicos
        nfkd_form = unicodedata.normalize("NFKD", input_str)

        # Reconstruye la cadena preservando solo los caracteres ASCII, los caracteres preservados y convirtiendo los demás a su forma base.
        only_preserved = "".join(
            [
                c if c in preserved_chars else unicodedata.normalize("NFC", c)
                for c in nfkd_form
                if not unicodedata.combining(c)
            ]
        )

        return only_preserved

    def limpiar_caracteres_en_db(self, txTabla, mapeo_caracteres):
        try:
            # Obtener los nombres de todas las columnas de la tabla
            info_columnas = self.obtener_nombres_columnas_texto(txTabla)

            with self.engine_mysql_bi.connect() as connection:
                for columna, tipo in info_columnas.items():
                    columna_escaped = f"`{columna}`"
                    # Aquí podrías agregar lógica para verificar si la columna es de texto
                    for original, reemplazo in mapeo_caracteres.items():
                        sql = text(
                            f"UPDATE {txTabla} SET {columna_escaped} = REPLACE({columna_escaped}, :original, :reemplazo) WHERE {columna_escaped} LIKE :original"
                        )
                        connection.execute(
                            sql, {"original": original, "reemplazo": reemplazo}
                        )
        except Exception as e:
            logging.error(f"Error al limpiar caracteres en {txTabla}: {e}")

    def agregar_numero_linea(self, df):
        # Claves principales para ordenar y agrupar
        claves_principales = ["Fac. numero", "Tipo"]

        # Otras columnas a considerar en el ordenamiento
        otras_columnas = ["Cod. productto", "Cantidad"]

        # Combina las claves principales con las otras columnas para el ordenamiento
        columnas_ordenamiento = claves_principales + otras_columnas

        # Asegúrate de que todas las columnas especificadas existan en el DataFrame
        columnas_existentes = [
            col for col in columnas_ordenamiento if col in df.columns
        ]
        if len(columnas_existentes) != len(columnas_ordenamiento):
            missing_cols = set(columnas_ordenamiento) - set(columnas_existentes)
            raise ValueError(
                f"Las siguientes columnas faltan en el DataFrame: {missing_cols}"
            )

        # Ordena el DataFrame por las columnas especificadas
        df = df.sort_values(by=columnas_existentes)

        # Agrega la columna 'nbLinea' que enumera secuencialmente las filas dentro de cada grupo
        df["nbLinea"] = (
            df.groupby(claves_principales).cumcount() + 1
        )  # +1 para que comience en 1

        return df

    def eliminar_duplicados_df(self, df, txTabla):
        # Obtener las claves primarias de la tabla
        claves_primarias = self.obtener_claves_primarias(txTabla)
        print(txTabla, "tabla en :", self.config["txDescripcion"])

        # Si la tabla es 'tmp_infoventas', agregar un número de línea para manejar duplicados
        if txTabla == "tmp_infoventas":
            df = self.agregar_numero_linea(df)

        # Si se obtuvieron claves primarias, y no es 'tmp_infoventas', eliminar duplicados en el DataFrame
        # basado en las claves primarias. Si es 'tmp_infoventas', se supone que ya es única con 'nbLinea'.
        if claves_primarias:
            df = df.drop_duplicates(subset=claves_primarias)
            print("se eliminó duplicados para la tabla ", txTabla)
        elif not claves_primarias and txTabla != "tmp_infoventas":
            # Registra un mensaje informativo si no hay claves primarias y no es 'tmp_infoventas'
            logging.info(
                f"La tabla {txTabla} no tiene claves primarias definidas. No se eliminarán duplicados."
            )

        return df

    def insertar_sql(self, resultado_out, txTabla):
        try:
            txTabla = f"{txTabla}"
            resultado = self.eliminar_duplicados_df(resultado_out, txTabla)
            with self.engine_mysql_bi.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")
                resultado.to_sql(
                    name=txTabla,
                    con=cursor,
                    if_exists="append",
                    index=False,
                    index_label=None,
                )
                # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
                # Proceso de limpieza
                self.proceso_de_limpieza(txTabla)
                return logging.info("los datos se han insertado correctamente")
        except IntegrityError as e:
            logging.error(f"Error de integridad al insertar datos en {txTabla}: {e}")
        except OperationalError as e:
            logging.error(
                f"Error operacional en la base de datos al insertar datos en {txTabla}: {e}"
            )
        except SQLAlchemyError as e:
            logging.error(
                f"Error general de SQLAlchemy al insertar datos en {txTabla}: {e}"
            )
        except Exception as e:
            logging.error(f"Error inesperado al insertar datos en {txTabla}: {e}")

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

    def proceso_de_limpieza(self, txTabla):
        # Obtener el mapeo de caracteres
        mapeo_caracteres = self.mapeo_de_caracteres()

        if not mapeo_caracteres:
            logging.error(
                "No se pudo obtener el mapeo de caracteres. La limpieza no se realizará."
            )
            return

        # Continuar con la limpieza en la base de datos
        self.limpiar_caracteres_en_db(txTabla, mapeo_caracteres)

    # def consulta_txt_out(self):
    #     """
    #     Lee y procesa un archivo de texto, intentando varias codificaciones.
    #     Carga los datos en un DataFrame.

    #     Returns:
    #         DataFrame: Un DataFrame con los datos procesados del archivo.
    #                 Devuelve un DataFrame vacío si hay un error.
    #     """

    #     # Construye la ruta completa al archivo
    #     file_path = os.path.join(self.extract_path, self.config["txDescripcion"])

    #     # Verificar si el archivo existe
    #     if not os.path.exists(file_path):
    #         logging.error(f"Archivo no encontrado: {file_path}")
    #         return pd.DataFrame()

    #     # Lista de codificaciones a probar
    #     codificaciones = ["utf-8", "ISO-8859-1", "windows-1252"]

    #     # Variable para almacenar el contenido del archivo
    #     contenido = None

    #     # Intenta leer el archivo con cada codificación
    #     for codificacion in codificaciones:
    #         try:
    #             with open(file_path, "r", encoding=codificacion) as file:
    #                 contenido = file.read()
    #                 print("estamos probando la codificacion; ", codificacion)
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
    #         # Reescribe el archivo con la codificación correcta
    #         with open(file_path, "w", encoding=codificacion) as file:
    #             file.write(contenido)

    #         # Obtener información de tipos de datos de columnas para la tabla
    #         tipos_columnas = self.obtener_nombres_columnas_texto(self.config["txTabla"])

    #         # Carga el contenido en un DataFrame con los tipos especificados
    #         if self.config["txDescripcion"] == "interinfototal.txt":
    #             df = pd.read_csv(
    #                 file_path,
    #                 delimiter="{",
    #                 encoding="utf-8",
    #                 dtype=tipos_columnas,
    #             )
    #         else:
    #             df = pd.read_csv(
    #                 file_path,
    #                 delimiter=";",
    #                 encoding=codificacion,
    #                 dtype=tipos_columnas,
    #             )

    #         # Reemplazar valores NaN con una cadena vacía en todo el DataFrame
    #         df = df.fillna("")  # Esto aplicará el reemplazo en todas las columnas
    #         # Ajusta el formato de los números flotantes aquí
    #         for col in df.select_dtypes(include="number").columns:
    #             df[col] = df[col].round(2)  # Redondea a dos decimales
    #         # Aplicar la función de limpieza específicamente para la tabla 'tmp_intercliente'
    #         if self.config["txTabla"] == "tmp_intercliente":
    #             df = self.limpiar_datos_intercliente(df)

    #         df_limpio = self.limpiar_espacios_y_caracteres(df, tipos_columnas)

    #         return df_limpio
    #     except Exception as e:
    #         # Registra un error si falla el procesamiento del archivo
    #         logging.error(f"Error al procesar archivo {file_path}: {e}")
    #         return pd.DataFrame()

    def consulta_txt_out(self):
        """
        Lee y procesa un archivo de texto utilizando diferentes codificaciones.
        Limpia y formatea los datos para prepararlos para su inserción en la base de datos.

        Returns:
            DataFrame: Un DataFrame con los datos procesados del archivo, o un DataFrame vacío en caso de error.
        """
        file_path = os.path.join(self.extract_path, self.config["txDescripcion"])

        # Verifica si el archivo existe en la ruta especificada.
        if not os.path.exists(file_path):
            logging.error(f"Archivo no encontrado: {file_path}")
            return pd.DataFrame()

        # Define las codificaciones que se intentarán para leer el archivo.
        codificaciones = ["utf-8", "ISO-8859-1", "windows-1252"]
        contenido = None

        # Intenta leer el archivo con cada codificación disponible hasta que una funcione.
        for codificacion in codificaciones:
            try:
                with open(file_path, "r", encoding=codificacion) as file:
                    contenido = file.read()
                    break  # Sal del bucle si la lectura es exitosa.
            except UnicodeDecodeError as e:
                logging.warning(
                    f"Fallo al intentar con la codificación {codificacion}: {e}"
                )

        # Si no se pudo leer el archivo con ninguna codificación, retorna un DataFrame vacío.
        if contenido is None:
            logging.error(
                f"No se pudo leer el archivo {file_path} con ninguna de las codificaciones probadas."
            )
            return pd.DataFrame()

        try:
            # Reescribe el archivo con la codificación detectada para corregir problemas de codificación.
            with open(file_path, "w", encoding=codificacion) as file:
                file.write(contenido)

            # Determina el delimitador basado en la descripción del archivo y lee el contenido en un DataFrame.
            delimiter = (
                "{" if self.config["txDescripcion"] == "interinfototal.txt" else ";"
            )
            tipos_columnas = self.obtener_nombres_columnas_texto(self.config["txTabla"])
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=codificacion,
                dtype=tipos_columnas,
            )

            # Limpia y transforma los datos antes de devolverlos.
            return self.limpiar_y_transformar_datos(df)
        except Exception as e:
            logging.error(f"Error al procesar archivo {file_path}: {e}")
            return pd.DataFrame()

    def limpiar_y_transformar_datos(self, df):
        """
        Aplica limpiezas generales y transformaciones específicas del negocio al DataFrame dado.

        Args:
            df (DataFrame): DataFrame original que se limpiará y transformará.

        Returns:
            DataFrame: DataFrame limpio y transformado.
        """
        # Reemplaza valores NaN con una cadena vacía en todo el DataFrame.
        df = df.fillna("")

        # Redondea los números flotantes a dos decimales.
        for col in df.select_dtypes(include="number").columns:
            df[col] = df[col].round(2)

        # Limpieza específica para la tabla 'tmp_intercliente'.
        if self.config["txTabla"] == "tmp_intercliente":
            df = self.limpiar_datos_intercliente(df)

        # Limpieza general de espacios y caracteres en todas las columnas de texto.
        tipos_columnas = self.obtener_nombres_columnas_texto(self.config["txTabla"])
        df_limpio = self.limpiar_espacios_y_caracteres(df, tipos_columnas)

        return df_limpio

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
                f"{nmReporte} contiene {len(self.config['resultado_out'].index)}"
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
