import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import ast
import json
from django.core.exceptions import ImproperlyConfigured
import ast
import sqlalchemy
import time

# Configuración del logging
logging.basicConfig(
    filename="logExtractor.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)


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
        print(self.engine_sqlite)

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


class Extrae_Bi:
    def __init__(self, database_name, IdtReporteIni, IdtReporteFin):
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.configurar(database_name)

    def configurar(self, database_name):
        try:
            config_basic = ConfigBasic(database_name)
            self.config = config_basic.config
            config_basic.print_configuration()
            print(self.config.get("txProcedureExtrae", []))
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_out = self.db_connection.engine_mysql_out
            print("Configuraciones preliminares de actualización terminadas")
        except Exception as e:
            logging.error(f"Error al inicializar Actualización: {e}")
            raise

    def extractor(self):
        print("Iniciando extractor")
        try:
            txProcedureExtrae = self.config.get("txProcedureExtrae", [])
            print("txProcedureExtrae:", txProcedureExtrae)
            if isinstance(txProcedureExtrae, str):
                txProcedureExtrae = ast.literal_eval(txProcedureExtrae)
            print(
                "Tipo de txProcedureExtrae:", type(txProcedureExtrae)
            )  # Esto debería mostrarte <class 'list'>
            for a in txProcedureExtrae:
                print("Procesando:", a)
                with self.engine_mysql_bi.connect() as connectionin:
                    sql = text("SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = :a")
                    result = connectionin.execute(sql, {"a": a})
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())

                    if not df.empty:
                        self.config["txTabla"] = str(df["txTabla"].values[0])
                        self.config["nmReporte"] = str(df["nmReporte"].values[0])
                        self.config["nmProcedure_out"] = str(
                            df["nmProcedure_out"].values[0]
                        )
                        self.config["nmProcedure_in"] = str(
                            df["nmProcedure_in"].values[0]
                        )
                        self.config["txSql"] = str(df["txSql"].values[0])

                        logging.info(f"Se va a procesar {self.config['nmReporte']}")

                        try:
                            self.procedimiento_a_sql(
                                IdtReporteIni=self.IdtReporteIni,
                                IdtReporteFin=self.IdtReporteFin,
                                nmReporte=self.config.get("nmReporte"),
                                nmProcedure_out=self.config.get("nmProcedure_out"),
                                txTabla=self.config.get("txTabla"),
                            )
                            logging.info(
                                f"La información se generó con éxito de {self.config.get('nmReporte')}"
                            )
                        except Exception as e:
                            logging.info(
                                f"No fue posible extraer la información de {self.config.get('nmReporte')} por {e}"
                            )
                            print(
                                f"Error al ejecutar procedimiento_a_sql para {self.config.get('nmReporte')}: {e}"
                            )
                    else:
                        logging.warning(
                            f"No se encontraron resultados para nbSql = {a}"
                        )
                        print(f"No se encontraron resultados para nbSql = {a}")
            print("Extracción completada con éxito")
            return {"success": True}
        except Exception as e:
            print(f"Error general en el extractor: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logging.error(
                f"Error durante la ejecución de la lista de procedimientos con nbSql = {a}: {e}"
            )
            return {"success": False, "error": str(e)}
        finally:
            logging.info("Finalizado el procedimiento de ejecución SQL.")

    def insertar_sql(self, df):
        """Inserta los datos en la base de datos de BI evitando duplicados"""
        try:
            with self.engine_mysql_bi.connect() as connection:
                cursorbi = connection.execution_options(
                    isolation_level="READ COMMITTED"
                )
                registros_a_insertar = len(df)
                logging.info(
                    f"Intentando insertar {registros_a_insertar} registros en {self.txTabla}"
                )

                # Verificar si la tabla tiene clave primaria o campos únicos para prevenir duplicados
                primary_keys = self.obtener_claves_primarias(self.txTabla)

                if primary_keys:
                    # Si hay claves primarias, podemos usar INSERT IGNORE o REPLACE INTO
                    # Convertir DataFrame a lista de diccionarios para inserción manual
                    records = df.to_dict("records")

                    # Usar INSERT IGNORE para evitar duplicados si existe primary key
                    self.insertar_con_ignore(records, self.txTabla, cursorbi)

                    logging.info(
                        f"Se insertaron registros en {self.txTabla} usando INSERT IGNORE"
                    )
                else:
                    # Si no hay claves primarias, intentar eliminar duplicados en memoria
                    logging.warning(
                        f"No se encontraron claves primarias en {self.txTabla}. Eliminando duplicados en DataFrame."
                    )
                    # Intentar eliminar duplicados si no hay clave primaria (menos eficiente)
                    if len(df) > 0:
                        df_sin_duplicados = df.drop_duplicates()
                        if len(df_sin_duplicados) < len(df):
                            logging.info(
                                f"Se eliminaron {len(df) - len(df_sin_duplicados)} duplicados antes de insertar"
                            )

                        df_sin_duplicados.to_sql(
                            name=self.txTabla,
                            con=cursorbi,
                            if_exists="append",
                            index=False,
                        )
                    else:
                        logging.warning(
                            f"DataFrame vacío, no se insertará en {self.txTabla}"
                        )

                logging.info(f"Proceso de inserción completado para {self.txTabla}")

        except Exception as e:
            logging.error(f"Error al insertar datos en {self.txTabla}: {e}")
            raise

    def obtener_claves_primarias(self, tabla):
        """Obtiene las claves primarias de una tabla"""
        query = text(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{self.config.get("dbBi")}' 
            AND TABLE_NAME = '{tabla}'
            AND CONSTRAINT_NAME = 'PRIMARY';
        """
        )

        try:
            with self.engine_mysql_bi.connect() as connection:
                resultado = connection.execute(query)
                return [row[0] for row in resultado.fetchall()]
        except Exception as e:
            logging.error(f"Error obteniendo claves primarias de {tabla}: {e}")
            return []

    def insertar_con_ignore(self, records, tabla, connection):
        """Inserta registros usando INSERT IGNORE para evitar duplicados"""
        if not records:
            logging.warning(f"No hay registros para insertar en {tabla}")
            return

        try:
            # Obtener los nombres de columnas del primer registro
            columns = list(records[0].keys())

            # Construir la consulta INSERT IGNORE
            placeholders = ", ".join([":" + col for col in columns])
            columns_str = ", ".join(columns)

            insert_query = text(
                f"INSERT IGNORE INTO {tabla} ({columns_str}) VALUES ({placeholders})"
            )

            # Ejecutar la inserción por lotes para mejorar rendimiento
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                connection.execute(insert_query, batch)
                logging.info(
                    f"Insertado lote {i//batch_size + 1} de {(len(records)-1)//batch_size + 1} en {tabla}"
                )

        except Exception as e:
            logging.error(f"Error en insertar_con_ignore para {tabla}: {e}")
            raise

    def consulta_sql_out(
        self, nmProcedure_out, IdtReporteIni, IdtReporteFin, nmReporte
    ):
        max_retries = 3  # Define aquí el número máximo de reintentos
        retry_count = 0

        while retry_count < max_retries:
            try:
                with self.engine_mysql_out.connect() as connection:
                    cursorout = connection.execution_options(
                        isolation_level="READ COMMITTED"
                    )
                    # Preparar el llamado al procedimiento almacenado de manera segura
                    sqlout = text(
                        f"CALL {nmProcedure_out}(:IdtReporteIni, :IdtReporteFin, :nmReporte)"
                    )
                    # Ejecutar el procedimiento almacenado pasando los parámetros de forma segura
                    resultado = pd.read_sql_query(
                        sql=sqlout,
                        con=cursorout,
                        params={
                            "IdtReporteIni": IdtReporteIni,
                            "IdtReporteFin": IdtReporteFin,
                            "nmReporte": nmReporte,
                        },
                    )
                    return resultado

            except sqlalchemy.exc.IntegrityError as e:
                logging.error(f"Error de integridad: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

            except sqlalchemy.exc.ProgrammingError as e:
                logging.error(f"Error de programación: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

            except Exception as e:
                logging.error(f"Error desconocido: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

        # Si se alcanza este punto, se agotaron los reintentos sin éxito
        return None

    def consulta_sql_bi(self, IdtReporteIni, IdtReporteFin):
        """
        Ejecuta una consulta SQL en la base de datos BI para borrar datos
        entre dos fechas especificadas.

        Este método asume que `self.config["txSql"]` contiene una consulta SQL
        preparada para ejecutar una operación de borrado, donde `:fi` y `:ff`
        son marcadores de posición para las fechas de inicio y fin, respectivamente.

        Parámetros:
        - IdtReporteIni: Fecha de inicio para la condición del borrado.
        - IdtReporteFin: Fecha de fin para la condición del borrado.

        La función no devuelve ningún valor, pero registra un mensaje de éxito
        una vez que los datos han sido borrados.
        """
        try:
            # Establecer conexión con la base de datos BI
            with self.engine_mysql_bi.connect() as connection:
                # Preparar la consulta SQL con parámetros de seguridad
                sqldelete = text(self.config["txSql"])
                # Ejecutar la consulta con los parámetros proporcionados
                connection.execute(
                    sqldelete, {"fi": IdtReporteIni, "ff": IdtReporteFin}
                )
                # Registrar el éxito de la operación
                logging.info(
                    "Datos borrados correctamente entre las fechas proporcionadas."
                )
        except Exception as e:
            # Registrar cualquier excepción ocurrida durante la ejecución
            logging.error(f"Error al borrar datos: {e}")
            # Podría ser adecuado manejar la excepción de forma más específica o re-lanzarla
            raise

    def procedimiento_a_sql(
        self, IdtReporteIni, IdtReporteFin, nmReporte, nmProcedure_out, txTabla
    ):
        es_vacio = (
            True  # Python utiliza snake_case para nombres de variables por convención
        )

        reportes_especificos = {
            "update_cubo_bi",
            "borra_impactos_bi",
            "impactos_bi",
            "actualizar_usuarios_periodo",
            "actualizar_usuarios_conteo_diario",
        }

        for intento in range(3):  # Intentar la conexión hasta tres veces
            try:
                if nmReporte in reportes_especificos:
                    self.consulta_sql_bi(IdtReporteIni, IdtReporteFin)
                    es_vacio = True
                else:
                    resultado_out = self.consulta_sql_out(
                        nmProcedure_out=nmProcedure_out,
                        IdtReporteIni=IdtReporteIni,
                        IdtReporteFin=IdtReporteFin,
                        nmReporte=nmReporte,
                    )
                    es_vacio = resultado_out.empty

                if not es_vacio:
                    self.consulta_sql_bi(IdtReporteIni, IdtReporteFin)
                    self.insertar_sql(txTabla=txTabla, resultado_out=resultado_out)

                logging.info(f"Proceso completado para {txTabla}.")
                return

            except Exception as e:
                logging.error(
                    f"Error en procedimiento_a_sql (Intento {intento + 1}/3): {e}"
                )
                if intento >= 2:  # Si se agotaron los intentos
                    logging.error(
                        "Se agotaron los intentos. No se pudo ejecutar el procedimiento."
                    )
                    break
                else:
                    logging.info(
                        f"Reintentando procedimiento (Intento {intento + 1}/3)..."
                    )
                    time.sleep(5)  # Esperar 5 segundos antes de reintentar

    # def procedimiento_a_sql(
    #     self,
    #     IdtReporteIni,
    #     IdtReporteFin,
    #     nmReporte,
    #     nmProcedure_out,
    #     txTabla,
    # ):
    #     """
    #     Ejecuta un conjunto de operaciones SQL basadas en el tipo de reporte especificado.
    #     Dependiendo del reporte, puede ejecutar consultas para actualizar datos, borrar impactos,
    #     o insertar resultados de procedimientos almacenados en una tabla específica.

    #     Parámetros:
    #     - IdtReporteIni (str): Fecha de inicio para filtrar los datos.
    #     - IdtReporteFin (str): Fecha de fin para filtrar los datos.
    #     - nmReporte (str): Nombre del reporte, que determina la operación a realizar.
    #     - nmProcedure_in (str): Nombre del procedimiento almacenado para operaciones internas (no utilizado actualmente).
    #     - nmProcedure_out (str): Nombre del procedimiento almacenado para recuperar datos.
    #     - txTabla (str): Nombre de la tabla donde se insertarán los datos.

    #     No devuelve un valor explícito pero registra el progreso y los resultados de las operaciones.
    #     """
    #     try:
    #         if nmReporte in ["update_cubo_bi", "borra_impactos_bi", "impactos_bi"]:
    #             # Ejecuta consultas de borrado o actualización específicas para algunos reportes
    #             self.consulta_sql_bi(IdtReporteIni, IdtReporteFin)
    #             logging.info(
    #                 f"Proceso realizado para {txTabla}. Datos insertados entre {IdtReporteIni} y {IdtReporteFin}."
    #             )
    #         else:
    #             # Para otros tipos de reporte, ejecuta un procedimiento almacenado y procesa los resultados
    #             resultado_out = self.consulta_sql_out(
    #                 nmProcedure_out, IdtReporteIni, IdtReporteFin, nmReporte
    #             )
    #             if not resultado_out.empty:
    #                 logging.info(
    #                     f"{nmReporte} contiene {len(resultado_out.index)} registros. Procediendo con la inserción de datos."
    #                 )
    #                 self.insertar_sql(txTabla, resultado_out)
    #                 return {"success": True}
    #             else:
    #                 logging.info(f"No hay datos para insertar en {txTabla}.")
    #     except Exception as e:
    #         logging.error(
    #             f"Error durante la ejecución del procedimiento para {nmReporte}: {e}"
    #         )
    #         return {"success": False}
    #     finally:
    #         logging.info("Finalizado el procedimiento de ejecución SQL.")
