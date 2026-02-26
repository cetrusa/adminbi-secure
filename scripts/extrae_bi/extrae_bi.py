import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import ast
import time
import sqlalchemy
import sqlalchemy.exc
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic

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
        self.engine_mysql_bi = mysql_engine if mysql_engine else self.create_engine_mysql_bi()
        self.engine_mysql_out = mysql_engine if mysql_engine else self.create_engine_mysql_out()
        self.engine_sqlite = sqlite_engine if sqlite_engine else create_engine("sqlite:///mydata.db")

    def create_engine_mysql_bi(self):
        return con.ConexionMariadb3(
            self.config.get("nmUsrIn"),
            self.config.get("txPassIn"),
            self.config.get("hostServerIn"),
            int(self.config.get("portServerIn")),
            self.config.get("dbBi"),
        )

    def create_engine_mysql_out(self):
        return con.ConexionMariadb3(
            self.config.get("nmUsrOut"),
            self.config.get("txPassOut"),
            self.config.get("hostServerOut"),
            int(self.config.get("portServerOut")),
            self.config.get("dbSidis"),
        )

class Extrae_Bi:
    def __init__(self, database_name, IdtReporteIni, IdtReporteFin):
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.configurar(database_name)

    def configurar(self, database_name):
        try:
            self.config_basic = ConfigBasic(database_name)
            self.config = self.config_basic.config
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_out = self.db_connection.engine_mysql_out
            print("Configuraciones preliminares completadas")
        except Exception as e:
            logging.error(f"Error al inicializar configuración: {e}")
            raise

    def extractor(self):
        """
        Ejecuta el proceso de extracción para cada procedimiento definido en 'txProcedureExtrae'.
        """

        print("Iniciando extractor")

        try:
            # Obtener la lista de procedimientos a ejecutar
            txProcedureExtrae = self.config.get("txProcedureExtrae", [])

            if isinstance(txProcedureExtrae, str):
                try:
                    txProcedureExtrae = ast.literal_eval(txProcedureExtrae)
                    if not isinstance(txProcedureExtrae, list):
                        raise ValueError("txProcedureExtrae no es una lista válida.")
                except (SyntaxError, ValueError) as e:
                    logging.error(f"Error al interpretar txProcedureExtrae: {e}")
                    print(f"Error al interpretar txProcedureExtrae: {e}")
                    return {"success": False, "error": "txProcedureExtrae inválido"}

            # Validar si hay procedimientos para ejecutar
            if not txProcedureExtrae:
                logging.warning("No hay procedimientos a ejecutar en txProcedureExtrae.")
                print("No hay procedimientos a ejecutar.")
                return {"success": False, "error": "No hay procedimientos a ejecutar"}

            for a in txProcedureExtrae:
                print(f"Procesando: {a}")

                # Obtener configuración del procedimiento
                sql = text("SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = :a")
                df = self.config_basic.execute_sql_query(sql, {"a": a})

                if df.empty:
                    logging.warning(f"No se encontraron resultados para nbSql = {a}")
                    print(f"No se encontraron resultados para nbSql = {a}")
                    continue  # Salta al siguiente procedimiento

                # Asignar variables de configuración
                self.txTabla = df["txTabla"].iloc[0]
                self.nmReporte = df["nmReporte"].iloc[0]
                self.nmProcedure_out = df["nmProcedure_out"].iloc[0]
                self.nmProcedure_in = df["nmProcedure_in"].iloc[0]
                self.txSql = df["txSql"].iloc[0]
                self.txSqlExtrae = df["txSqlExtrae"].iloc[0]

                logging.info(f"Se va a procesar {self.nmReporte}")

                try:
                    self.procedimiento_a_sql()
                    logging.info(f"La información se generó con éxito de {self.nmReporte}")
                except Exception as e:
                    logging.error(f"Error en procedimiento {self.nmReporte}: {e}")
                    print(f"Error al ejecutar {self.nmReporte}: {e}")

            print("Extracción completada con éxito")
            return {"success": True}

        except Exception as e:
            logging.error(f"Error general en el extractor: {e}")
            print(f"Error general en el extractor: {e}")
            return {"success": False, "error": str(e)}

        finally:
            logging.info("Finalizado el procedimiento de ejecución SQL.")
    
    def obtener_tipos_de_datos(self, tabla):
        """ Obtiene los tipos de datos de la tabla desde INFORMATION_SCHEMA """
        query = text(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.config.get("dbBi")}' 
            AND TABLE_NAME = '{tabla}';
        """)

        try:
            with self.engine_mysql_bi.connect() as connection:
                resultado = connection.execute(query)
                return {row[0]: row[1] for row in resultado.fetchall()}
        except Exception as e:
            logging.error(f"Error obteniendo tipos de datos de {tabla}: {e}")
            return {}

    def limpiar_datos(self, df, tipos_columnas):
        """ Limpia los datos en función de los tipos de la base de datos """
        df = df.copy()
        for columna, tipo in tipos_columnas.items():
            if columna in df.columns:
                if tipo in ['varchar', 'text', 'char']:
                    df[columna] = df[columna].fillna("").astype(str)
                elif tipo in ['int', 'bigint', 'smallint']:
                    df[columna] = pd.to_numeric(df[columna], errors='coerce').fillna(0).astype(int)
                elif tipo in ['float', 'double', 'decimal']:
                    df[columna] = pd.to_numeric(df[columna], errors='coerce').fillna(0.0)
        return df

    def consulta_sql_bi(self):
        """ Ejecuta un DELETE, TRUNCATE o procedimiento en la base de datos BI """
        if not self.txSql:
            logging.warning("txSql está vacío.")
            return 0

        for intento in range(3):
            try:
                with self.engine_mysql_bi.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                    query = text(self.txSql)
                    result = connection.execute(query, {"fi": self.IdtReporteIni, "ff": self.IdtReporteFin})
                    rows_deleted = result.rowcount
                    logging.info(f"Datos eliminados: {rows_deleted} registros.")
                    return rows_deleted

            except Exception as e:
                logging.error(f"Error en consulta_sql_bi (Intento {intento+1}/3): {e}")

            if intento >= 2:
                logging.error("Se agotaron los intentos en consulta_sql_bi.")
                break

            time.sleep(5)
        return 0

    def consulta_sql_out_extrae(self, chunksize=10000):
        """ 
        Ejecuta una consulta SQL en la base de datos de salida con el nivel de aislamiento adecuado.
        Procesa los datos en chunks para evitar timeouts y reducir uso de memoria.
        
        Args:
            chunksize (int): Tamaño de cada chunk. Por defecto 10,000 registros.
        """

        max_retries = 3

        # Si `txSqlExtrae` es None o vacío, no ejecutar la extracción
        if not self.txSqlExtrae:
            logging.warning(f"No hay consulta de extracción (txSqlExtrae) definida para {self.txTabla}.")
            return None

        txSqlExtrae_upper = self.txSqlExtrae.strip().upper() if isinstance(self.txSqlExtrae, str) else ""

        isolation_level = "AUTOCOMMIT" if txSqlExtrae_upper.startswith(("INSERT", "CALL")) else "READ COMMITTED"

        for retry_count in range(max_retries):
            try:
                with self.engine_mysql_out.connect().execution_options(isolation_level=isolation_level) as connection:
                    # Leer datos en chunks para evitar timeouts
                    chunks = []
                    total_rows = 0
                    
                    logging.info(f"Iniciando lectura de datos en chunks de {chunksize} registros...")
                    
                    for chunk_num, chunk in enumerate(pd.read_sql_query(
                        text(self.txSqlExtrae), 
                        connection, 
                        params={"fi": self.IdtReporteIni, "ff": self.IdtReporteFin},
                        chunksize=chunksize
                    ), start=1):
                        chunk_rows = len(chunk)
                        total_rows += chunk_rows
                        chunks.append(chunk)
                        logging.info(f"Chunk {chunk_num}: {chunk_rows:,} registros leídos (Total acumulado: {total_rows:,})")
                    
                    if chunks:
                        resultado = pd.concat(chunks, ignore_index=True)
                        logging.info(f"Consulta ejecutada con éxito en {isolation_level}. Total: {total_rows:,} registros.")
                        return resultado
                    else:
                        logging.warning("No se obtuvieron datos de la consulta.")
                        return pd.DataFrame()

            except (sqlalchemy.exc.IntegrityError, sqlalchemy.exc.ProgrammingError) as e:
                logging.error(f"Error en consulta_sql_out_extrae (Intento {retry_count+1}/3): {e}")

            if retry_count == max_retries - 1:
                logging.error("Se agotaron los intentos en consulta_sql_out_extrae.")
                return None

            time.sleep(1)


    def insertar_sql(self, df, chunksize=5000):
        """ 
        Inserta los datos en la base de datos de BI en chunks para evitar timeouts.
        
        Args:
            df (DataFrame): DataFrame con los datos a insertar
            chunksize (int): Tamaño de cada chunk. Por defecto 5,000 registros.
        """
        try:
            total_registros = len(df)
            logging.info(f"Intentando insertar {total_registros:,} registros en {self.txTabla}")
            
            if total_registros == 0:
                logging.warning(f"No hay registros para insertar en {self.txTabla}")
                return
            
            # Insertar en chunks si hay muchos registros
            if total_registros > chunksize:
                logging.info(f"Insertando en chunks de {chunksize:,} registros...")
                registros_insertados = 0
                
                for i in range(0, total_registros, chunksize):
                    chunk = df.iloc[i:i + chunksize]
                    chunk_size = len(chunk)
                    
                    with self.engine_mysql_bi.connect() as connection:
                        cursorbi = connection.execution_options(isolation_level="READ COMMITTED")
                        chunk.to_sql(name=self.txTabla, con=cursorbi, if_exists="append", index=False)
                    
                    registros_insertados += chunk_size
                    logging.info(f"Chunk insertado: {chunk_size:,} registros (Total: {registros_insertados:,}/{total_registros:,})")
                
                logging.info(f"Se insertaron {registros_insertados:,} registros en {self.txTabla}")
            else:
                # Si son pocos registros, insertar todo de una vez
                with self.engine_mysql_bi.connect() as connection:
                    cursorbi = connection.execution_options(isolation_level="READ COMMITTED")
                    df.to_sql(name=self.txTabla, con=cursorbi, if_exists="append", index=False)
                logging.info(f"Se insertaron {total_registros:,} registros en {self.txTabla}")

        except Exception as e:
            logging.error(f"Error al insertar datos en {self.txTabla}: {e}")
            raise

    def procedimiento_a_sql(self, read_chunksize=10000, insert_chunksize=5000):
        """ 
        Ejecuta el proceso de eliminación y luego la extracción e inserción de datos.
        
        Args:
            read_chunksize (int): Tamaño de chunks para lectura (por defecto 10,000)
            insert_chunksize (int): Tamaño de chunks para inserción (por defecto 5,000)
        """

        for intento in range(3):
            try:
                logging.info(f"Procesando tabla {self.txTabla}")
                tipos_columnas = self.obtener_tipos_de_datos(self.txTabla)

                # Determinar si txSql es un DELETE, TRUNCATE o INSERT
                txSql_upper = self.txSql.strip().upper() if isinstance(self.txSql, str) else ""

                if txSql_upper.startswith("INSERT"):
                    # Si es un INSERT, simplemente ejecutar la consulta sin contar eliminaciones
                    with self.engine_mysql_bi.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                        query = text(self.txSql)
                        result = connection.execute(query, {"fi": self.IdtReporteIni, "ff": self.IdtReporteFin})
                        rows_inserted = result.rowcount
                        logging.info(f"Datos insertados: {rows_inserted} registros.")

                    return  # No se necesita extracción de datos en este caso
                
                # Si es un DELETE o TRUNCATE, proceder con la extracción
                rows_deleted = self.consulta_sql_bi() if self.txSql else 0
                delete_success = ("TRUNCATE" in txSql_upper) or (rows_deleted > 0)

                resultado_out = self.consulta_sql_out_extrae(chunksize=read_chunksize)

                if resultado_out is not None and not resultado_out.empty:
                    registros_extraidos = len(resultado_out)
                    logging.info(f"Registros extraídos: {registros_extraidos}")

                    resultado_limpio = self.limpiar_datos(resultado_out, tipos_columnas)

                    if delete_success:
                        self.insertar_sql(resultado_limpio, chunksize=insert_chunksize)
                    else:
                        logging.warning("No se eliminaron registros en consulta_sql_bi, pero se insertarán datos.")
                        self.insertar_sql(resultado_limpio, chunksize=insert_chunksize)
                else:
                    logging.warning("No se obtuvieron resultados en consulta_sql_out_extrae.")

                logging.info(f"Proceso completado para {self.txTabla}.")
                return

            except Exception as e:
                logging.error(f"Error en procedimiento_a_sql (Intento {intento+1}/3): {e}")

            time.sleep(5)
