from datetime import datetime

import zipfile
import os
import numpy as np
import pandas as pd
import logging

# from scripts.conexion import Conexion as con
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

import sqlite3
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, String, Float, Date

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


Base = declarative_base()


class HistoricoCostoPromedio(Base):
    __tablename__ = (
        "HistoricoCostoPromedio"  # El nombre de tu tabla en la base de datos
    )

    # Define las columnas de acuerdo a tu esquema de base de datos
    fecha = Column(Date, primary_key=True)
    almacen = Column(String, primary_key=True)
    producto = Column(String, primary_key=True)
    costoPromedioInicial = Column(Float)
    unidadesInicial = Column(Float)
    costoCompradia = Column(Float)
    unidadesCompradia = Column(Float)
    unidadesMovimientodia = Column(Float)
    costoPromedioFinal = Column(Float)
    unidadesFinal = Column(Float)


class CargueHistoricoCostos:
    """
    Clase para manejar el proceso de carga de historico de costos.

    Esta clase se encargará de procesar los archivos planos, realizar limpieza y validaciones
    necesarias, y cargar los datos en la base de datos.
    """

    def __init__(self, database_name):
        """
        Inicializa la instancia de CargueHistoricoCostos.

        Args:
            database_name (str): Nombre de la base de datos.

        Raises:
            ValueError: Si el nombre de la base de datos es vacío o nulo.
        """

        if not database_name:
            raise ValueError("El nombre de la base de datos no puede ser vacío o nulo.")

        self.database_name = database_name
        self.configurar()

    def configurar(self):
        """
        Configura la conexión a las bases de datos y establece las variables de entorno necesarias.

        Esta función crea una configuración básica y establece las conexiones a las bases de datos MySQL y SQLite
        utilizando los parámetros de configuración.

        Raises:
            Exception: Propaga cualquier excepción que ocurra durante la configuración.
        """

        try:
            # Crear un objeto de configuración básica
            config_basic = ConfigBasic(self.database_name)
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

    def obtener_fechas_de_movimientos(self, fecha):
        with self.engine_mysql_bi.connect() as connection:
            consulta_fechas = text(
                """
                SELECT 
                    DATE(m.dtContabilizacion) AS fecha,
                    CAST(m.nbAlmacen AS CHAR) almacen,
                    CAST(m.nbProducto AS CHAR) producto
                FROM mmovlogistico m
                WHERE DATE(m.dtContabilizacion) <= :fecha
                GROUP BY DATE(m.dtContabilizacion), m.nbAlmacen, m.nbProducto;
                """
            )
            df_fechas = pd.read_sql_query(
                consulta_fechas, connection, params={"fecha": fecha}
            )
            # df_fechas.set_index(["fecha", "almacen", "producto"], inplace=True)
            print(df_fechas)
            return df_fechas

    def cargar_compras(self, fecha):
        """Carga las compras en un DataFrame.

        Args:
            fecha: Fecha a procesar.

        Returns:
            DataFrame con las compras.
        """

        parametros = {"fecha": fecha}

        sqlcompras = text(
            """
            SELECT 
            DATE(m.dtContabilizacion) fecha,
            CAST(m.nbAlmacen AS CHAR) almacen,
            CAST(m.nbProducto AS CHAR) producto, 
            COALESCE(((SUM((m.flPrecioUnitario * m.flCantidad))) / (SUM(m.flCantidad))),0) AS costoCompradia, 
            COALESCE(SUM(m.flCantidad),0) AS unidadesCompradia
            FROM mmovlogistico m
            WHERE m.nbMovimientoClase = '200' 
            AND DATE(m.dtContabilizacion) = :fecha
            GROUP BY DATE(m.dtContabilizacion), m.nbAlmacen, m.nbProducto;
            """
        )

        df_compras = pd.read_sql(
            sqlcompras,
            self.engine_mysql_bi,
            params=parametros,
        )

        if df_compras.empty:
            df_compras = pd.DataFrame(
                columns=[
                    "fecha",
                    "almacen",
                    "producto",
                    "costoCompradia",
                    "unidadesCompradia",
                ]
            ).astype(
                {
                    "fecha": "datetime64[ns]",
                    "almacen": "object",
                    "producto": "object",
                    "costoCompradia": "float",
                    "unidadesCompradia": "int",
                }
            )

        df_compras.set_index(["fecha", "almacen", "producto"], inplace=True)
        print(df_compras)
        return df_compras

    def cargar_movimientos(self, fecha):
        """Carga los movimientos en un DataFrame.

        Args:
            fecha: Fecha a procesar.

        Returns:
            DataFrame con los movimientos.
        """

        parametros = {"fecha": fecha}

        sqlmovimientos = text(
            """
            SELECT
                DATE( m.dtContabilizacion ) fecha,
                CAST(m.nbAlmacen AS CHAR) almacen,
                CAST(m.nbProducto AS CHAR) producto,
                COALESCE ( SUM( m.flCantidad ), 0 ) AS unidadesMovimientodia
            FROM
                mmovlogistico m 
            WHERE
                m.nbMovimientoClase != '200' 
                AND DATE( m.dtContabilizacion ) = :fecha
            GROUP BY
                DATE( m.dtContabilizacion ),
                m.nbAlmacen,
                m.nbProducto;
            """
        )

        df_otros_movimientos = pd.read_sql(
            sqlmovimientos,
            self.engine_mysql_bi,
            params=parametros,
        )

        if df_otros_movimientos.empty:
            df_otros_movimientos = pd.DataFrame(
                columns=["fecha", "almacen", "producto", "unidadesMovimientodia"]
            ).astype(
                {
                    "fecha": "datetime64[ns]",
                    "almacen": "object",
                    "producto": "object",
                    "unidadesMovimientodia": "int",
                }
            )

        df_otros_movimientos.set_index(["fecha", "almacen", "producto"], inplace=True)

        return df_otros_movimientos

    def cargar_costos_iniciales(self, fecha):
        """Carga los costos iniciales para un almacén y producto específicos en un DataFrame.

        Args:
            almacen (str): El código o identificador del almacén.
            producto (str): El código o identificador del producto.

        Returns:
            DataFrame con los costos iniciales para el almacén y producto especificados.
            Si no se encuentran datos, devuelve un DataFrame vacío con la estructura de columnas esperada.
        """

        print(f"Cargando costos iniciales ...")
        parametros = {"fecha": fecha}
        sql_costos_iniciales = text(
            """
            SELECT DISTINCT
                DATE(:fecha) AS fecha,
                CAST( m.nbAlmacen AS CHAR ) AS almacen,
                CAST( m.nbProducto AS CHAR ) AS producto,
                MIN(
                DATE( m.dtContabilizacion )) AS fechaminima,
                m.flPrecioUnitario AS costoPromedioInicial_inicial 
            FROM
                mmovlogistico m
                INNER JOIN cmovimientoclase c ON m.nbMovimientoClase = c.nbMovimientoClase 
            WHERE
                c.tpMovimientoClase = 'E' 
                AND m.nbMovimientoClase NOT IN ( '601', '603' ) 
                AND DATE( m.dtContabilizacion ) <= :fecha 
            GROUP BY
                m.nbAlmacen,
                m.nbProducto;
            """
        )

        df_costos_iniciales = pd.read_sql(
            sql_costos_iniciales,
            self.engine_mysql_bi,
            params=parametros,
        )

        if df_costos_iniciales.empty:
            print(f"No se encontraron costos iniciales.")
            df_costos_iniciales = pd.DataFrame(
                columns=["fecha", "almacen", "producto", "costoPromedioInicial_inicial"]
            ).astype(
                {
                    "fecha": "datetime64[ns]",
                    "almacen": "object",
                    "producto": "object",
                    "costoPromedioInicial_inicial": "float",
                }
            )

        df_costos_iniciales.set_index(["fecha", "almacen", "producto"], inplace=True)
        print(df_costos_iniciales)
        return df_costos_iniciales

    def procesar_todas_las_fechas(self, fecha):
        # Obtener todas las fechas de movimientos hasta la fecha de corte
        df_fechas = self.obtener_fechas_de_movimientos(fecha)

        # Ahora df_fechas debería contener solo fechas únicas
        fechas_unicas = df_fechas["fecha"].drop_duplicates()

        for fecha in fechas_unicas:
            try:
                print(f"Procesando datos para la fecha: {fecha}")
                # Llamar a procesar_datos_por_fecha para cada fecha
                self.procesar_datos_por_fecha(fecha)
            except Exception as e:
                print(f"Error al procesar la fecha {fecha}: {e}")
                continue

    def procesar_datos_por_fecha(self, fecha):
        """Procesa los datos para una fecha específica."""
        print("Procesando datos por fecha")
        # Cargar datos de compras y otros movimientos
        df_fechas = self.obtener_fechas_de_movimientos(fecha)
        df_compras = self.cargar_compras(fecha)
        print("df compras cargado")
        df_otros_movimientos = self.cargar_movimientos(fecha)
        print("df movimientos cargado")
        df_costos_iniciales = self.cargar_costos_iniciales(fecha)
        print("df costos iniciales cargado")
        df_historico_previo = self.get_historico_previo(fecha)
        print(df_historico_previo)
        print("listo el historico")
        # Unificar los DataFrames
        df_total_movimientos = df_fechas
        if df_costos_iniciales.empty:
            df_total_movimientos['costoPromedioInicial_inicial'] = np.nan
        else:
            df_total_movimientos = pd.merge(
                df_total_movimientos,
                df_costos_iniciales,
                on=["fecha", "almacen", "producto"],
                how="left",
            )
        print(df_total_movimientos)
        if df_otros_movimientos.empty:
            df_total_movimientos['unidadesMovimientodia'] = np.nan
        else:
            df_total_movimientos = pd.merge(
                df_total_movimientos,
                df_otros_movimientos,
                on=["fecha", "almacen", "producto"],
                how="left",
            )
        print(df_total_movimientos)
        if df_compras.empty:
            df_total_movimientos['costoCompradia'] = np.nan
            df_total_movimientos['unidadesCompradia'] = np.nan
        else:
            df_total_movimientos = pd.merge(
                df_total_movimientos,
                df_compras,
                on=["fecha", "almacen", "producto"],
                how="left",
            )
        print(df_total_movimientos)
        if df_historico_previo.empty:
            df_total_movimientos['costoPromedioInicial_previo'] = np.nan
            df_total_movimientos['unidadesInicial_previo'] = np.nan
        else:
            df_total_movimientos = pd.merge(
                df_total_movimientos,
                df_historico_previo,
                on=["fecha", "almacen", "producto"],
                how="left",
            )
        print(df_total_movimientos)
        print("Unificando compras y movimientos")
        if df_total_movimientos.empty:
            print(f"No hay movimientos para procesar en la fecha {fecha}.")
            return

        # Asegurar que los valores faltantes sean tratados correctamente
        df_total_movimientos.fillna(
            {
                "unidadesMovimientodia": 0,
                "costoCompradia": 0,
                "unidadesCompradia": 0,
                "costoPromedioInicial_inicial": 0,
                "costoPromedioInicial_previo": 0,
                "unidadesInicial_previo": 0,
            },
            inplace=True,
        )
        print("Terminamos el indice e iniciamos historico")
        print(df_total_movimientos)
        # Obtener el último registro de HistoricoCostoPromedio para todos los almacenes y productos

        # Aquí necesitas ajustar `_calcular_valores_dia` para que funcione sobre el DataFrame completo o grupos.
        df_resultado = self.calcular_valores_dia(fecha, df_total_movimientos)
        print("estoy listo para insertar los datos")
        print(df_resultado)
        # Insertar o actualizar registros en la base de datos
        self.insertar_o_actualizar(df_resultado)

    def calcular_valores_dia(self, fecha, df_total_movimientos):
        # Copia del DataFrame para evitar SettingWithCopyWarning y asegurar manipulación sin afectar el original
        df_filtrado = df_total_movimientos[
            df_total_movimientos["fecha"] == fecha
        ].copy()

        if df_filtrado.empty:
            print("No hay movimientos para la fecha proporcionada.")
            return pd.DataFrame()

        try:
            # Preprocesamiento: Llenar NaNs y reemplazar infinitos
            df_filtrado.fillna(0, inplace=True)
            df_filtrado.replace([np.inf, -np.inf], 0, inplace=True)
            print(df_filtrado.columns.tolist())
            print(df_filtrado)
            # Ajuste inicial de CostoPromedioInicial y UnidadesInicial
            df_filtrado["costoPromedioInicial"] = np.where(
                df_filtrado["costoPromedioInicial_previo"] == 0,
                df_filtrado["costoPromedioInicial_inicial"],
                df_filtrado["costoPromedioInicial_previo"],
            )

            df_filtrado["unidadesInicial"] = np.where(
                df_filtrado["unidadesInicial_previo"] == 0,
                0,
                df_filtrado["unidadesInicial_previo"],
            )

            # Aplicación de condiciones adicionales y cálculos

            # Cálculo de UnidadesFinal
            df_filtrado["unidadesFinal"] = (
                df_filtrado["unidadesInicial"]
                + df_filtrado["unidadesCompradia"]
                + df_filtrado["unidadesMovimientodia"]
            )

            # Cálculo optimizado de CostoPromedioFinal para evitar divisiones por cero
            df_filtrado["costoPromedioFinal"] = np.where(
                df_filtrado["unidadesFinal"] > 0,
                # Aquí se realiza el cálculo seguro sin riesgo de división por cero
                (
                    (
                        df_filtrado["costoPromedioInicial"]
                        * df_filtrado["unidadesInicial"]
                    )
                    + (df_filtrado["costoCompradia"] * df_filtrado["unidadesCompradia"])
                    + (
                        df_filtrado["costoPromedioInicial"]
                        * df_filtrado["unidadesMovimientodia"]
                    )
                )
                / df_filtrado["unidadesFinal"],
                0,  # Valor por defecto para evitar la división por cero
            )

            # Selección y renombrado de columnas para el resultado final
            columnas_resultado = [
                "fecha",
                "almacen",
                "producto",
                "costoPromedioInicial",
                "unidadesInicial",
                "costoCompradia",
                "unidadesCompradia",
                "unidadesMovimientodia",
                "costoPromedioFinal",
                "unidadesFinal",
            ]
            df_resultado = df_filtrado[columnas_resultado]

            print("Hemos logrado terminar de organizar los datos del día.")
        except Exception as e:
            print(f"Error al calcular valores por día: {e}")
            df_resultado = pd.DataFrame()

        print(df_resultado.columns.tolist())

        return df_resultado

    def insertar_o_actualizar(self, df_resultado):
        # Inicializa la sesión de SQLAlchemy
        Session = sessionmaker(bind=self.engine_mysql_bi)
        session = Session()

        try:
            # Convertir el DataFrame a una lista de diccionarios para procesamiento
            data_to_process = df_resultado.to_dict(orient="records")

            # Primero, determina qué registros son nuevos y cuáles necesitan ser actualizados
            # Esto puede requerir una lógica específica basada en tu modelo de datos y requisitos
            nuevos_registros = self.filtrar_nuevos_registros(data_to_process, session)
            registros_a_actualizar = self.determinar_registros_a_actualizar(
                data_to_process, session
            )

            # Inserta los nuevos registros
            if nuevos_registros:
                self.insertar_nuevos_registros(nuevos_registros, session)

            # Actualiza los registros existentes
            if registros_a_actualizar:
                self.actualizar_registros_existentes(registros_a_actualizar, session)

            # Compromete la transacción
            session.commit()
            print("Todos los registros insertados/actualizados con éxito.")

        except IntegrityError as e:
            # Manejo de errores de integridad, como violaciones de clave única
            session.rollback()
            print(f"Error de integridad al insertar datos: {e}")
        except Exception as e:
            # Manejo de cualquier otro tipo de error
            session.rollback()
            print(f"Error al insertar datos: {e}")
        finally:
            # Asegura que la sesión siempre se cierre al final
            session.close()

    def filtrar_nuevos_registros(self, data_to_insert, session):
        # Obtener todos los identificadores únicos existentes como una lista de tuplas (fecha, almacen, producto)
        identificadores_existentes = session.query(
            HistoricoCostoPromedio.fecha,
            HistoricoCostoPromedio.almacen,
            HistoricoCostoPromedio.producto,
        ).all()
        identificadores_existentes = set(
            identificadores_existentes
        )  # Convertir a un conjunto para búsquedas más rápidas

        # Filtrar los registros en data_to_insert para conservar solo aquellos que no existen en identificadores_existentes
        nuevos_registros = [
            registro
            for registro in data_to_insert
            if (registro["fecha"], registro["almacen"], registro["producto"])
            not in identificadores_existentes
        ]

        return nuevos_registros

    def determinar_registros_a_actualizar(self, data_to_insert, session):
        # Obtener todos los identificadores únicos existentes como una lista de tuplas (fecha, almacen, producto)
        identificadores_existentes = session.query(
            HistoricoCostoPromedio.fecha,
            HistoricoCostoPromedio.almacen,
            HistoricoCostoPromedio.producto,
        ).all()
        identificadores_existentes = set(
            identificadores_existentes
        )  # Convertir a un conjunto para búsquedas más rápidas

        # Filtrar los registros en data_to_insert para conservar solo aquellos que no existen en identificadores_existentes
        registros_a_actualizar = [
            registro
            for registro in data_to_insert
            if (registro["fecha"], registro["almacen"], registro["producto"])
            in identificadores_existentes
        ]

        return registros_a_actualizar

    def insertar_nuevos_registros(self, nuevos_registros, session):
        objects_to_save = [HistoricoCostoPromedio(**data) for data in nuevos_registros]
        session.bulk_save_objects(objects_to_save)

    def actualizar_registros_existentes(self, registros_a_actualizar, session):
        for registro in registros_a_actualizar:
            # Ajustar para usar la clave compuesta de fecha, almacen y producto.
            obj = (
                session.query(HistoricoCostoPromedio)
                .filter_by(
                    fecha=registro["fecha"],
                    almacen=registro["almacen"],
                    producto=registro["producto"],
                )
                .first()
            )
            if obj:
                for clave, valor in registro.items():
                    setattr(obj, clave, valor)

    def get_historico_previo(self, fecha):
        print("Iniciando el get_historico")
        """Obtiene el último registro de HistoricoCostoPromedio para todas las combinaciones de almacén y producto hasta una fecha específica."""
        query = text(
            """
            SELECT DATE(:fecha) fecha,h.almacen, h.producto, h.costoPromedioFinal AS "costoPromedioInicial_previo", h.unidadesFinal AS "unidadesInicial_previo"
            FROM HistoricoCostoPromedio h
            INNER JOIN (
                SELECT DATE(:fecha) AS fecha, almacen, producto, MAX(fecha) AS maxFecha
                FROM HistoricoCostoPromedio
                WHERE fecha < :fecha
                GROUP BY almacen, producto
            ) AS subquery ON h.almacen = subquery.almacen AND h.producto = subquery.producto AND h.fecha = subquery.maxFecha
        """
        )
        df_historico_previo = pd.read_sql(
            query, self.engine_mysql_bi, params={"fecha": fecha}
        )

        if df_historico_previo.empty:
            print(
                "No se encontraron registros históricos previos. Creando un DataFrame vacío con estructura definida."
            )
            df_historico_previo = pd.DataFrame(
                columns=[
                    "fecha",
                    "almacen",
                    "producto",
                    "costoPromedioInicial_previo",
                    "unidadesInicial_previo",
                ]
            ).astype(
                {
                    "fecha": "datetime64[ns]",
                    "almacen": "object",
                    "producto": "object",
                    "costoPromedioInicial_previo": "float",
                    "unidadesInicial_previo": "int",
                }
            )

        df_historico_previo.set_index(["fecha", "almacen", "producto"], inplace=True)
        print("termino historico")
        print(df_historico_previo)

        return df_historico_previo
