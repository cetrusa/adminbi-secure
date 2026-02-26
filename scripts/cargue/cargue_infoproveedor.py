from datetime import datetime
import time
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Date
from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con
import json
from django.core.exceptions import ImproperlyConfigured

# Configuración del logging
logging.basicConfig(
    filename="cargueinfoventas.txt",
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
        raise ImproperlyConfigured(f"No se encontró el archivo de configuración {secrets_file}")

Base = declarative_base()

class Infoventas(Base):
    __tablename__ = "infoventas"
    cod_cliente = Column("Cod. cliente", String(30), primary_key=True, nullable=False)
    nom_cliente = Column("Nom. Cliente", String(100))
    cod_vendedor = Column("Cod. vendedor", String(30), primary_key=True, nullable=False)
    nombre = Column("Nombre", String(100))
    cod_productto = Column("Cod. productto", String(30), primary_key=True, nullable=False)
    descripcion = Column("Descripción", String(255))
    fecha = Column("Fecha", Date)
    fac_numero = Column("Fac. numero", String(30), primary_key=True, nullable=False)
    cantidad = Column("Cantidad", Float)
    vta_neta = Column("Vta neta", Float)
    tipo = Column("Tipo", String(30), primary_key=True, nullable=False)
    costo = Column("Costo", Float)
    unidad = Column("Unidad", String(30))
    pedido = Column("Pedido", String(30))
    proveedor = Column("Proveedor", String(100))
    empresa = Column("Empresa", String(100))
    lider = Column("Líder", String(100))
    area = Column("Área", String(100))
    codigo_bodega = Column("Codigo bodega", String(30))
    bodega = Column("Bodega", String(100))
    categoria = Column("Categoría", String(100))
    tipo_prod = Column("Tipo Prod", String(50))
    cod_barra = Column("Cod. Barra", String(50))
    nbLinea = Column("nbLinea", Integer, primary_key=True, nullable=False, default=1)

class DataBaseConnection:
    def __init__(self, config):
        print("Inicializando DataBaseConnection")
        self.config = config
        self.engine_mysql_bi = self.create_engine_mysql_bi()

    def create_engine_mysql_bi(self):
        print("Creando engine para MySQL BI")
        user = self.config.get("nmUsrIn")
        password = self.config.get("txPassIn")
        host = self.config.get("hostServerIn")
        port = self.config.get("portServerIn")
        database = self.config.get("dbBi")

        if not all([user, password, host, port, database]):
            raise ValueError("Faltan parámetros de configuración para la conexión MySQL BI")

        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

class CargueInfoVentas:
    def __init__(self, excel_file, database_name, user_id=None):
        print(f"Inicializando CargueInfoVentas con archivo {excel_file}")
        self.excel_file = excel_file
        self.database_name = database_name
        self.user_id = user_id
        self.configurar()

    def configurar(self):
        print("Configurando CargueInfoVentas")
        try:
            config_basic = ConfigBasic(self.database_name, self.user_id)
            self.config = config_basic.config
            print("Configuración del servidor:")
            print(self.config)
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.Session = sessionmaker(bind=self.engine_mysql_bi)
        except Exception as e:
            logging.error(f"Error al inicializar Interface: {e}")
            raise

    def cargar_datos_excel(self):
        print(f"Cargando datos desde el archivo Excel: {self.excel_file}")
        try:
            df = pd.read_excel(self.excel_file, sheet_name="infoventas")
            df['Fecha'] = pd.to_datetime(df['Fecha'], format="%Y/%m/%d").dt.date
            df = df.rename(columns={
                "Cod. cliente": "Cod. cliente",
                "Nom. Cliente": "Nom. Cliente",
                "Cod. vendedor": "Cod. vendedor",
                "Nombre": "Nombre",
                "Cod. productto": "Cod. productto",
                "Descripción": "Descripción",
                "Fecha": "Fecha",
                "Fac. numero": "Fac. numero",
                "Cantidad": "Cantidad",
                "Vta neta": "Vta neta",
                "Tipo": "Tipo",
                "Costo": "Costo",
                "Unidad": "Unidad",
                "Pedido": "Pedido",
                "Proveedor": "Proveedor",
                "Empresa": "Empresa",
                "Líder": "Líder",
                "Área": "Área",
                "Codigo bodega": "Codigo bodega",
                "Bodega": "Bodega",
                "Categoría": "Categoría",
                "Tipo Prod": "Tipo Prod",
                "Cod. Barra": "Cod. Barra",
            })
            return df
        except Exception as e:
            logging.error(f"Error al leer el archivo Excel: {e}")
            print(f"Error al leer el archivo Excel: {e}")
            raise

    def agrupar_datos(self, df):
        print("Agrupando datos...")
        primary_keys = [
            "Cod. cliente", "Cod. vendedor", "Cod. productto", "Fac. numero", "Tipo"
        ]
        numeric_columns = ["Cantidad", "Vta neta", "Costo"]

        # Ensure all primary key columns are present in the DataFrame
        missing_keys = [key for key in primary_keys if key not in df.columns]
        if missing_keys:
            raise ValueError(f"Faltan columnas clave primaria en el DataFrame: {missing_keys}")

        # Ensure numeric columns exist
        existing_numeric_columns = [col for col in numeric_columns if col in df.columns]

        # Define aggregation dictionary
        agg_dict = {col: 'sum' for col in existing_numeric_columns}

        # Add first for non-numeric, non-primary key columns
        for col in df.columns:
            if col not in primary_keys and col not in existing_numeric_columns:
                agg_dict[col] = 'first'

        grouped_df = df.groupby(primary_keys, dropna=False).agg(agg_dict).reset_index()

        if grouped_df.empty:
            logging.info("No hay datos para insertar después de la agrupación.")
            print("No hay datos para insertar después de la agrupación.")
            return None

        print(f"Datos agrupados. Número de registros después de la agrupación: {len(grouped_df)}")
        return grouped_df

    def insertar_datos_db(self, df):
        print("Insertando datos en la base de datos")
        start_time = time.time()
        try:
            session = self.Session()
            for index, row in df.iterrows():
                try:
                    session.execute(
                        text("""
                            INSERT IGNORE INTO infoventas (
                                `Cod. cliente`, `Nom. Cliente`, `Cod. vendedor`, `Nombre`, `Cod. productto`,
                                `Descripción`, `Fecha`, `Fac. numero`, `Cantidad`, `Vta neta`,
                                `Tipo`, `Costo`, `Unidad`, `Pedido`, `Proveedor`,
                                `Empresa`, `Líder`, `Área`, `Codigo bodega`, `Bodega`,
                                `Categoría`, `Tipo Prod`, `Cod. Barra`
                            ) VALUES (
                                :cod_cliente, :nom_cliente, :cod_vendedor, :nombre, :cod_productto,
                                :descripcion, :fecha, :fac_numero, :cantidad, :vta_neta,
                                :tipo, :costo, :unidad, :pedido, :proveedor,
                                :empresa, :lider, :area, :codigo_bodega, :bodega,
                                :categoria, :tipo_prod, :cod_barra
                            )
                        """),
                        {
                            "cod_cliente": row["Cod. cliente"],
                            "nom_cliente": row["Nom. Cliente"],
                            "cod_vendedor": row["Cod. vendedor"],
                            "nombre": row["Nombre"],
                            "cod_productto": row["Cod. productto"],
                            "descripcion": row["Descripción"],
                            "fecha": row["Fecha"],
                            "fac_numero": row["Fac. numero"],
                            "cantidad": row["Cantidad"],
                            "vta_neta": row["Vta neta"],
                            "tipo": row["Tipo"],
                            "costo": row["Costo"],
                            "unidad": row["Unidad"],
                            "pedido": row["Pedido"],
                            "proveedor": row["Proveedor"],
                            "empresa": row["Empresa"],
                            "lider": row["Líder"],
                            "area": row["Área"],
                            "codigo_bodega": row["Codigo bodega"],
                            "bodega": row["Bodega"],
                            "categoria": row["Categoría"],
                            "tipo_prod": row["Tipo Prod"],
                            "cod_barra": row["Cod. Barra"],
                        }
                    )
                except SQLAlchemyError as e:
                    logging.error(f"Error al insertar fila: {row.to_dict()}. Error: {e}")
                    session.rollback()
                    raise
            session.commit()
            session.close()
            end_time = time.time()
            logging.info(f"Datos insertados correctamente en {end_time - start_time:.2f} segundos.")
            print(f"Datos insertados correctamente en {end_time - start_time:.2f} segundos.")

        except Exception as e:
            logging.error(f"Error al insertar datos: {e}")
            print(f"Error al insertar datos: {e}")
            raise

    def procesar_cargue(self):
        print("Iniciando el proceso de carga")
        try:
            df = self.cargar_datos_excel()
            if df is None or df.empty:
                logging.info("No hay datos en el archivo Excel para procesar.")
                print("No hay datos en el archivo Excel para procesar.")
                return

            grouped_df = self.agrupar_datos(df)
            if grouped_df is None or grouped_df.empty:
                logging.info("No hay datos para insertar después de la agrupación.")
                print("No hay datos para insertar después de la agrupación.")
                return

            self.insertar_datos_db(grouped_df)
            print("Proceso de carga completado exitosamente.")
            logging.info("Proceso de carga completado exitosamente.")
        except Exception as e:
            print(f"Error durante el proceso de carga: {e}")
            logging.error(f"Error durante el proceso de carga: {e}")

# if __name__ == "__main__":
#     excel_file = "path/to/your/excel/file.xlsx"  # Reemplaza con la ruta a tu archivo Excel
#     database_name = "your_database_name"  # Reemplaza con el nombre de tu base de datos
#     cargue = CargueInfoVentas(excel_file, database_name)
#     cargue.procesar_cargue()