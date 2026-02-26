from datetime import datetime
import time
import pandas as pd
import logging
from sqlalchemy import create_engine, text, tuple_
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Date
from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con
import json
from django.core.exceptions import ImproperlyConfigured
import os
import uuid

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
        raise ImproperlyConfigured(
            f"No se encontró el archivo de configuración {secrets_file}"
        )


class DataBaseConnection:
    def __init__(self, config):
        print("Inicializando DataBaseConnection")
        self.config = config
        self.engine_mysql_bi = self.create_engine_mysql_bi()
        # Configuramos el engine SQLite con opciones optimizadas
        sqlite_table_name = getattr(
            self, "sqlite_table_name", f"cargueinfoventas_{uuid.uuid4().hex[:8]}"
        )
        sqlite_path = os.path.join("media", f"temp_{sqlite_table_name}.db")
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        self.engine_sqlite = create_engine(
            f"sqlite:///{sqlite_path}",
            connect_args={
                "timeout": 60,  # Aumentamos timeout para operaciones largas
                "check_same_thread": False,
                "isolation_level": None,  # Autocommit mode para mejor rendimiento
            },
            # Optimizaciones de pool de conexiones
            pool_recycle=1800,  # 30 minutos
            pool_pre_ping=True,  # Verifica conexiones antes de usarlas
            pool_size=10,  # Tamaño del pool para paralelización
            max_overflow=20,  # Conexiones adicionales si el pool está lleno
        )

        # Aplicamos optimizaciones PRAGMA a SQLite
        self._optimize_sqlite()

    def _optimize_sqlite(self):
        """Configura optimizaciones para SQLite"""
        try:
            with self.engine_sqlite.connect() as conn:
                # Aumentamos la memoria caché a 200MB para mejorar rendimiento con datasets grandes
                conn.execute(text("PRAGMA cache_size = -200000"))
                # Usar modo WAL para mejor concurrencia y rendimiento
                conn.execute(text("PRAGMA journal_mode = WAL"))
                # Ajustar sincronización para mejor rendimiento
                conn.execute(text("PRAGMA synchronous = NORMAL"))
                # Ajustar tamaño de página para mejor rendimiento
                conn.execute(
                    text("PRAGMA page_size = 8192")
                )  # 8KB para mejor manejo de datos grandes
                # Tamaño mínimo para vacuum automático
                conn.execute(text("PRAGMA auto_vacuum = INCREMENTAL"))
                # Aumentamos el mmap a 2GB para datasets más grandes
                conn.execute(text("PRAGMA mmap_size = 2147483648"))  # 2GB
                # Mejora en el manejo de índices
                conn.execute(
                    text("PRAGMA temp_store = MEMORY")
                )  # Usar memoria para temp
                # Optimizar operaciones secuenciales
                conn.execute(text("PRAGMA locking_mode = EXCLUSIVE"))
            print("Optimizaciones SQLite avanzadas aplicadas correctamente")
        except Exception as e:
            print(f"Error aplicando optimizaciones SQLite: {e}")
            logging.warning(f"Error aplicando optimizaciones SQLite: {e}")

    def create_engine_mysql_bi(self):
        print("Creando engine para MySQL BI")
        user = self.config.get("nmUsrIn")
        password = self.config.get("txPassIn")
        host = self.config.get("hostServerIn")
        port = self.config.get("portServerIn")
        database = self.config.get("dbBi")

        if not all([user, password, host, port, database]):
            raise ValueError(
                "Faltan parámetros de configuración para la conexión MySQL BI"
            )

        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def execute_query_mysql_chunked(
        self, query, table_name, chunksize=50000, params=None
    ):
        """
        Ejecuta una consulta en MySQL y guarda los resultados en SQLite con optimizaciones.
        Automáticamente detecta y crea índices para mejorar rendimiento.
        """
        print(
            f"Ejecutando consulta en MySQL de manera chunked para la tabla {table_name}"
        )
        try:
            self.eliminar_tabla_sqlite(table_name)

            # Variables para seguimiento de progreso
            total_procesados = 0
            chunk_num = 0
            indices = []
            inicio = time.time()  # Para medir el tiempo de ejecución

            with self.engine_mysql_bi.connect() as connection:
                cursor = connection.execution_options(
                    isolation_level="READ COMMITTED",
                    stream_results=True,  # Para reducir uso de memoria
                )

                # Configuramos opciones para mejor rendimiento en inserción masiva
                with self.engine_sqlite.connect() as sqlite_conn:
                    # Deshabilitar restricciones y sincronización durante carga masiva
                    sqlite_conn.execute(text("PRAGMA foreign_keys = OFF"))
                    sqlite_conn.execute(text("PRAGMA synchronous = OFF"))
                    # Iniciar transacción para toda la operación
                    sqlite_trans = sqlite_conn.begin()

                    try:
                        # Procesar por chunks para reducir uso de memoria
                        for chunk in pd.read_sql_query(
                            query, con=cursor, chunksize=chunksize, params=params
                        ):
                            chunk_num += 1
                            total_procesados += len(chunk)
                            print(
                                f"Procesando chunk #{chunk_num}: {len(chunk)} registros (Total: {total_procesados})"
                            )

                            # Optimizar tipos de datos para reducir uso de memoria
                            chunk = self._optimize_dataframe(chunk)

                            # Detectar columnas para indexar solo en el primer chunk
                            if chunk_num == 1:
                                indices = self._detect_index_columns(chunk)

                            # Guardar chunk en SQLite
                            chunk.to_sql(
                                name=table_name,
                                con=sqlite_conn,  # Usar la conexión de la transacción
                                if_exists="append",
                                index=False,
                                method="multi",  # Método más rápido para inserción masiva
                            )

                            # Liberar memoria explícitamente
                            del chunk

                            # Reportar progreso periódicamente
                            if chunk_num % 5 == 0:
                                tiempo_actual = time.time() - inicio
                                velocidad = (
                                    total_procesados / tiempo_actual
                                    if tiempo_actual > 0
                                    else 0
                                )
                                print(
                                    f"Progreso: {total_procesados} registros en {tiempo_actual:.2f} segundos "
                                    f"({velocidad:.2f} registros/seg)"
                                )

                        # Confirmar la transacción completa
                        sqlite_trans.commit()
                    except Exception as e:
                        # Revertir en caso de error
                        sqlite_trans.rollback()
                        raise e
                    finally:
                        # Restaurar configuración normal
                        sqlite_conn.execute(text("PRAGMA foreign_keys = ON"))
                        sqlite_conn.execute(text("PRAGMA synchronous = NORMAL"))

            # Creamos índices después de cargar todos los datos para mejorar rendimiento
            self._create_indices(table_name, indices)

            # Obtener y reportar el conteo total y tiempo de ejecución
            with self.engine_sqlite.connect() as connection:
                total_records = connection.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).fetchone()[0]

            tiempo_total = time.time() - inicio
            print(
                f"Total de registros insertados en la tabla {table_name}: {total_records}"
            )
            print(f"Tiempo total de procesamiento: {tiempo_total:.2f} segundos")
            print(
                f"Velocidad promedio: {total_records/tiempo_total if tiempo_total > 0 else 0:.2f} registros/segundo"
            )

            return total_records

        except Exception as e:
            logging.error(f"Error al ejecutar el query: {e}")
            print(f"Error al ejecutar el query: {e}")
            raise

    def _optimize_dataframe(self, df):
        """Optimiza los tipos de datos en el DataFrame para reducir uso de memoria"""
        for col in df.columns:
            # Convertir columnas de objetos a categorías si son apropiadas
            if df[col].dtype == "object":
                if (
                    df[col].nunique() < len(df) * 0.5
                ):  # Si hay menos de 50% de valores únicos
                    df[col] = df[col].astype("category")

            # Convertir enteros a tipos más pequeños si es posible
            elif pd.api.types.is_integer_dtype(df[col]):
                c_min, c_max = df[col].min(), df[col].max()

                if c_min >= 0:
                    if c_max < 255:
                        df[col] = df[col].astype("uint8")
                    elif c_max < 65535:
                        df[col] = df[col].astype("uint16")
                    elif c_max < 4294967295:
                        df[col] = df[col].astype("uint32")
                else:
                    if c_min > -128 and c_max < 127:
                        df[col] = df[col].astype("int8")
                    elif c_min > -32768 and c_max < 32767:
                        df[col] = df[col].astype("int16")
                    elif c_min > -2147483648 and c_max < 2147483647:
                        df[col] = df[col].astype("int32")

            # Optimizar flotantes
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype("float32")

        return df

    def _detect_index_columns(self, df):
        """Detecta inteligentemente las columnas a indexar basado en cardinalidad y patrones"""
        indices = []

        # Analizar cardinalidad relativa para identificar buenos candidatos para índices
        cardinalidad = {}
        for col in df.columns:
            if df[col].dtype != "object" and not pd.api.types.is_categorical_dtype(
                df[col]
            ):
                continue

            # Calcular ratio de valores únicos (cardinalidad relativa)
            unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
            cardinalidad[col] = unique_ratio

        # Buenos candidatos para índices: baja-media cardinalidad (0.001 a 0.2)
        buenos_candidatos = {k: v for k, v in cardinalidad.items() if 0.001 <= v <= 0.2}

        # Patrones de columnas frecuentemente usadas en filtros y joins
        patrones_clave = [
            "id",
            "codigo",
            "fecha",
            "cliente",
            "producto",
            "factura",
            "key",
            "num",
            "vendedor",
            "zona",
            "periodo",
            "almacen",
        ]

        # Priorizar columnas de bajo-medio cardinal con patrones conocidos
        for col, ratio in sorted(buenos_candidatos.items(), key=lambda x: x[1]):
            col_lower = col.lower()
            if any(patron in col_lower for patron in patrones_clave):
                indices.append(col)
                if (
                    len(indices) >= 3
                ):  # Limitamos a 3 índices iniciales para no demorar la carga
                    break

        # Siempre indexar columnas de fecha si existen, son críticas para filtrado
        for col in df.columns:
            if pd.api.types.is_datetime64_dtype(df[col]) and col not in indices:
                indices.append(col)
                break

        # Añadir más índices si hay pocos candidatos previos
        if len(indices) < 2:
            # Añadir columnas con alta cardinalidad que probablemente sean claves primarias
            for col, ratio in sorted(
                cardinalidad.items(), key=lambda x: x[1], reverse=True
            ):
                if ratio > 0.8 and col not in indices:  # Probablemente clave única
                    indices.append(col)
                    if len(indices) >= 3:
                        break

        return indices

    def _create_indices(self, table_name, columns):
        """Crea índices optimizados en la tabla SQLite"""
        if not columns:
            return

        with self.engine_sqlite.connect() as connection:
            # Desactivar las restricciones temporalmente para acelerar creación de índices
            connection.execute(text("PRAGMA foreign_keys = OFF"))

            # Iniciar transacción para todos los índices
            trans = connection.begin()
            try:
                # Crear estadísticas para consultas optimizadas
                connection.execute(text(f"ANALYZE {table_name}"))

                # Crear índices para cada columna
                for col in columns:
                    try:
                        # Nombre seguro para el índice
                        idx_name = f"idx_{table_name}_{col}".replace(".", "_").replace(
                            " ", "_"
                        )
                        # Crear índice si no existe
                        connection.execute(
                            text(
                                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({col})"
                            )
                        )
                        print(f"Índice creado para columna {col} en tabla {table_name}")
                    except Exception as e:
                        print(f"Error creando índice para columna {col}: {e}")
                        logging.warning(f"Error creando índice para columna {col}: {e}")

                # Si hay múltiples columnas, podemos crear un índice compuesto
                if len(columns) >= 2:
                    cols_str = ", ".join(
                        columns[:3]
                    )  # Tomamos máximo 3 columnas para índice compuesto
                    idx_name = f"idx_{table_name}_compound".replace(".", "_").replace(
                        " ", "_"
                    )
                    try:
                        connection.execute(
                            text(
                                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({cols_str})"
                            )
                        )
                        print(
                            f"Índice compuesto creado para columnas {cols_str} en tabla {table_name}"
                        )
                    except Exception as e:
                        print(f"Error creando índice compuesto: {e}")

                # Confirmar todos los cambios
                trans.commit()
                # Actualizar estadísticas para el optimizador de consultas
                connection.execute(text(f"ANALYZE {table_name}"))
            except Exception as e:
                # Revertir en caso de error
                trans.rollback()
                print(f"Error al crear índices en tabla {table_name}: {e}")
            finally:
                # Reactivar restricciones
                connection.execute(text("PRAGMA foreign_keys = ON"))

    def eliminar_tabla_sqlite(self, table_name):
        print(f"Eliminando tabla {table_name} en SQLite")
        sql = text(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine_sqlite.connect() as connection:
            connection.execute(sql)
        print(f"Tabla {table_name} eliminada exitosamente")
        logging.info(f"Tabla {table_name} eliminada exitosamente")


Base = declarative_base()


class FactVentasItems(Base):
    __tablename__ = "fact_ventas_items"

    empresa = Column(String(30), primary_key=True)
    tipo_documento = Column(String(30), primary_key=True)
    zona_id = Column(String(30))
    cliente_id = Column(String(30))
    fecha_factura = Column(Date)
    fecha_planilla = Column(Date)
    fecha_cierre_planilla = Column(Date)
    factura_prefijo = Column(String(30), primary_key=True, default=" ")
    factura_id = Column(String(30), primary_key=True)
    planilla_id = Column(String(30))
    pedido_id = Column(String(30))
    ordencompra_id = Column(String(30))
    producto_id = Column(String(10), primary_key=True)
    nro_linea = Column(String(10), primary_key=True)
    bodega_id = Column(String(10), primary_key=True)
    tplinea_id = Column(String(1), primary_key=True)
    virtual_id = Column(String(10))
    cantidad = Column(Float, default=0)
    costo_unitario = Column(Float, default=0)
    vlrbruto = Column(Float, default=0)
    por_iva = Column(Float)
    valor_iva = Column(Float)
    descuentos = Column(Float)
    consumo = Column(Float)
    nota_id = Column(Integer)
    motivo_id = Column(String(10))
    motivo_descripcion = Column(String(50))
    afecta_venta = Column(String(10))
    formapago_id = Column(String(10))
    formapago_nombre = Column(String(50))
    ruta_id = Column(String(10))
    ruta_nombre = Column(String(50))
    placa_vehiculo = Column(String(10))
    transportador_id = Column(String(10))
    transportador_nombre = Column(String(50))
    auxiliar1_id = Column(String(10))
    auxiliar1_nombre = Column(String(50))
    auxiliar2_id = Column(String(10))
    auxiliar2_nombre = Column(String(50))


class TmpInfoVentas(Base):
    __tablename__ = "tmp_infoventas"
    Cod_cliente = Column("Cod. cliente", String(30), primary_key=True, nullable=False)
    Cod_vendedor = Column("Cod. vendedor", String(30), primary_key=True, nullable=False)
    Cod_producto = Column(
        "Cod. productto", String(30), primary_key=True, nullable=False
    )
    Fecha = Column("Fecha", Date)
    Fac_numero = Column("Fac. numero", String(30), primary_key=True, nullable=False)
    Cantidad = Column("Cantidad", Integer)
    Vta_neta = Column("Vta neta", Float)
    Tipo = Column("Tipo", String(30), primary_key=True, nullable=False)
    Costo = Column("Costo", Float)
    Unidad = Column("Unidad", String(30))
    Pedido = Column("Pedido", String(30))
    Codigo_bodega = Column("Codigo bodega", String(30))
    nbLinea = Column(
        "nbLinea", String(5), primary_key=True, nullable=False, default="1"
    )
    procesado = Column("procesado", Integer, default=0)


class CargueInfoVentas:
    def __init__(
        self,
        IdtReporteIni,
        IdtReporteFin,
        database_name,
        user_id=None,
        batch_size=50000,
    ):
        print(
            f"Inicializando CargueInfoVentas para las fechas {IdtReporteIni} - {IdtReporteFin}"
        )
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.database_name = database_name
        self.user_id = user_id
        self.batch_size = batch_size
        self.configurar()
        self.empresa = self.obtener_id_empresa()
        # Estadísticas para seguimiento del proceso
        self.stats = {
            "registros_procesados": 0,
            "registros_insertados": 0,
            "registros_descartados": 0,
            "tiempo_inicio": time.time(),
        }

    def configurar(self):
        print("Configurando CargueInfoVentas")
        try:
            config_basic = ConfigBasic(self.database_name, self.user_id)
            self.config = config_basic.config
            print("Configuración del servidor:")
            print(self.config)
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
        except Exception as e:
            logging.error(f"Error al inicializar Interface: {e}")
            raise

    def obtener_id_empresa(self):
        print("Obteniendo ID de la empresa")
        try:
            with self.engine_mysql_bi.connect() as connection:
                result = connection.execute(text("SELECT id FROM dim_empresa LIMIT 1;"))
                empresa_id = result.scalar()
                logging.debug(f"ID de la empresa obtenida: {empresa_id}")
                return empresa_id
        except OperationalError as e:
            logging.error(f"Error al obtener el ID de la empresa: {e}")
            raise

    def preparar_datos(self, empresa, IdtReporteIni, IdtReporteFin):
        print(
            f"Preparando datos para la empresa {empresa} en las fechas {IdtReporteIni} - {IdtReporteFin}"
        )
        parametros = {
            "empresa": empresa,
            "IdtReporteIni": IdtReporteIni,
            "IdtReporteFin": IdtReporteFin,
        }
        logging.debug(f"Parámetros de la consulta: {parametros}")

        # Optimizamos la consulta SQL para solo procesar registros no procesados
        sqltemporal = text(
            """
            SELECT 
                :empresa AS empresa, 
                t.`Cod. cliente` AS cliente_id, 
                t.`Cod. vendedor` AS zona_id, 
                t.Fecha AS fecha_factura, 
                t.`Fac. numero` AS factura_id, 
                t.`Cod. productto` AS producto_id, 
                t.`nbLinea` AS nro_linea, 
                t.`Codigo bodega` AS bodega_id, 
                t.`Tipo` AS tplinea_id, 
                NULL AS virtual_id,  
                t.`Cantidad` AS cantidad, 
                t.`Costo` AS costo_unitario, 
                t.`Vta neta` AS vlrbruto,
                NULL AS fecha_planilla,
                NULL AS fecha_cierre_planilla,
                t.`Pedido` AS pedido_id,
                NULL AS ordencompra_id,
                NULL AS por_iva,
                NULL AS valor_iva,
                NULL AS descuentos,
                NULL AS consumo,
                NULL AS nota_id,
                NULL AS motivo_id,
                NULL AS motivo_descripcion,
                NULL AS afecta_venta,
                NULL AS formapago_id,
                NULL AS formapago_nombre,
                NULL AS ruta_id,
                NULL AS ruta_nombre,
                NULL AS placa_vehiculo,
                NULL AS transportador_id,
                NULL AS transportador_nombre,
                NULL AS auxiliar1_id,
                NULL AS auxiliar1_nombre,
                NULL AS auxiliar2_id,
                NULL AS auxiliar2_nombre
            FROM tmp_infoventas t 
            WHERE t.Fecha BETWEEN :IdtReporteIni AND :IdtReporteFin
            AND t.procesado = 0;
        """
        )

        try:
            # Usamos el método optimizado para procesar grandes volúmenes de datos
            df_chunks = []
            rows_count = 0

            # Conexión optimizada con timeout más alto
            with self.engine_mysql_bi.connect().execution_options(
                stream_results=True, isolation_level="READ COMMITTED"
            ) as connection:
                # Procesamos la consulta por chunks para reducir consumo de memoria
                for chunk in pd.read_sql(
                    sqltemporal,
                    connection,
                    params=parametros,
                    chunksize=self.batch_size,
                ):
                    # Optimizamos tipos de datos para reducir memoria
                    chunk = self._optimize_dataframe(chunk)
                    rows_count += len(chunk)
                    df_chunks.append(chunk)
                    # Reportamos progreso
                    if rows_count % (self.batch_size * 2) == 0:
                        print(f"Procesados {rows_count} registros hasta ahora")
                        logging.info(f"Procesados {rows_count} registros hasta ahora")

            # Si hay múltiples chunks, los concatenamos eficientemente
            if len(df_chunks) > 1:
                df_temporal = pd.concat(df_chunks, ignore_index=True)
            elif len(df_chunks) == 1:
                df_temporal = df_chunks[0]
            else:
                # Si no hay datos, retornamos DataFrame vacío
                df_temporal = pd.DataFrame()

            print(f"Datos temporales obtenidos: {df_temporal.shape[0]} registros")
            self.stats["registros_procesados"] += df_temporal.shape[0]
            logging.debug(
                f"Datos temporales obtenidos para {IdtReporteIni} - {IdtReporteFin}: {df_temporal.shape[0]} registros."
            )
            return df_temporal

        except Exception as e:
            logging.error(f"Error al ejecutar la consulta SQL: {e}")
            print(f"Error al ejecutar la consulta SQL: {e}")
            raise

    def _optimize_dataframe(self, df):
        """Optimiza los tipos de datos en el DataFrame para reducir uso de memoria"""
        for col in df.columns:
            # Convertir columnas de texto a categorías si son apropiadas
            if df[col].dtype == "object":
                null_mask = df[col].isnull()
                if null_mask.sum() > 0:
                    continue  # Saltar columnas con valores NULL para conversión de categoría

                if (
                    df[col].nunique() < len(df) * 0.5
                ):  # Si hay menos de 50% de valores únicos
                    df[col] = df[col].astype("category")

            # Convertir enteros a tipos más pequeños si es posible
            elif pd.api.types.is_integer_dtype(df[col]):
                c_min, c_max = df[col].min(), df[col].max()

                if c_min >= 0:
                    if c_max < 255:
                        df[col] = df[col].astype("uint8")
                    elif c_max < 65535:
                        df[col] = df[col].astype("uint16")
                    elif c_max < 4294967295:
                        df[col] = df[col].astype("uint32")
                else:
                    if c_min > -128 and c_max < 127:
                        df[col] = df[col].astype("int8")
                    elif c_min > -32768 and c_max < 32767:
                        df[col] = df[col].astype("int16")
                    elif c_min > -2147483648 and c_max < 2147483647:
                        df[col] = df[col].astype("int32")

            # Optimizar flotantes
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype("float32")

        return df

    def insertar_registros_ignore(self, modelo, data_to_insert):
        print(f"Insertando registros en {modelo.__tablename__}")
        inicio = time.time()

        # Si no hay datos, retornamos inmediatamente
        if not data_to_insert or (
            isinstance(data_to_insert, list) and len(data_to_insert) == 0
        ):
            print(f"No hay datos para insertar en {modelo.__tablename__}")
            return

        try:
            with self.engine_mysql_bi.connect() as connection:
                # Iniciar transacción para múltiples operaciones
                trans = connection.begin()
                try:
                    nuevos_registros = self.filtrar_nuevos_registros(
                        modelo, data_to_insert, connection
                    )

                    if nuevos_registros:
                        print(
                            f"Insertando {len(nuevos_registros)} nuevos registros en {modelo.__tablename__}"
                        )
                        self.stats["registros_insertados"] += len(nuevos_registros)

                        # Insertamos por lotes optimizados
                        for chunk in self.chunk_data(
                            nuevos_registros, batch_size=self.batch_size
                        ):
                            # Preparamos los valores para inserción en masa
                            columnas = list(chunk[0].keys())
                            valores = []

                            for registro in chunk:
                                row_values = [
                                    registro.get(col, None) for col in columnas
                                ]
                                valores.append(row_values)

                            # Construir consulta para inserción masiva
                            placeholders = ", ".join(["%s"] * len(columnas))
                            columnas_str = ", ".join([f"`{col}`" for col in columnas])

                            insert_query = f"""
                                INSERT INTO {modelo.__tablename__} ({columnas_str}) 
                                VALUES ({placeholders})
                                ON DUPLICATE KEY UPDATE {columnas[0]} = VALUES({columnas[0]})
                            """

                            # Ejecutar la inserción masiva
                            connection.execute(text(insert_query), valores)

                            # Reportamos progreso cada cierto número de registros
                            if len(chunk) >= 5000:
                                print(
                                    f"Insertados {len(chunk)} registros en {modelo.__tablename__}"
                                )

                        logging.info(
                            f"{len(nuevos_registros)} registros insertados en {modelo.__tablename__}."
                        )
                    else:
                        self.stats["registros_descartados"] += (
                            len(data_to_insert)
                            if hasattr(data_to_insert, "__len__")
                            else 0
                        )
                        logging.info(
                            f"No hay nuevos registros para insertar en {modelo.__tablename__}."
                        )
                        print(
                            f"No hay nuevos registros para insertar en {modelo.__tablename__}."
                        )

                    # Confirmar transacción
                    trans.commit()
                except Exception as e:
                    # Revertir transacción en caso de error
                    trans.rollback()
                    raise e

        except IntegrityError as e:
            logging.error(
                f"Error de integridad al insertar registros en {modelo.__tablename__}: {e}"
            )
            print(
                f"Error de integridad al insertar registros en {modelo.__tablename__}: {e}"
            )
        except SQLAlchemyError as e:
            logging.error(
                f"Error de SQLAlchemy al insertar registros en {modelo.__tablename__}: {e}"
            )
            print(
                f"Error de SQLAlchemy al insertar registros en {modelo.__tablename__}: {e}"
            )
        finally:
            fin = time.time()
            tiempo_transcurrido = fin - inicio
            logging.info(
                f"Tiempo transcurrido en insertar_registros_ignore: {tiempo_transcurrido:.2f} segundos."
            )
            print(
                f"Tiempo transcurrido en insertar_registros_ignore: {tiempo_transcurrido:.2f} segundos."
            )

    def filtrar_nuevos_registros(self, modelo, data_to_insert, connection):
        print(f"Filtrando nuevos registros para {modelo.__tablename__}")
        inicio = time.time()
        claves_unicas = self.obtener_claves_unicas(modelo)
        todos_nuevos_registros = []

        for batch in self.chunk_data(data_to_insert, batch_size=50000):
            batch = [
                record
                for record in batch
                if all(key in record for key in claves_unicas)
            ]
            if not batch:
                continue

            keys_to_check = [
                self.construct_key(record, claves_unicas) for record in batch
            ]

            existing_records_query = connection.execute(
                text(
                    f"SELECT * FROM {modelo.__tablename__} WHERE ({', '.join(claves_unicas)}) IN ({', '.join(['%s'] * len(claves_unicas))})"
                ),
                keys_to_check,
            ).fetchall()

            existing_records_keys = {
                self.construct_key(record, claves_unicas)
                for record in existing_records_query
            }

            nuevos_registros = [
                record
                for record in batch
                if self.construct_key(record, claves_unicas)
                not in existing_records_keys
            ]

            todos_nuevos_registros.extend(nuevos_registros)
        final = time.time()
        tiempo_transcurrido = final - inicio
        logging.info(
            f"Tiempo transcurrido en filtrar nuevos registros: {tiempo_transcurrido} segundos."
        )
        print(
            f"Tiempo transcurrido en filtrar nuevos registros: {tiempo_transcurrido} segundos."
        )
        return todos_nuevos_registros

    def obtener_claves_unicas(self, modelo):
        return [
            columna.name
            for columna in modelo.__table__.columns
            if columna.primary_key or columna.unique
        ]

    def construct_key(self, record, key_columns):
        if not record:
            raise ValueError("El registro no puede estar vacío.")
        if not key_columns:
            raise ValueError("Debe proporcionar al menos una columna clave.")

        if isinstance(record, dict):
            return tuple(record.get(key) for key in key_columns)
        else:
            return tuple(getattr(record, key, None) for key in key_columns)

    def chunk_data(self, data, batch_size):
        for i in range(0, len(data), batch_size):
            yield data[i : i + batch_size]

    def marcar_registros_como_procesados(self):
        print("Marcando registros como procesados")
        inicio = time.time()
        try:
            with self.engine_mysql_bi.connect() as connection:
                facturas_items_ids = {
                    tuple(row)
                    for row in connection.execute(
                        text(
                            """
                        SELECT 
                            factura_id, 
                            producto_id, 
                            tplinea_id, 
                            nro_linea 
                        FROM fact_ventas_items
                    """
                        )
                    ).fetchall()
                }
                combined_ids = facturas_items_ids

                if combined_ids:
                    connection.execute(
                        text(
                            f"""
                            UPDATE tmp_infoventas
                            SET procesado = 1
                            WHERE ({', '.join(['Fac_numero', 'Cod_producto', 'Tipo', 'nbLinea'])}) IN ({', '.join(['%s'] * 4)})
                        """
                        ),
                        combined_ids,
                    )
            logging.info("Registros marcados como procesados.")
        except Exception as e:
            logging.error(f"Error al marcar registros como procesados: {e}")
            print(f"Error al marcar registros como procesados: {e}")
        finally:
            final = time.time()
            tiempo_transcurrido = final - inicio
            logging.info(
                f"Tiempo transcurrido en marcar registros procesados: {tiempo_transcurrido} segundos."
            )
            print(
                f"Tiempo transcurrido en marcar registros como procesados: {tiempo_transcurrido} segundos."
            )

    def eliminar_registros_procesados(self):
        print("Eliminando registros procesados")
        inicio = time.time()
        try:
            with self.engine_mysql_bi.connect() as connection:
                connection.execute(
                    text(
                        """
                        DELETE FROM tmp_infoventas
                        WHERE procesado = 1
                    """
                    )
                )
            logging.info(
                "Registros procesados eliminados exitosamente de 'tmp_infoventas'."
            )
            print("Registros procesados eliminados exitosamente de 'tmp_infoventas'.")
        except Exception as e:
            logging.error(f"Error al eliminar registros procesados: {e}")
            print(f"Error al eliminar registros procesados: {e}")
        finally:
            final = time.time()
            tiempo_transcurrido = final - inicio
            logging.info(
                f"Tiempo transcurrido en eliminar registros procesados: {tiempo_transcurrido} segundos."
            )
            print(
                f"Tiempo transcurrido en eliminar registros procesados: {tiempo_transcurrido} segundos."
            )

    def procesar_cargue_ventas(self):
        print(
            f"Procesando cargue de ventas para las fechas {self.IdtReporteIni} - {self.IdtReporteFin}"
        )
        inicio = time.time()
        empresa = self.obtener_id_empresa()

        # Calculamos el rango de fechas y su total para mostrar progreso
        fechas = pd.date_range(start=self.IdtReporteIni, end=self.IdtReporteFin)
        total_dias = len(fechas)
        dias_procesados = 0

        print(f"Total de días a procesar: {total_dias}")

        # En lugar de procesar día por día, procesamos grupos de días
        # para reducir el número de operaciones de I/O y mejorar el rendimiento
        for fecha_grupo in [fechas[i : i + 5] for i in range(0, total_dias, 5)]:
            if not fecha_grupo:
                continue

            fecha_inicio = fecha_grupo[0].strftime("%Y-%m-%d")
            fecha_fin = fecha_grupo[-1].strftime("%Y-%m-%d")

            print(
                f"Procesando grupo de fechas: {fecha_inicio} a {fecha_fin} ({len(fecha_grupo)} días)"
            )
            logging.info(
                f"Procesando grupo de fechas: {fecha_inicio} a {fecha_fin} ({len(fecha_grupo)} días)"
            )

            # Procesamos un grupo de días a la vez en lugar de día por día
            df_temporal = self.preparar_datos(empresa, fecha_inicio, fecha_fin)

            if df_temporal.empty:
                logging.info(
                    f"No hay datos para procesar en las fechas {fecha_inicio} a {fecha_fin}."
                )
                print(
                    f"No hay datos para procesar en las fechas {fecha_inicio} a {fecha_fin}."
                )
                dias_procesados += len(fecha_grupo)
                continue

            # Insertamos registros de manera eficiente
            registros = df_temporal.to_dict("records")
            self.insertar_registros_ignore(FactVentasItems, registros)

            # Liberamos memoria del DataFrame
            del df_temporal

            # Marcamos como procesados y eliminamos
            self.marcar_registros_como_procesados()
            self.eliminar_registros_procesados()

            # Actualizamos progreso
            dias_procesados += len(fecha_grupo)
            progreso = (dias_procesados / total_dias) * 100
            print(f"Progreso: {progreso:.2f}% ({dias_procesados}/{total_dias} días)")

            # Forzamos garbage collection para liberar memoria
            import gc

            gc.collect()

        # Mostramos estadísticas finales
        final = time.time()
        tiempo_transcurrido = final - inicio
        tiempo_por_dia = tiempo_transcurrido / total_dias if total_dias > 0 else 0

        # Resumen del procesamiento
        print(f"\n===== RESUMEN DE PROCESAMIENTO =====")
        print(
            f"Período procesado: {self.IdtReporteIni} a {self.IdtReporteFin} ({total_dias} días)"
        )
        print(f"Registros procesados: {self.stats['registros_procesados']}")
        print(f"Registros insertados: {self.stats['registros_insertados']}")
        print(f"Registros descartados: {self.stats['registros_descartados']}")
        print(
            f"Tiempo total: {tiempo_transcurrido:.2f} segundos ({tiempo_transcurrido/60:.2f} minutos)"
        )
        print(f"Tiempo promedio por día: {tiempo_por_dia:.2f} segundos")
        print(
            f"Velocidad: {self.stats['registros_procesados']/tiempo_transcurrido if tiempo_transcurrido > 0 else 0:.2f} registros/segundo"
        )
        print(f"=====================================\n")

        logging.info(
            f"Tiempo transcurrido en proceso cargue de ventas: {tiempo_transcurrido:.2f} segundos ({tiempo_transcurrido/60:.2f} minutos)."
        )
        logging.info(
            f"Procesados {self.stats['registros_procesados']} registros, insertados {self.stats['registros_insertados']}, descartados {self.stats['registros_descartados']}."
        )

        if self.stats["registros_procesados"] == 0:
            return {
                "success": False,
                "error_message": "No hay datos para mostrar en el cargue de ventas.",
                "file_path": None,
                "file_name": None,
                "registros_procesados": 0,
                "registros_insertados": 0,
                "tiempo_transcurrido": tiempo_transcurrido,
            }

        return {
            "success": True,
            "registros_procesados": self.stats["registros_procesados"],
            "registros_insertados": self.stats["registros_insertados"],
            "tiempo_transcurrido": tiempo_transcurrido,
        }
