from datetime import datetime
import time
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con
import json
from django.core.exceptions import ImproperlyConfigured
import os
import uuid
import unicodedata
import re
import numbers

# Configuración del logging
logging.basicConfig(
    filename="cargue_maestras.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.info("Iniciando Proceso Cargue Tablas Maestras")


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


class CargueTablasMaestras:
    """
    Clase para cargar tablas maestras (dimensiones) desde archivos Excel
    Reemplaza completamente los datos (truncate + insert)
    """
    
    def __init__(self, database_name):
        self.database_name = database_name
        self.config = ConfigBasic(database_name).config
        self.engine_mysql_bi = self.create_engine_mysql_bi()
        self.advertencias_tablas = {}
        
        # Configurar SQLite temporal para procesamiento
        sqlite_table_name = f"maestras_{uuid.uuid4().hex[:8]}"
        sqlite_path = os.path.join("media", f"temp_{sqlite_table_name}.db")
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        
        self.engine_sqlite = create_engine(
            f"sqlite:///{sqlite_path}",
            connect_args={
                "timeout": 60,
                "check_same_thread": False,
                "isolation_level": None,
            },
            pool_recycle=1800,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
        )
        self._optimize_sqlite()
        
        # Configuración de archivos Excel y mapeo de tablas
        self.archivos_config = {
            'PROVEE-TSOL.xlsx': {
                'clientes': {
                    'hoja': 'CLIENTES',
                    'tabla': 'dim_clientes',
                    'pk': 'cod_cliente',
                    'mapeo_columnas': {
                        'Empresas': 'empresas',
                        'Cod. Cliente': 'cod_cliente',
                        'Nom. Cliente': 'nom_cliente',
                        'Fecha Ingreso': 'fecha_ingreso',
                        'Nit': 'nit',
                        'Direccion': 'direccion',
                        'Telefono': 'telefono',
                        'Representante Legal': 'representante_legal',
                        'Codigo Municipio': 'codigo_municipio',
                        'Codigo Negocio': 'codigo_negocio',
                        'Tipo Negocio': 'tipo_negocio',
                        'Estracto': 'estracto',
                        'Barrio': 'barrio',
                        'Clasificación': 'clasificacion',
                        'Comuna': 'comuna',
                        'Estrato': 'estrato',
                        'Coord': 'coord',
                        'Longitud': 'longitud',
                        'Latitud': 'latitud'
                    }
                },
                'productos': {
                    'hoja': 'PRODUCTO',
                    'tabla': 'dim_productos',
                    'pk': 'codigo_sap',
                    'mapeo_columnas': {
                        'Codigo SAP': 'codigo_sap',
                        'Nombre': 'nombre',
                        'Tipo referencia': 'tipo_referencia',
                        'Unidad': 'unidad',
                        'Codigo de barras': 'codigo_barras',
                        'Proveedor': 'proveedor',
                        'PROVEE 2': 'proveedor_2',  # Tía Mati - marca propia
                        'PROVEE2': 'proveedor_2',
                        'PROVEE_2': 'proveedor_2',
                        'Categoría': 'categoria',
                        'Tipo Prod': 'tipo_prod',
                        'Contenido': 'contenido'
                    }
                },
                'proveedores': {
                    'hoja': 'Proveedores',
                    'tabla': 'dim_proveedores',
                    'pk': 'codigo_proveedor',
                    'mapeo_columnas': {
                        'GENERA REPORTE PARA PROVEEDORES': 'codigo_proveedor',
                        '1': 'emails',
                        'COPIA': 'copia',
                        'COLUMNAS': 'columnas',
                        'PERÍODO': 'periodo',
                        'PPKS': 'ppks'
                    }
                },
                'estructura': {
                    'hoja': 'ESTRUCTURA',
                    'tabla': 'dim_estructura',
                    'pk': 'cod_ejecutivo',
                    'mapeo_columnas': {
                        'Cod Ejecutivo': 'cod_ejecutivo',
                        'Nom Ejecutivo': 'nom_ejecutivo',
                        'Líder TSOL': 'lider_tsol',
                        'Empresa': 'empresa',
                        'Lider Comercial': 'lider_comercial',
                        'Cod. Bod': 'cod_bod',
                        'Bodega': 'bodega',
                        'areanombre': 'area_nombre'
                    }
                },
                'cuotas_vendedores': {
                    'hoja': 'ESTRUCTURA',
                    'tabla': 'fact_cuotas_vendedores',
                    'tipo_procesamiento': 'cuotas_dinamicas'  # Procesamiento especial
                },
                'asi_vamos': {
                    'hoja': 'Envío Así Vamos',
                    'tabla': 'dim_asi_vamos',
                    'pk': 'asesor',
                    'mapeo_columnas': {
                        'Asesor': 'asesor',
                        'Para': 'para',
                        'Ccopia': 'copia',
                        '30': 'campo_30'
                    }
                }
            },
            '023-COLGATE PALMOLIVE.xlsx': {
                'productos_colgate': {
                    'hoja': 'Productos EQ',
                    'tabla': 'dim_productos_colgate',
                    'pk': 'cod_texto',
                    'mapeo_columnas': {
                        'Cod Texto': 'cod_texto',
                        'Pro_Cod': 'pro_cod',
                        'Producto': 'producto',
                        'ALTERNO': 'alterno',
                        'Cod': 'cod',
                        'Portafolio': 'portafolio',
                        'inici': 'inici',
                        'Subc': 'subc',
                        'Producto.1': 'producto_alt'
                    }
                }
            },
            'rutero_distrijass_total.xlsx': {
                'rutero': {
                    'hoja': 'Sheet1',
                    'tabla': 'dim_rutero',
                    'pk': ['cod_asesor', 'codigo', 'dias'],
                    'mapeo_columnas': {
                        'Cod. Asesor': 'cod_asesor',
                        'Asesores': 'asesores',
                        'Codigo': 'codigo',
                        'Documento': 'documento',
                        'Cliente': 'cliente',
                        'Razon s.': 'razon_social',
                        'Direccion': 'direccion',
                        'Barrio': 'barrio',
                        'Ciudad': 'ciudad',
                        'Telefono': 'telefono',
                        'Sucur.': 'sucursal',
                        'Dias': 'dias',
                        'Orden': 'orden',
                        'Orden Dia': 'orden_dia',
                        'Email.': 'email',
                        'Segmento': 'segmento',
                        'Telefono2': 'telefono2'
                    }
                }
            }
        }

    def _obtener_limites_longitud(self, nombre_tabla):
        """Obtener límites de longitud para columnas de texto desde MySQL"""
        try:
            with self.engine_mysql_bi.connect() as conn:
                query = text(
                    """
                    SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :schema
                      AND TABLE_NAME = :table
                      AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL
                    """
                )
                resultado = conn.execute(
                    query,
                    {
                        "schema": self.config.get("dbBi"),
                        "table": nombre_tabla,
                    },
                )
                return {
                    fila[0]: fila[1]
                    for fila in resultado
                    if fila[1] is not None
                }
        except Exception as e:
            logging.warning(
                f"No se pudieron obtener los límites de longitud para {nombre_tabla}: {e}"
            )
        return {}

    def _aplicar_limites_longitud(self, df, nombre_tabla):
        """Aplicar límites de longitud según la definición en MySQL"""
        if df.empty:
            return df

        limites = self._obtener_limites_longitud(nombre_tabla)
        if not limites:
            return df

        for columna, limite in limites.items():
            if columna not in df.columns or limite is None:
                continue

            serie_original = df[columna]
            if serie_original.dropna().empty:
                continue

            def truncar_valor(valor):
                if valor is None or (isinstance(valor, float) and pd.isna(valor)):
                    return None
                valor_str = str(valor)
                if len(valor_str) > limite:
                    return valor_str[:limite]
                return valor

            valores_largos = serie_original.dropna().apply(lambda v: len(str(v)) > limite)
            total_truncados = int(valores_largos.sum())

            df[columna] = serie_original.apply(truncar_valor)

            if total_truncados:
                logging.warning(
                    f"Columna '{columna}' en {nombre_tabla}: {total_truncados} valores truncados a {limite} caracteres"
                )

        return df

    def _eliminar_duplicados_por_pk(self, df, pk_cols, tabla_mysql, tabla_nombre):
        """Eliminar filas con claves primarias (simples o compuestas) duplicadas o nulas antes de la inserción"""
        if df.empty:
            return df, 0, 0

        if isinstance(pk_cols, (list, tuple, set)):
            pk_col_list = [col for col in pk_cols]
        else:
            pk_col_list = [pk_cols]

        columnas_faltantes = [col for col in pk_col_list if col not in df.columns]
        if columnas_faltantes:
            logging.warning(
                f"{tabla_nombre}: columnas de clave primaria faltantes en DataFrame: {columnas_faltantes}"
            )
            pk_col_list = [col for col in pk_col_list if col not in columnas_faltantes]
            if not pk_col_list:
                return df, 0, 0

        pk_descripcion = "+".join(pk_col_list)

        nulos_mask = df[pk_col_list].isna().any(axis=1)
        total_nulos = int(nulos_mask.sum())
        if total_nulos:
            mensaje_nulos = (
                f"{tabla_nombre}: descartadas {total_nulos} filas sin valores completos en la clave primaria ({pk_descripcion})"
            )
            print(f"⚠️ {mensaje_nulos}")
            logging.warning(f"{mensaje_nulos} (tabla destino: {tabla_mysql})")
            df = df[~nulos_mask]

        duplicados_mask = df.duplicated(subset=pk_col_list, keep='first')
        total_duplicados = int(duplicados_mask.sum())

        if total_duplicados:
            valores_duplicados = (
                df.loc[duplicados_mask, pk_col_list]
                .astype(str)
                .drop_duplicates()
                .head(10)
                .to_dict(orient='records')
            )
            mensaje_dup = (
                f"{tabla_nombre}: descartados {total_duplicados} registros duplicados por clave primaria ({pk_descripcion})"
            )
            print(f"⚠️ {mensaje_dup}")
            if valores_duplicados:
                print(f"   Ejemplos duplicados: {valores_duplicados}")
            logging.warning(
                f"{mensaje_dup} (tabla destino: {tabla_mysql}). Ejemplos: {valores_duplicados}"
            )
            df = df.drop_duplicates(subset=pk_col_list, keep='first')

        return df, total_nulos, total_duplicados

    def create_engine_mysql_bi(self):
        """Crear engine para conexión a MySQL BI"""
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

    def _optimize_sqlite(self):
        """Configurar optimizaciones para SQLite"""
        try:
            with self.engine_sqlite.connect() as conn:
                conn.execute(text("PRAGMA cache_size = -200000"))
                conn.execute(text("PRAGMA journal_mode = WAL"))
                conn.execute(text("PRAGMA synchronous = NORMAL"))
                conn.execute(text("PRAGMA page_size = 8192"))
                conn.execute(text("PRAGMA auto_vacuum = INCREMENTAL"))
                conn.execute(text("PRAGMA mmap_size = 2147483648"))
                conn.execute(text("PRAGMA temp_store = MEMORY"))
                conn.execute(text("PRAGMA locking_mode = EXCLUSIVE"))
            print("Optimizaciones SQLite aplicadas correctamente")
        except Exception as e:
            print(f"Error aplicando optimizaciones SQLite: {e}")
            logging.warning(f"Error aplicando optimizaciones SQLite: {e}")

    @staticmethod
    def _normalizar_nombre_columna(nombre):
        """Normaliza nombres de columnas para coincidencias flexibles"""
        if not isinstance(nombre, str):
            return ""
        nombre_normalizado = unicodedata.normalize("NFKD", nombre)
        nombre_normalizado = nombre_normalizado.encode("ascii", "ignore").decode("ascii")
        nombre_normalizado = nombre_normalizado.strip().lower()
        nombre_normalizado = re.sub(r"[^a-z0-9]+", "_", nombre_normalizado)
        return nombre_normalizado.strip("_")

    @staticmethod
    def _normalizar_codigo(valor):
        """Normaliza códigos eliminando decimales y espacios innecesarios"""
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return None

        if pd.isna(valor):
            return None

        if isinstance(valor, numbers.Number):
            if isinstance(valor, float) and not float(valor).is_integer():
                return str(valor).strip()
            return str(int(valor))

        valor_str = str(valor).strip()
        if not valor_str:
            return None

        if valor_str.endswith(".0"):
            valor_str = valor_str[:-2]

        return valor_str.upper()

    @staticmethod
    def _normalizar_texto_generico(valor):
        """Normaliza textos: elimina espacios extremos, reemplaza cadenas especiales y vacías por None"""
        if valor is None:
            return None

        if isinstance(valor, float) and pd.isna(valor):
            return None

        valor_str = str(valor).strip()

        if not valor_str:
            return None

        if valor_str.lower() in {"nan", "none", "null", "n/a", "#n/a"}:
            return None

        if valor_str == "0":
            return None

        return valor_str

    def cargar_tabla_desde_excel(self, archivo_excel, tabla_config, tabla_nombre):
        """
        Cargar una tabla específica desde un archivo Excel
        """
        try:
            archivo_path = os.path.join("media", archivo_excel)
            
            if not os.path.exists(archivo_path):
                raise FileNotFoundError(f"No se encontró el archivo {archivo_path}")
            
            print(f"📁 Leyendo archivo: {archivo_excel}")
            print(f"📊 Cargando tabla: {tabla_nombre}")
            logging.info(f"Iniciando carga de {tabla_nombre}")
            
            # Verificar si es procesamiento especial de cuotas
            if tabla_config.get('tipo_procesamiento') == 'cuotas_dinamicas':
                return self._cargar_cuotas_dinamicas(archivo_path, tabla_config, tabla_nombre)
            
            # Validar que la hoja existe
            try:
                hojas_disponibles = pd.ExcelFile(archivo_path).sheet_names
                if tabla_config['hoja'] not in hojas_disponibles:
                    raise ValueError(f"Hoja '{tabla_config['hoja']}' no encontrada. Hojas disponibles: {hojas_disponibles}")
            except Exception as e:
                raise ValueError(f"Error accediendo al archivo Excel: {str(e)}")
            
            # Leer Excel con manejo de errores mejorado
            try:
                df = pd.read_excel(
                    archivo_path, 
                    sheet_name=tabla_config['hoja']
                )
                print(f"📋 Leídas {len(df)} filas de la hoja '{tabla_config['hoja']}'")
            except Exception as e:
                raise ValueError(f"Error leyendo hoja '{tabla_config['hoja']}': {str(e)}")
            
            # Validar que hay datos
            if df.empty:
                raise ValueError(f"La hoja '{tabla_config['hoja']}' está vacía")
            
            # Filtrar filas vacías
            filas_originales = len(df)
            df = df.dropna(how='all')
            if len(df) < filas_originales:
                print(f"🧹 Eliminadas {filas_originales - len(df)} filas vacías")
            
            # Validar estructura de columnas con mapeo flexible
            print(f"📊 Columnas disponibles en Excel: {list(df.columns)}")
            mapeo = tabla_config['mapeo_columnas']
            columnas_map = {}
            columnas_faltantes = []
            columnas_norm = {self._normalizar_nombre_columna(col): col for col in df.columns}

            for columna_excel, columna_destino in mapeo.items():
                if columna_excel in df.columns:
                    columnas_map[columna_excel] = columna_destino
                    continue

                columna_normalizada = self._normalizar_nombre_columna(columna_excel)
                columna_en_df = columnas_norm.get(columna_normalizada)

                if columna_en_df:
                    columnas_map[columna_en_df] = columna_destino
                else:
                    columnas_faltantes.append(columna_excel)

            if columnas_faltantes:
                print(f"⚠️  Columnas faltantes (no se encontraron coincidencias): {columnas_faltantes}")

            if not columnas_map:
                raise ValueError(
                    f"No se encontraron columnas válidas. Requeridas: {list(mapeo.keys())}, Disponibles: {list(df.columns)}"
                )

            print(f"✅ Columnas mapeadas: {columnas_map}")

            # Seleccionar y renombrar columnas
            df_clean = df[list(columnas_map.keys())].copy()
            df_clean = df_clean.rename(columns=columnas_map)

            # Agregar columnas faltantes como None para mantener consistencia
            for columna_excel in columnas_faltantes:
                columna_destino = mapeo[columna_excel]
                df_clean[columna_destino] = None
                print(f"ℹ️  Columna '{columna_destino}' agregada con valores nulos por ausencia en el archivo")
            
            print(f"🔄 Aplicando limpieza de datos...")
            # Limpiar datos
            df_clean = self._limpiar_datos(df_clean, tabla_nombre)

            pk_columna = tabla_config.get('pk')
            if pk_columna:
                df_clean, eliminadas_nulas, eliminadas_duplicadas = self._eliminar_duplicados_por_pk(
                    df_clean,
                    pk_columna,
                    tabla_config['tabla'],
                    tabla_nombre
                )
                advertencias_pk = []
                if eliminadas_nulas:
                    advertencias_pk.append(
                        f"{eliminadas_nulas} registros sin clave primaria"
                    )
                if eliminadas_duplicadas:
                    advertencias_pk.append(
                        f"{eliminadas_duplicadas} registros duplicados"
                    )
                if advertencias_pk:
                    mensaje_pk = "; ".join(advertencias_pk)
                    existente = self.advertencias_tablas.get(tabla_nombre)
                    if existente:
                        self.advertencias_tablas[tabla_nombre] = f"{existente}; {mensaje_pk}"
                    else:
                        self.advertencias_tablas[tabla_nombre] = mensaje_pk

            # Ajustar longitudes antes de insertar
            df_clean = self._aplicar_limites_longitud(df_clean, tabla_config['tabla'])
            
            if df_clean.empty:
                print(f"❌ No hay datos válidos para cargar en {tabla_nombre}")
                return 0
            
            print(f"📊 {len(df_clean)} registros válidos para insertar")
            
            # Truncar tabla en MySQL
            print(f"🗑️  Truncando tabla {tabla_config['tabla']}")
            self._truncar_tabla(tabla_config['tabla'])
            
            # Insertar datos en lotes
            print(f"💾 Insertando datos en {tabla_config['tabla']}")
            total_insertado = self._insertar_en_lotes(df_clean, tabla_config['tabla'])
            print(f"✅ {tabla_nombre}: {total_insertado} registros cargados")
            logging.info(f"{tabla_nombre}: {total_insertado} registros cargados exitosamente")

            return total_insertado
            
        except Exception as e:
            error_msg = f"Error cargando {tabla_nombre}: {str(e)}"
            print(f"❌ {error_msg}")
            logging.error(error_msg)
            raise

    def _cargar_cuotas_dinamicas(self, archivo_path, tabla_config, tabla_nombre):
        """
        Cargar cuotas mensuales de forma dinámica desde las columnas CUOTA [MES] [AÑO]
        """
        import re
        from datetime import datetime
        
        print(f"Procesando cuotas dinámicas para {tabla_nombre}")
        
        # Leer Excel completo
        df = pd.read_excel(archivo_path, sheet_name=tabla_config['hoja'])
        df = df.dropna(how='all')
        
        # Identificar columnas de cuotas (patrón: CUOTA [MES] [AÑO])
        patron_cuota = r'CUOTA\s+([A-Z]+)\s+(\d{4})'
        columnas_cuota = []
        
        for col in df.columns:
            if isinstance(col, str):
                match = re.match(patron_cuota, col.strip().upper())
                if match:
                    mes_nombre = match.group(1)
                    anio = int(match.group(2))
                    columnas_cuota.append({
                        'columna': col,
                        'mes_nombre': mes_nombre,
                        'anio': anio,
                        'mes': self._convertir_mes_a_numero(mes_nombre)
                    })
        
        print(f"Encontradas {len(columnas_cuota)} columnas de cuotas: {[c['columna'] for c in columnas_cuota]}")
        
        if not columnas_cuota:
            print("No se encontraron columnas de cuotas válidas")
            return 0
        
        # Verificar que existe la columna de código ejecutivo
        if 'Cod Ejecutivo' not in df.columns:
            raise ValueError("No se encontró la columna 'Cod Ejecutivo' para las cuotas")
        
        # Construir DataFrame de cuotas
        cuotas_data = []
        
        for _, row in df.iterrows():
            cod_ejecutivo = self._normalizar_codigo(row['Cod Ejecutivo'])
            if cod_ejecutivo is None:
                continue
                
            for cuota_info in columnas_cuota:
                cuota_valor = row.get(cuota_info['columna'])
                
                # Solo agregar si hay un valor de cuota
                if pd.notna(cuota_valor) and cuota_valor != 0:
                    cuotas_data.append({
                        'cod_ejecutivo': cod_ejecutivo,
                        'periodo': f"{cuota_info['anio']}{cuota_info['mes']:02d}",
                        'anio': cuota_info['anio'],
                        'mes': cuota_info['mes'],
                        'mes_nombre': cuota_info['mes_nombre'],
                        'cuota': float(cuota_valor)
                    })
        
        if not cuotas_data:
            print("No se encontraron datos de cuotas válidos")
            return 0
        
        df_cuotas = pd.DataFrame(cuotas_data)

        # Validar contra estructura vigente
        codigos_validos = None
        try:
            with self.engine_mysql_bi.connect() as conn:
                resultado = conn.execute(text("SELECT cod_ejecutivo FROM dim_estructura"))
                codigos_validos = {
                    self._normalizar_codigo(fila[0])
                    for fila in resultado
                    if fila[0] is not None
                }
        except Exception as e:
            logging.warning(
                f"No fue posible verificar códigos de cuotas contra dim_estructura: {e}"
            )

        if codigos_validos is not None:
            if not codigos_validos:
                advertencia = (
                    "dim_estructura está vacía; se omiten cuotas hasta cargar la estructura"
                )
                print(f"⚠️ {advertencia}")
                logging.warning(advertencia)
                self.advertencias_tablas[tabla_nombre] = advertencia
                return 0

            antes = len(df_cuotas)
            df_invalidos = df_cuotas[~df_cuotas['cod_ejecutivo'].isin(codigos_validos)]
            df_cuotas = df_cuotas[df_cuotas['cod_ejecutivo'].isin(codigos_validos)]
            descartados = antes - len(df_cuotas)
            if descartados:
                logging.warning(
                    f"Cuotas: descartados {descartados} registros por códigos sin estructura asociada"
                )
                codigos_descartados = sorted(
                    {
                        self._normalizar_codigo(valor)
                        for valor in df_invalidos['cod_ejecutivo'].dropna().unique()
                    }
                )
                if codigos_descartados:
                    logging.warning(
                        f"Códigos descartados (primeros 10): {codigos_descartados[:10]}"
                    )

        # Ajustar longitudes según definición de la tabla destino
        df_cuotas = self._aplicar_limites_longitud(df_cuotas, tabla_config['tabla'])
        
        # Truncar tabla de cuotas
        self._truncar_tabla(tabla_config['tabla'])
        
        # Insertar cuotas
        total_insertado = self._insertar_en_lotes(df_cuotas, tabla_config['tabla'])
        
        print(f"✅ {tabla_nombre}: {total_insertado} registros de cuotas cargados")
        logging.info(f"{tabla_nombre}: {total_insertado} registros de cuotas cargados")
        
        return total_insertado

    def _convertir_mes_a_numero(self, mes_nombre):
        """Convertir nombre de mes en español a número"""
        meses = {
            'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4,
            'MAYO': 5, 'JUNIO': 6, 'JULIO': 7, 'AGOSTO': 8,
            'SEPTIEMBRE': 9, 'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12
        }
        return meses.get(mes_nombre.upper(), 1)

    def _limpiar_datos(self, df, tabla_nombre):
        """Limpiar y validar datos según el tipo de tabla"""
        
        print(f"🧹 Iniciando limpieza de datos para {tabla_nombre}")
        filas_originales = len(df)
        
        # Limpiezas generales
        df = df.replace({pd.NA: None, '': None, 'nan': None})
        
        # Reemplazar valores comunes de "vacío"
        df = df.replace(['N/A', 'n/a', 'NULL', 'null', '-', '#N/A'], None)
        
        print(f"📊 Datos antes de limpieza: {filas_originales} filas")
            
        # Limpiezas específicas por tabla
        if tabla_nombre == 'clientes':
            if 'cod_cliente' in df.columns:
                filas_antes = len(df)
                df['cod_cliente'] = df['cod_cliente'].apply(self._normalizar_codigo)
                df = df[df['cod_cliente'].notna()]
                print(f"🔍 Clientes: Eliminadas {filas_antes - len(df)} filas sin cod_cliente")
            
        elif tabla_nombre == 'productos':
            if 'codigo_sap' in df.columns:
                filas_antes = len(df)
                df['codigo_sap'] = df['codigo_sap'].apply(self._normalizar_codigo)
                df = df[df['codigo_sap'].notna()]
                print(f"🔍 Productos: Eliminadas {filas_antes - len(df)} filas sin codigo_sap")

            # Normalizar proveedores
            if 'proveedor' in df.columns:
                df['proveedor'] = df['proveedor'].apply(self._normalizar_texto_generico)
                muestras = df['proveedor'].dropna().unique()[:5]
                print(f"ℹ️ Productos: normalizados valores en proveedor. Ejemplos: {list(muestras)}")
            
            if 'proveedor_2' in df.columns:
                df['proveedor_2'] = df['proveedor_2'].apply(self._normalizar_texto_generico)
                # Si proveedor_2 está vacío, usar el valor de proveedor
                if 'proveedor' in df.columns:
                    mask_vacio = df['proveedor_2'].isna()
                    df.loc[mask_vacio, 'proveedor_2'] = df.loc[mask_vacio, 'proveedor']
                    print(f"ℹ️ Productos: {mask_vacio.sum()} registros con proveedor_2 vacío completados con proveedor principal")
                muestras = df['proveedor_2'].dropna().unique()[:5]
                print(f"ℹ️ Productos: normalizados valores en proveedor_2. Ejemplos: {list(muestras)}")
            
        elif tabla_nombre == 'estructura':
            if 'cod_ejecutivo' in df.columns:
                filas_antes = len(df)
                df['cod_ejecutivo'] = df['cod_ejecutivo'].apply(self._normalizar_codigo)
                df = df[df['cod_ejecutivo'].notna()]
                print(f"🔍 Estructura: Eliminadas {filas_antes - len(df)} filas sin cod_ejecutivo")

        elif tabla_nombre == 'proveedores':
            if 'codigo_proveedor' in df.columns:
                filas_antes = len(df)
                df['codigo_proveedor'] = df['codigo_proveedor'].apply(self._normalizar_codigo)
                df = df[df['codigo_proveedor'].notna()]
                print(f"🔍 Proveedores: Eliminadas {filas_antes - len(df)} filas sin codigo_proveedor")
            
        elif tabla_nombre == 'rutero':
            # Buscar la columna de código (puede variar)
            columnas_codigo = [col for col in df.columns if 'cod' in col.lower() or 'código' in col.lower()]
            if columnas_codigo:
                col_codigo = columnas_codigo[0]
                filas_antes = len(df)
                df[col_codigo] = df[col_codigo].apply(self._normalizar_codigo)
                df = df[df[col_codigo].notna()]
                print(f"🔍 Rutero: Eliminadas {filas_antes - len(df)} filas sin {col_codigo}")

            # Normalizar columnas numéricas
            for columna_numerica in ['orden', 'orden_dia']:
                if columna_numerica in df.columns:
                    df[columna_numerica] = pd.to_numeric(df[columna_numerica], errors='coerce')
                    df[columna_numerica] = df[columna_numerica].apply(lambda x: int(x) if pd.notna(x) else None)

            if 'cod_asesor' in df.columns:
                df['cod_asesor'] = df['cod_asesor'].apply(self._normalizar_codigo)
            
        elif tabla_nombre == 'productos_colgate':
            # Verificar ambas columnas posibles
            if 'pro_cod' in df.columns:
                filas_antes = len(df)
                df['pro_cod'] = df['pro_cod'].apply(self._normalizar_codigo)
                df = df[df['pro_cod'].notna()]
                print(f"🔍 Productos Colgate: Eliminadas {filas_antes - len(df)} filas sin pro_cod")
            if 'cod_texto' in df.columns:
                filas_antes = len(df)
                df['cod_texto'] = df['cod_texto'].apply(self._normalizar_codigo)
                df = df[df['cod_texto'].notna()]
                print(f"🔍 Productos Colgate: Eliminadas {filas_antes - len(df)} filas sin cod_texto")

        elif tabla_nombre == 'asi_vamos':
            if 'asesor' in df.columns:
                filas_antes = len(df)
                df['asesor'] = df['asesor'].apply(
                    lambda v: str(v).strip() if pd.notna(v) and str(v).strip() else None
                )
                df = df[df['asesor'].notna()]
                print(f"🔍 Así Vamos: Eliminadas {filas_antes - len(df)} filas sin asesor")
        
        # Eliminar filas completamente vacías después de la limpieza
        df = df.dropna(how='all')
        
        print(f"✅ Datos después de limpieza: {len(df)} filas válidas ({filas_originales - len(df)} eliminadas)")
        
        return df

    def _truncar_tabla(self, nombre_tabla):
        """Truncar tabla en MySQL, con fallback a DELETE si existen restricciones FK"""
        try:
            with self.engine_mysql_bi.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {nombre_tabla}"))
            print(f"Tabla {nombre_tabla} truncada")
        except OperationalError as e:
            error_code = None
            if hasattr(e, "orig") and getattr(e.orig, "args", None):
                error_code = e.orig.args[0]

            if error_code == 1701:
                mensaje = (
                    f"No se pudo truncar {nombre_tabla} por restricciones FK. "
                    "Aplicando DELETE para limpiar contenidos."
                )
                print(f"⚠️  {mensaje}")
                logging.warning(mensaje)
                try:
                    with self.engine_mysql_bi.begin() as conn:
                        conn.execute(text(f"DELETE FROM {nombre_tabla}"))
                    print(f"Tabla {nombre_tabla} limpiada mediante DELETE")
                except Exception as delete_error:
                    print(f"Error eliminando registros de {nombre_tabla}: {delete_error}")
                    raise
            else:
                print(f"Error truncando {nombre_tabla}: {e}")
                raise
        except Exception as e:
            print(f"Error truncando {nombre_tabla}: {e}")
            raise

    def _insertar_en_lotes(self, df, nombre_tabla, batch_size=1000):
        """Insertar datos en lotes"""
        total_insertado = 0
        total_filas = len(df)
        
        for i in range(0, total_filas, batch_size):
            batch = df.iloc[i:i + batch_size]
            
            try:
                # Usar streaming para evitar cargas en memoria
                with self.engine_mysql_bi.connect().execution_options(stream_results=True) as conn:
                    batch.to_sql(
                        nombre_tabla,
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                
                total_insertado += len(batch)
                progreso = (i + len(batch)) / total_filas * 100
                print(f"Progreso: {progreso:.1f}% ({total_insertado}/{total_filas})")
                
            except Exception as e:
                print(f"Error insertando lote {i//batch_size + 1}: {e}")
                raise
        
        return total_insertado

    def cargar_todas_las_tablas(self, progress_callback=None):
        """Cargar todas las tablas maestras configuradas"""
        resultados = {}
        total_start_time = time.time()
        
        print("=== INICIANDO CARGA DE TABLAS MAESTRAS ===")
        
        # Contar total de tablas para calcular progreso
        total_tablas = sum(len(tablas) for tablas in self.archivos_config.values())
        tabla_actual = 0
        tablas_completadas = 0
        registros_acumulados = 0
        
        for archivo_excel, tablas in self.archivos_config.items():
            print(f"\nProcesando archivo: {archivo_excel}")
            
            # Validar que el archivo existe
            archivo_path = os.path.join("media", archivo_excel)
            if not os.path.exists(archivo_path):
                print(f"❌ Archivo no encontrado: {archivo_path}")
                for tabla_nombre in tablas.keys():
                    resultados[tabla_nombre] = {
                        'status': 'error',
                        'error': f'Archivo no encontrado: {archivo_excel}',
                        'tiempo': 0
                    }
                    tabla_actual += 1
                continue
            
            for tabla_nombre, tabla_config in tablas.items():
                start_time = time.time()
                tabla_actual += 1
                progreso = int((tabla_actual / total_tablas) * 80) + 10  # 10-90%
                
                try:
                    if progress_callback:
                        progress_callback(progreso, f"Cargando tabla: {tabla_nombre}", {
                            'tabla_actual': tabla_nombre,
                            'completadas': tablas_completadas,
                            'total': total_tablas,
                            'registros': registros_acumulados
                        })
                    
                    print(f"📋 Cargando tabla {tabla_nombre} ({tabla_actual}/{total_tablas})")
                    registros = self.cargar_tabla_desde_excel(archivo_excel, tabla_config, tabla_nombre)
                    tiempo_transcurrido = time.time() - start_time
                    
                    advertencia = self.advertencias_tablas.pop(tabla_nombre, None)
                    estado = 'exitoso'
                    if advertencia:
                        estado = 'advertencia'

                    tablas_completadas += 1
                    registros_acumulados += registros

                    resultados[tabla_nombre] = {
                        'status': estado,
                        'registros': registros,
                        'tiempo': tiempo_transcurrido
                    }

                    if advertencia:
                        resultados[tabla_nombre]['mensaje'] = advertencia
                        print(f"⚠️ {tabla_nombre}: {advertencia} ({tiempo_transcurrido:.2f}s)")
                    else:
                        print(f"✅ {tabla_nombre}: {registros} registros ({tiempo_transcurrido:.2f}s)")

                    if progress_callback:
                        meta_exito = {
                            'tabla_actual': tabla_nombre,
                            'completadas': tablas_completadas,
                            'total': total_tablas,
                            'registros': registros_acumulados,
                            'details': f"{tabla_nombre}: {registros} registros ({tiempo_transcurrido:.2f}s)"
                        }
                        if advertencia:
                            meta_exito['errores'] = {tabla_nombre: advertencia}
                        progreso_completado = min(progreso + 5, 95)
                        progress_callback(
                            progreso_completado,
                            f"{tabla_nombre} completada",
                            meta_exito
                        )
                    
                except Exception as e:
                    tiempo_transcurrido = time.time() - start_time
                    print(f"❌ Error cargando {tabla_nombre}: {str(e)}")
                    resultados[tabla_nombre] = {
                        'status': 'error',
                        'error': str(e),
                        'tiempo': tiempo_transcurrido
                    }
                    if progress_callback:
                        progress_callback(
                            progreso,
                            f"Error en {tabla_nombre}",
                            {
                                'tabla_actual': tabla_nombre,
                                'completadas': tablas_completadas,
                                'total': total_tablas,
                                'registros': registros_acumulados,
                                'errores': {tabla_nombre: str(e)},
                                'details': f"Error en {tabla_nombre}: {str(e)}"
                            }
                        )
        
        total_time = time.time() - total_start_time
        
        # Resumen final
        print(f"\n=== RESUMEN DE CARGA ===")
        print(f"Tiempo total: {total_time:.2f} segundos")

        exitosos = 0
        errores = 0
        advertencias = 0
        total_registros = 0

        for tabla, resultado in resultados.items():
            if resultado['status'] == 'exitoso':
                status_icon = "✅"
                print(f"{status_icon} {tabla}: {resultado['registros']} registros ({resultado['tiempo']:.2f}s)")
                exitosos += 1
                total_registros += resultado['registros']
            elif resultado['status'] == 'advertencia':
                status_icon = "⚠️"
                mensaje = resultado.get('mensaje', 'Advertencia durante el cargue')
                print(f"{status_icon} {tabla}: {mensaje} ({resultado['tiempo']:.2f}s)")
                advertencias += 1
                total_registros += resultado['registros']
            else:
                status_icon = "❌"
                print(f"{status_icon} {tabla}: ERROR - {resultado['error']} ({resultado['tiempo']:.2f}s)")
                errores += 1

        print(f"\nResultado: {exitosos} exitosos, {advertencias} con advertencias, {errores} errores")
        print(f"Total de registros cargados: {total_registros}")

        return resultados

    def cargar_tabla_individual(self, nombre_tabla):
        """Cargar una tabla específica"""
        for archivo_excel, tablas in self.archivos_config.items():
            if nombre_tabla in tablas:
                tabla_config = tablas[nombre_tabla]
                return self.cargar_tabla_desde_excel(archivo_excel, tabla_config, nombre_tabla)
        
        raise ValueError(f"Tabla {nombre_tabla} no encontrada en la configuración")


# Funciones de utilidad para usar desde las tareas RQ
def cargar_tablas_maestras(database_name):
    """Función principal para cargar todas las tablas maestras"""
    try:
        cargador = CargueTablasMaestras(database_name)
        return cargador.cargar_todas_las_tablas()
    except Exception as e:
        logging.error(f"Error en carga masiva: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'detalles': getattr(e, 'args', [''])[0]
        }


def cargar_tabla_individual(database_name, nombre_tabla):
    """Función para cargar una tabla específica"""
    try:
        cargador = CargueTablasMaestras(database_name)
        registros = cargador.cargar_tabla_individual(nombre_tabla)
        return {nombre_tabla: {'status': 'exitoso', 'registros': registros}}
    except Exception as e:
        logging.error(f"Error cargando {nombre_tabla}: {str(e)}")
        raise


if __name__ == "__main__":
    # Para pruebas directas
    database_name = "test_db"  # Cambiar por el nombre real
    resultado = cargar_tablas_maestras(database_name)
    print(f"Resultado final: {resultado}")