import os
import pandas as pd
import logging
import time
from sqlalchemy import create_engine, text
from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con

# Configuración del logging
logging.basicConfig(
    filename="logCargueInfoVentas.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)


class DataBaseConnection:
    def __init__(self, config):
        self.config = config
        print(f"[DataBaseConnection] Iniciando configuración de conexión MySQL...")
        print(f"[DataBaseConnection] Config completo recibido: {self.config}")
        self.engine_mysql_bi = self.create_engine_mysql_bi()

    def create_engine_mysql_bi(self):
        print(
            f"[DataBaseConnection] create_engine_mysql_bi - Iniciando creación del engine..."
        )

        # Extraer parámetros de configuración
        user = self.config.get("nmUsrIn")
        password = self.config.get("txPassIn")
        host = self.config.get("hostServerIn")
        port = self.config.get("portServerIn")
        database = self.config.get("dbBi")

        # Imprimir cada parámetro para diagnóstico
        print(f"[DataBaseConnection] Parámetros de conexión MySQL:")
        print(f"  - Usuario (nmUsrIn): {user}")
        print(f"  - Password (txPassIn): {'***' if password else 'None'}")
        print(f"  - Host (hostServerIn): {host}")
        print(f"  - Puerto (portServerIn): {port} (tipo: {type(port)})")
        print(f"  - Base de datos (dbBi): {database}")

        # Validar parámetros
        if not all([user, password, host, port, database]):
            missing_params = []
            if not user:
                missing_params.append("nmUsrIn")
            if not password:
                missing_params.append("txPassIn")
            if not host:
                missing_params.append("hostServerIn")
            if not port:
                missing_params.append("portServerIn")
            if not database:
                missing_params.append("dbBi")
            error_msg = f"Faltan parámetros de configuración para la conexión MySQL: {missing_params}"
            print(f"[DataBaseConnection][ERROR] {error_msg}")
            raise ValueError(error_msg)

        print(
            f"[DataBaseConnection] Intentando crear conexión a: {user}@{host}:{port}/{database}"
        )

        try:
            engine = con.ConexionMariadb3(
                str(user), str(password), str(host), int(port), str(database)
            )
            print(f"[DataBaseConnection] Engine MySQL creado exitosamente: {engine}")
            return engine
        except Exception as e:
            print(f"[DataBaseConnection][ERROR] Error al crear engine MySQL: {e}")
            print(f"[DataBaseConnection][ERROR] Tipo de error: {type(e).__name__}")
            raise


class CargueInfoVentasInsert:
    def __init__(
        self, excel_file, database_name, IdtReporteIni, IdtReporteFin, user_id=None
    ):
        # Validación: solo aceptar rutas de archivo (str), nunca objetos archivo abiertos
        if not isinstance(excel_file, str):
            raise ValueError(
                "El parámetro excel_file debe ser una ruta de archivo (str), no un archivo abierto."
            )
        self.excel_file = excel_file
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.user_id = user_id
        self.configurar()

    def configurar(self):
        print(
            f"[CargueInfoVentasInsert] configurar: database_name={self.database_name}, user_id={self.user_id}"
        )
        try:
            config_basic = ConfigBasic(self.database_name, self.user_id)
            self.config = config_basic.config
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            print(f"[CargueInfoVentasInsert] Configuración cargada: {self.config}")
        except Exception as e:
            logging.error(f"Error al inicializar configuración: {e}")
            print(
                f"[CargueInfoVentasInsert][ERROR] Error al inicializar configuración: {e}"
            )
            raise

    def corregir_caracteres(self, df):
        """
        Aplica correcciones de caracteres específicos según el mapeo definido.

        Mapeo de caracteres:
        ┴ -> A
        ╔ -> E
        ═ -> I
        Ë -> O
        ┌ -> U
        Ð -> Ñ
        " -> (eliminar)
        """
        print("[corregir_caracteres] Iniciando corrección de caracteres...")

        # Diccionario de mapeo de caracteres
        mapeo_caracteres = {
            "┴": "A",
            "╔": "E",
            "═": "I",
            "Ë": "O",
            "┌": "U",
            "Ð": "Ñ",
            '"': "",  # Eliminar comillas dobles
        }

        # Contador de correcciones
        total_correcciones = 0

        # Aplicar correcciones a todas las columnas de tipo string/object
        for col in df.columns:
            if df[col].dtype == "object":
                # Convertir a string para asegurar el procesamiento
                df[col] = df[col].astype(str)

                # Aplicar cada corrección del mapeo
                for caracter_original, caracter_reemplazo in mapeo_caracteres.items():
                    # Contar cuántas correcciones se van a hacer en esta columna
                    correcciones_col = (
                        df[col].str.contains(caracter_original, regex=False).sum()
                    )
                    if correcciones_col > 0:
                        print(
                            f"[corregir_caracteres] Columna '{col}': {correcciones_col} correcciones de '{caracter_original}' -> '{caracter_reemplazo}'"
                        )
                        total_correcciones += correcciones_col

                    # Aplicar el reemplazo
                    df[col] = df[col].str.replace(
                        caracter_original, caracter_reemplazo, regex=False
                    )

        print(
            f"[corregir_caracteres] Total de correcciones aplicadas: {total_correcciones}"
        )
        return df

    def limpiar_columnas(self, df):
        # Limpieza de nombres de columnas y tipos
        df.columns = [str(col).strip() for col in df.columns]
        return df

    def limpiar_datos(self, df):
        # Limpieza de datos: reemplazo de NaN, caracteres problemáticos, etc.
        date_columns = {"Fecha"}

        for col in df.columns:
            if col in date_columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    df[col] = df[col].dt.date
                except Exception as e:
                    logging.warning(
                        f"[limpiar_datos] No se pudo convertir la columna '{col}' a fecha: {e}"
                    )
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                continue

            if df[col].dtype == object:
                df[col] = (
                    df[col]
                    .fillna("")
                    .astype(str)
                    .str.replace("'", " ")
                    .str.replace('"', " ")
                )
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df

    def cargar_datos_excel(self):
        print(f"[CargueInfoVentasInsert] cargar_datos_excel: archivo={self.excel_file}")
        try:
            df = pd.read_excel(self.excel_file, sheet_name="infoventas")
            print(f"[CargueInfoVentasInsert] Excel leído. Registros: {len(df)}")

            # Aplicar corrección de caracteres problemáticos antes de cualquier otra transformación
            df = self.corregir_caracteres(df)

            df = self.limpiar_columnas(df)

            df = self.limpiar_datos(df)
            # Filtrar por fechas si las columnas y los valores existen
            if self.IdtReporteIni and self.IdtReporteFin and "Fecha" in df.columns:
                # Convertir columna Fecha y parámetros a datetime
                df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
                fecha_ini = pd.to_datetime(self.IdtReporteIni).date()
                fecha_fin = pd.to_datetime(self.IdtReporteFin).date()
                # Filtrar solo si las conversiones son válidas
                if not pd.isnull(fecha_ini) and not pd.isnull(fecha_fin):
                    df = df[(df["Fecha"] >= fecha_ini) & (df["Fecha"] <= fecha_fin)]
                    print(
                        f"[CargueInfoVentasInsert] Filtrado por fechas: {fecha_ini} a {fecha_fin}. Registros tras filtro: {len(df)}"
                    )
            return df
        except Exception as e:
            logging.error(f"Error al leer el archivo Excel: {e}")
            print(
                f"[CargueInfoVentasInsert][ERROR] Error al leer el archivo Excel: {e}"
            )
            raise

    def obtener_registros_existentes(self, df, fecha_columna="Fecha"):
        """
        Consulta los registros existentes en la base de datos para el rango de fechas
        y devuelve un conjunto de claves únicas para comparación.
        """
        print("[obtener_registros_existentes] INICIO")
        if fecha_columna not in df.columns or df.empty:
            print(
                "[obtener_registros_existentes] No se encontró la columna de fecha o el DataFrame está vacío."
            )
            return set()

        fecha_min = df[fecha_columna].min()
        fecha_max = df[fecha_columna].max()
        print(
            f"[obtener_registros_existentes] Consultando registros existentes para rango: {fecha_min} a {fecha_max}"
        )

        if pd.isnull(fecha_min) or pd.isnull(fecha_max):
            print(
                "[obtener_registros_existentes] No se pudo determinar el rango de fechas del archivo."
            )
            return set()

        try:
            with self.engine_mysql_bi.connect() as connection:
                # Consultar registros existentes con clave compuesta
                select_stmt = text(
                    """
                    SELECT 
                        `Cod. cliente`, `Cod. vendedor`, `Cod. productto`, 
                        `Fac. numero`, `Tipo`, `Fecha`
                    FROM infoventas
                    WHERE `Fecha` >= :fecha_min AND `Fecha` <= :fecha_max
                """
                )

                result = connection.execute(
                    select_stmt, {"fecha_min": fecha_min, "fecha_max": fecha_max}
                )

                # Crear conjunto de claves únicas de registros existentes
                registros_existentes = set()
                for row in result:
                    # Crear clave compuesta como tupla
                    clave = (
                        str(row[0]) if row[0] is not None else "",  # Cod. cliente
                        str(row[1]) if row[1] is not None else "",  # Cod. vendedor
                        str(row[2]) if row[2] is not None else "",  # Cod. productto
                        str(row[3]) if row[3] is not None else "",  # Fac. numero
                        str(row[4]) if row[4] is not None else "",  # Tipo
                        str(row[5]) if row[5] is not None else "",  # Fecha
                    )
                    registros_existentes.add(clave)

                print(
                    f"[obtener_registros_existentes] Se encontraron {len(registros_existentes)} registros existentes"
                )
                return registros_existentes

        except Exception as e:
            print(
                f"[obtener_registros_existentes] Error al consultar registros existentes: {e}"
            )
            # En caso de error, retornar conjunto vacío para que se inserten todos los registros
            return set()

    def filtrar_registros_nuevos(self, df, registros_existentes):
        """
        Filtra el DataFrame para incluir solo registros que no existen en la base de datos.
        """
        print(f"[filtrar_registros_nuevos] INICIO. Registros totales: {len(df)}")

        if not registros_existentes:
            print(
                "[filtrar_registros_nuevos] No hay registros existentes, todos los registros son nuevos"
            )
            return df

        # Crear claves para cada registro del DataFrame
        def crear_clave_registro(row):
            return (
                (
                    str(row.get("Cod. cliente", ""))
                    if pd.notna(row.get("Cod. cliente"))
                    else ""
                ),
                (
                    str(row.get("Cod. vendedor", ""))
                    if pd.notna(row.get("Cod. vendedor"))
                    else ""
                ),
                (
                    str(row.get("Cod. productto", ""))
                    if pd.notna(row.get("Cod. productto"))
                    else ""
                ),
                (
                    str(row.get("Fac. numero", ""))
                    if pd.notna(row.get("Fac. numero"))
                    else ""
                ),
                str(row.get("Tipo", "")) if pd.notna(row.get("Tipo")) else "",
                str(row.get("Fecha", "")) if pd.notna(row.get("Fecha")) else "",
            )

        # Filtrar registros que NO están en registros_existentes
        mask = df.apply(
            lambda row: crear_clave_registro(row) not in registros_existentes, axis=1
        )
        df_nuevos = df[mask].copy()

        registros_omitidos = len(df) - len(df_nuevos)
        print(
            f"[filtrar_registros_nuevos] Registros nuevos a insertar: {len(df_nuevos)}"
        )
        print(
            f"[filtrar_registros_nuevos] Registros omitidos (ya existen): {registros_omitidos}"
        )

        return df_nuevos

    def insertar_datos_db(self, registros, chunk_size=50000, progress_callback=None):
        print(
            f"[insertar_datos_db] INICIO. Total registros a insertar: {len(registros)}"
        )
        try:
            with self.engine_mysql_bi.connect() as connection:
                total = len(registros)
                for i, start in enumerate(range(0, total, chunk_size)):
                    end = min(start + chunk_size, total)
                    # Handle both DataFrame and list of dictionaries
                    if hasattr(registros, "iloc"):  # It's a DataFrame
                        chunk = registros.iloc[start:end]
                        values = [
                            {
                                "cod_cliente": row.get("Cod. cliente"),
                                "nom_cliente": row.get("Nom. Cliente"),
                                "cod_vendedor": row.get("Cod. vendedor"),
                                "nombre": row.get("Nombre"),
                                "cod_productto": row.get("Cod. productto"),
                                "descripcion": row.get("Descripción"),
                                "fecha": row.get("Fecha"),
                                "fac_numero": row.get("Fac. numero"),
                                "cantidad": row.get("Cantidad"),
                                "vta_neta": row.get("Vta neta"),
                                "tipo": row.get("Tipo"),
                                "costo": row.get("Costo"),
                                "unidad": row.get("Unidad"),
                                "pedido": row.get("Pedido"),
                                "proveedor": row.get("Proveedor"),
                                "empresa": row.get("Empresa"),
                                "lider": row.get("Líder"),
                                "area": row.get("Área"),
                                "codigo_bodega": row.get("Codigo bodega"),
                                "bodega": row.get("Bodega"),
                                "categoria": row.get("Categoría"),
                                "tipo_prod": row.get("Tipo Prod"),
                                "cod_barra": row.get("Cod. Barra"),
                                "nbLinea": row.get("nbLinea", 1.0),
                            }
                            for _, row in chunk.iterrows()
                        ]
                    else:  # It's a list of dictionaries
                        chunk = registros[start:end]
                        values = [
                            {
                                "cod_cliente": row.get("Cod. cliente"),
                                "nom_cliente": row.get("Nom. Cliente"),
                                "cod_vendedor": row.get("Cod. vendedor"),
                                "nombre": row.get("Nombre"),
                                "cod_productto": row.get("Cod. productto"),
                                "descripcion": row.get("Descripción"),
                                "fecha": row.get("Fecha"),
                                "fac_numero": row.get("Fac. numero"),
                                "cantidad": row.get("Cantidad"),
                                "vta_neta": row.get("Vta neta"),
                                "tipo": row.get("Tipo"),
                                "costo": row.get("Costo"),
                                "unidad": row.get("Unidad"),
                                "pedido": row.get("Pedido"),
                                "proveedor": row.get("Proveedor"),
                                "empresa": row.get("Empresa"),
                                "lider": row.get("Líder"),
                                "area": row.get("Área"),
                                "codigo_bodega": row.get("Codigo bodega"),
                                "bodega": row.get("Bodega"),
                                "categoria": row.get("Categoría"),
                                "tipo_prod": row.get("Tipo Prod"),
                                "cod_barra": row.get("Cod. Barra"),
                                "nbLinea": row.get("nbLinea", 1.0),
                            }
                            for row in chunk
                        ]

                    print(
                        f"[insertar_datos_db] Insertando chunk {i+1}: registros {start} a {end}"
                    )
                    print(
                        f"[insertar_datos_db] Ejecutando INSERT IGNORE para {len(values)} registros"
                    )
                    insert_stmt = text(
                        """
                        INSERT IGNORE INTO infoventas (
                            `Cod. cliente`, `Nom. Cliente`, `Cod. vendedor`, `Nombre`, `Cod. productto`,
                            `Descripción`, `Fecha`, `Fac. numero`, `Cantidad`, `Vta neta`,
                            `Tipo`, `Costo`, `Unidad`, `Pedido`, `Proveedor`,
                            `Empresa`, `Líder`, `Área`, `Codigo bodega`, `Bodega`,
                            `Categoría`, `Tipo Prod`, `Cod. Barra`, `nbLinea`
                        ) VALUES (
                            :cod_cliente, :nom_cliente, :cod_vendedor, :nombre, :cod_productto,
                            :descripcion, :fecha, :fac_numero, :cantidad, :vta_neta,
                            :tipo, :costo, :unidad, :pedido, :proveedor,
                            :empresa, :lider, :area, :codigo_bodega, :bodega,
                            :categoria, :tipo_prod, :cod_barra, :nbLinea
                        )
                        """
                    )
                    connection.execute(insert_stmt, values)
                    percent = int((end / total) * 100)
                    print(
                        f"[insertar_datos_db] Chunk {i+1}: Insertados {end} de {total} registros ({percent}%)"
                    )
                    if progress_callback:
                        progress_callback(percent)
        except Exception as e:
            print(f"[insertar_datos_db] Error al insertar datos en infoventas: {e}")
            raise

    def _limpiar_filas_vacias(self, df):
        # Elimina filas completamente vacías
        return df.dropna(how="all")

    def agrupar_datos(self, df):
        print("Agrupando datos...")
        df_limpio = self._limpiar_filas_vacias(df)
        if df_limpio is None or df_limpio.empty:
            logging.info("No hay datos válidos para procesar después de la limpieza.")
            print("No hay datos válidos para procesar después de la limpieza.")
            return None
        primary_keys = [
            "Cod. cliente",
            "Cod. vendedor",
            "Cod. productto",
            "Fac. numero",
            "Tipo",
        ]
        numeric_columns_to_sum = ["Cantidad", "Vta neta", "Costo"]
        missing_keys = [key for key in primary_keys if key not in df_limpio.columns]
        if missing_keys:
            raise ValueError(
                f"Faltan columnas clave primaria en el DataFrame para agrupar: {missing_keys}"
            )
        for col in numeric_columns_to_sum:
            if col not in df_limpio.columns:
                print(
                    f"Advertencia: La columna numérica '{col}' no existe en el DataFrame. Se creará con ceros."
                )
                df_limpio[col] = 0
            else:
                df_limpio[col] = pd.to_numeric(df_limpio[col], errors="coerce").fillna(
                    0
                )
        agg_functions = {}
        for col in numeric_columns_to_sum:
            agg_functions[col] = "sum"
        other_columns = [
            col
            for col in df_limpio.columns
            if col not in primary_keys + numeric_columns_to_sum
        ]
        for col in other_columns:
            agg_functions[col] = "first"

        print(f"Agrupando por: {primary_keys}")
        print(f"Sumando columnas: {numeric_columns_to_sum}")
        print(f"Tomando primer valor para: {other_columns}")
        df_grouped = df_limpio.groupby(primary_keys, as_index=False).agg(agg_functions)

        if "nbLinea" in df_grouped.columns:
            df_grouped["nbLinea"] = 1
        else:
            df_grouped["nbLinea"] = 1
        print(f"Dataset agrupado. Registros finales: {len(df_grouped)}")
        return df_grouped

    def obtener_registros_existentes_detallados(self, df, fecha_columna="Fecha"):
        """
        Consulta los registros existentes en la base de datos con datos detallados (cantidad, vta_neta, costo)
        para implementar la lógica inteligente de preservación de histórico.
        """
        print("[obtener_registros_existentes_detallados] INICIO")
        if fecha_columna not in df.columns or df.empty:
            print(
                "[obtener_registros_existentes_detallados] No se encontró la columna de fecha o el DataFrame está vacío."
            )
            return {}

        fecha_min = df[fecha_columna].min()
        fecha_max = df[fecha_columna].max()
        print(
            f"[obtener_registros_existentes_detallados] Consultando registros existentes para rango: {fecha_min} a {fecha_max}"
        )

        if pd.isnull(fecha_min) or pd.isnull(fecha_max):
            print(
                "[obtener_registros_existentes_detallados] No se pudo determinar el rango de fechas del archivo."
            )
            return {}

        try:
            with self.engine_mysql_bi.connect() as connection:
                # Consultar registros existentes con datos detallados
                select_stmt = text(
                    """
                    SELECT 
                        `Cod. cliente`, `Cod. vendedor`, `Cod. productto`, 
                        `Fac. numero`, `Tipo`, `Fecha`, `Cantidad`, `Vta neta`, `Costo`
                    FROM infoventas
                    WHERE `Fecha` >= :fecha_min AND `Fecha` <= :fecha_max
                """
                )

                result = connection.execute(
                    select_stmt, {"fecha_min": fecha_min, "fecha_max": fecha_max}
                )

                # Crear diccionario de registros existentes con datos detallados
                registros_existentes = {}
                for row in result:
                    # Crear clave compuesta como tupla
                    clave = (
                        str(row[0]) if row[0] is not None else "",  # Cod. cliente
                        str(row[1]) if row[1] is not None else "",  # Cod. vendedor
                        str(row[2]) if row[2] is not None else "",  # Cod. productto
                        str(row[3]) if row[3] is not None else "",  # Fac. numero
                        str(row[4]) if row[4] is not None else "",  # Tipo
                        str(row[5]) if row[5] is not None else "",  # Fecha
                    )

                    # Almacenar datos detallados del registro existente
                    registros_existentes[clave] = {
                        "cantidad": float(row[6]) if row[6] is not None else 0.0,
                        "vta_neta": float(row[7]) if row[7] is not None else 0.0,
                        "costo": float(row[8]) if row[8] is not None else 0.0,
                    }

                print(
                    f"[obtener_registros_existentes_detallados] Se encontraron {len(registros_existentes)} registros existentes con datos detallados"
                )
                return registros_existentes

        except Exception as e:
            print(
                f"[obtener_registros_existentes_detallados] Error al consultar registros existentes: {e}"
            )
            # En caso de error, retornar diccionario vacío para que se inserten todos los registros
            return {}

    def clasificar_registros_para_procesamiento(self, df, registros_existentes):
        """
        Clasifica los registros del DataFrame según la lógica inteligente:
        - NUEVOS: Clave compuesta no existe en BD → INSERT
        - ACTUALIZAR: Cambió cantidad o vta_neta → UPDATE
        - PRESERVAR: Solo cambió costo → Mantener histórico (no procesar)
        """
        print("[clasificar_registros_para_procesamiento] INICIO")

        registros_nuevos = []
        registros_actualizar = []
        registros_preservar = []

        def crear_clave_registro(row):
            return (
                (
                    str(row.get("Cod. cliente", ""))
                    if pd.notna(row.get("Cod. cliente"))
                    else ""
                ),
                (
                    str(row.get("Cod. vendedor", ""))
                    if pd.notna(row.get("Cod. vendedor"))
                    else ""
                ),
                (
                    str(row.get("Cod. productto", ""))
                    if pd.notna(row.get("Cod. productto"))
                    else ""
                ),
                (
                    str(row.get("Fac. numero", ""))
                    if pd.notna(row.get("Fac. numero"))
                    else ""
                ),
                str(row.get("Tipo", "")) if pd.notna(row.get("Tipo")) else "",
                str(row.get("Fecha", "")) if pd.notna(row.get("Fecha")) else "",
            )

        for _, row in df.iterrows():
            clave = crear_clave_registro(row)

            # Obtener valores numéricos del registro actual
            cantidad_actual = (
                float(row.get("Cantidad", 0)) if pd.notna(row.get("Cantidad")) else 0.0
            )
            vta_neta_actual = (
                float(row.get("Vta neta", 0)) if pd.notna(row.get("Vta neta")) else 0.0
            )
            costo_actual = (
                float(row.get("Costo", 0)) if pd.notna(row.get("Costo")) else 0.0
            )

            if clave not in registros_existentes:
                # CASO 1: Registro completamente NUEVO
                registros_nuevos.append(row.to_dict())
            else:
                # CASO 2 y 3: Registro existe, analizar qué cambió
                datos_existentes = registros_existentes[clave]
                cantidad_existente = datos_existentes["cantidad"]
                vta_neta_existente = datos_existentes["vta_neta"]
                costo_existente = datos_existentes["costo"]

                # Verificar si cambió cantidad o vta_neta (con tolerancia para decimales)
                cambio_cantidad = abs(cantidad_actual - cantidad_existente) > 0.01
                cambio_vta_neta = abs(vta_neta_actual - vta_neta_existente) > 0.01
                cambio_costo = abs(costo_actual - costo_existente) > 0.01

                if cambio_cantidad or cambio_vta_neta:
                    # CASO 2: Cambió cantidad o vta_neta → ACTUALIZAR
                    registro_actualizar = row.to_dict()
                    registro_actualizar["_clave_compuesta"] = (
                        clave  # Para facilitar el UPDATE
                    )
                    registros_actualizar.append(registro_actualizar)
                elif cambio_costo:
                    # CASO 3: Solo cambió costo → PRESERVAR histórico
                    registros_preservar.append(row.to_dict())
                # Si no cambió nada, no hacer nada (registro idéntico)

        clasificacion = {
            "nuevos": registros_nuevos,
            "actualizar": registros_actualizar,
            "preservar": registros_preservar,
        }

        print(f"[clasificar_registros_para_procesamiento] Clasificación completada:")
        print(f"  - NUEVOS: {len(registros_nuevos)}")
        print(f"  - ACTUALIZAR: {len(registros_actualizar)}")
        print(f"  - PRESERVAR: {len(registros_preservar)}")

        return clasificacion

    def actualizar_registros_db(
        self, registros_actualizar, chunk_size=50000, progress_callback=None
    ):
        """
        Actualiza los registros en la base de datos que han cambiado en cantidad o vta_neta.
        Preserva el histórico de costos al no actualizar registros que solo cambiaron en costo.
        """
        print(
            f"[actualizar_registros_db] INICIO. Total registros a actualizar: {len(registros_actualizar)}"
        )

        if not registros_actualizar:
            print("[actualizar_registros_db] No hay registros para actualizar")
            return

        try:
            with self.engine_mysql_bi.connect() as connection:
                total = len(registros_actualizar)
                for i, start in enumerate(range(0, total, chunk_size)):
                    end = min(start + chunk_size, total)
                    chunk = registros_actualizar[start:end]
                    print(
                        f"[actualizar_registros_db] Actualizando chunk {i+1}: registros {start} a {end}"
                    )

                    for registro in chunk:
                        # Construir UPDATE usando la clave compuesta
                        clave = registro.get("_clave_compuesta")
                        if not clave:
                            print(
                                f"[actualizar_registros_db] Advertencia: Registro sin clave compuesta, saltando..."
                            )
                            continue

                        update_stmt = text(
                            """
                            UPDATE infoventas 
                            SET 
                                `Nom. Cliente` = :nom_cliente,
                                `Nombre` = :nombre,
                                `Descripción` = :descripcion,
                                `Cantidad` = :cantidad,
                                `Vta neta` = :vta_neta,
                                `Costo` = :costo,
                                `Unidad` = :unidad,
                                `Pedido` = :pedido,
                                `Proveedor` = :proveedor,
                                `Empresa` = :empresa,
                                `Líder` = :lider,
                                `Área` = :area,
                                `Codigo bodega` = :codigo_bodega,
                                `Bodega` = :bodega,
                                `Categoría` = :categoria,
                                `Tipo Prod` = :tipo_prod,
                                `Cod. Barra` = :cod_barra,
                                `nbLinea` = :nbLinea
                            WHERE 
                                `Cod. cliente` = :cod_cliente
                                AND `Cod. vendedor` = :cod_vendedor
                                AND `Cod. productto` = :cod_productto
                                AND `Fac. numero` = :fac_numero
                                AND `Tipo` = :tipo
                                AND `Fecha` = :fecha
                        """
                        )

                        connection.execute(
                            update_stmt,
                            {
                                "nom_cliente": registro.get("Nom. Cliente"),
                                "nombre": registro.get("Nombre"),
                                "descripcion": registro.get("Descripción"),
                                "cantidad": registro.get("Cantidad"),
                                "vta_neta": registro.get("Vta neta"),
                                "costo": registro.get("Costo"),
                                "unidad": registro.get("Unidad"),
                                "pedido": registro.get("Pedido"),
                                "proveedor": registro.get("Proveedor"),
                                "empresa": registro.get("Empresa"),
                                "lider": registro.get("Líder"),
                                "area": registro.get("Área"),
                                "codigo_bodega": registro.get("Codigo bodega"),
                                "bodega": registro.get("Bodega"),
                                "categoria": registro.get("Categoría"),
                                "tipo_prod": registro.get("Tipo Prod"),
                                "cod_barra": registro.get("Cod. Barra"),
                                "nbLinea": registro.get("nbLinea", 1.0),
                                # Cláusula WHERE
                                "cod_cliente": clave[0],
                                "cod_vendedor": clave[1],
                                "cod_productto": clave[2],
                                "fac_numero": clave[3],
                                "tipo": clave[4],
                                "fecha": clave[5],
                            },
                        )

                    percent = int((end / total) * 100)
                    print(
                        f"[actualizar_registros_db] Chunk {i+1}: Actualizados {end} de {total} registros ({percent}%)"
                    )

                    if progress_callback:
                        progress_callback(percent)

                print(
                    f"[actualizar_registros_db] Actualización completada exitosamente. Total registros actualizados: {total}"
                )

        except Exception as e:
            print(f"[actualizar_registros_db] Error al actualizar registros: {e}")
            raise

    def procesar_cargue(self, progress_callback=None):
        print("[CargueInfoVentasInsert] procesar_cargue INICIO")
        inicio = time.time()
        try:
            # 1. Cargar datos del Excel
            if progress_callback:
                progress_callback(10)
            df = self.cargar_datos_excel()
            print(
                f"[CargueInfoVentasInsert] DataFrame cargado. Registros: {len(df)}. Columnas: {list(df.columns)}"
            )
            if df is None or df.empty:
                print(
                    "[CargueInfoVentasInsert] No hay datos en el archivo Excel para procesar."
                )
                return {
                    "success": False,
                    "message": "No hay datos en el archivo Excel para procesar",
                    "registros_procesados": 0,
                    "registros_insertados": 0,
                    "registros_actualizados": 0,
                    "registros_preservados": 0,
                }

            # 2. Agrupar datos
            if progress_callback:
                progress_callback(20)
            df = self.agrupar_datos(df)
            if df is None or df.empty:
                print("[CargueInfoVentasInsert] No hay datos agrupados para insertar.")
                return {
                    "success": False,
                    "message": "No hay datos agrupados para insertar",
                    "registros_procesados": 0,
                    "registros_insertados": 0,
                    "registros_actualizados": 0,
                    "registros_preservados": 0,
                }

            if "Fecha" in df.columns:
                registros_antes_filtrado = len(df)
                df = df[df["Fecha"].notna()]
                if len(df) != registros_antes_filtrado:
                    print(
                        f"[CargueInfoVentasInsert] Filtrados {registros_antes_filtrado - len(df)} registros sin fecha válida"
                    )

            # Capturar rango de fechas detectado en el Excel agrupado
            fecha_min_df = None
            fecha_max_df = None
            if "Fecha" in df.columns:
                try:
                    fecha_min_df = df["Fecha"].min()
                    fecha_max_df = df["Fecha"].max()
                except Exception as e:
                    print(f"[CargueInfoVentasInsert] Advertencia al calcular rango de fechas: {e}")
                    fecha_min_df = None
                    fecha_max_df = None

            # 3. NUEVA LÓGICA INTELIGENTE: Preservación de histórico de costos
            print(
                "[CargueInfoVentasInsert] ===== IMPLEMENTANDO LÓGICA INTELIGENTE DE PRESERVACIÓN DE HISTÓRICO ====="
            )

            if progress_callback:
                progress_callback(30)

            # Obtener registros existentes con datos detallados (cantidad, vta_neta, costo)
            print(
                "[CargueInfoVentasInsert] Obteniendo registros existentes con datos detallados..."
            )
            registros_existentes = self.obtener_registros_existentes_detallados(df)

            if progress_callback:
                progress_callback(50)

            # Clasificar registros según la lógica inteligente
            print(
                "[CargueInfoVentasInsert] Clasificando registros para procesamiento inteligente..."
            )
            clasificacion = self.clasificar_registros_para_procesamiento(
                df, registros_existentes
            )

            registros_nuevos = clasificacion["nuevos"]
            registros_actualizar = clasificacion["actualizar"]
            registros_preservar = clasificacion["preservar"]

            print(f"[CargueInfoVentasInsert] RESULTADOS DE CLASIFICACIÓN:")
            print(
                f"[CargueInfoVentasInsert] - Registros NUEVOS (insertar): {len(registros_nuevos)}"
            )
            print(
                f"[CargueInfoVentasInsert] - Registros ACTUALIZAR (cantidad/vta_neta cambió): {len(registros_actualizar)}"
            )
            print(
                f"[CargueInfoVentasInsert] - Registros PRESERVAR (solo cambió costo): {len(registros_preservar)}"
            )

            if progress_callback:
                progress_callback(60)

            # 4. Ejecutar operaciones
            total_insertados = 0
            total_actualizados = 0

            # INSERTAR registros completamente nuevos
            if len(registros_nuevos) > 0:
                print(
                    f"[CargueInfoVentasInsert] Insertando {len(registros_nuevos)} registros nuevos..."
                )
                self.insertar_datos_db(
                    registros_nuevos,
                    chunk_size=50000,
                    progress_callback=progress_callback,
                )
                total_insertados = len(registros_nuevos)

            if progress_callback:
                progress_callback(80)

            # ACTUALIZAR registros que cambiaron cantidad o vta_neta
            if len(registros_actualizar) > 0:
                print(
                    f"[CargueInfoVentasInsert] Actualizando {len(registros_actualizar)} registros que cambiaron cantidad/vta_neta..."
                )
                self.actualizar_registros_db(
                    registros_actualizar,
                    chunk_size=50000,
                    progress_callback=progress_callback,
                )
                total_actualizados = len(registros_actualizar)

            if progress_callback:
                progress_callback(90)

            # PRESERVAR histórico (no hacer nada con estos registros)
            if len(registros_preservar) > 0:
                print(
                    f"[CargueInfoVentasInsert] PRESERVANDO histórico de {len(registros_preservar)} registros (solo cambió el costo)"
                )

            # 5. Resultado final
            final = time.time()
            tiempo_transcurrido = final - inicio

            # Forzar actualización de progreso a 100% antes de finalizar
            if progress_callback:
                try:
                    progress_callback(100)
                    print("[CargueInfoVentasInsert] Progreso final 100% reportado")
                except Exception as e:
                    print(
                        f"[CargueInfoVentasInsert] Error al actualizar progreso final: {e}"
                    )

            resultado = {
                "success": True,
                "message": "Cargue completado exitosamente con lógica inteligente de preservación de histórico",
                "registros_procesados": len(df),
                "registros_insertados": total_insertados,
                "registros_actualizados": total_actualizados,
                "registros_preservados": len(registros_preservar),
                "tiempo_transcurrido": tiempo_transcurrido,
                "fecha_min": fecha_min_df,
                "fecha_max": fecha_max_df,
                "beneficios": [
                    "✅ Histórico de costos preservado",
                    "✅ Solo se procesaron registros que realmente cambiaron",
                    "✅ Mejor rendimiento sin DELETE masivos",
                    "✅ Audit trail más limpio",
                ],
            }

            print(
                f"[CargueInfoVentasInsert] ===== PROCESO COMPLETADO EXITOSAMENTE ====="
            )
            print(
                f"[CargueInfoVentasInsert] Tiempo total: {tiempo_transcurrido:.2f} segundos"
            )
            print(f"[CargueInfoVentasInsert] Resumen: {resultado}")

            # Ya no es necesario duplicar la llamada al final, la movimos arriba para garantizar que se ejecute

            return resultado

        except Exception as e:
            print(
                f"[CargueInfoVentasInsert][ERROR] Error durante el proceso de carga: {e}"
            )
            if progress_callback:
                progress_callback(0)  # Reset progress on error
            raise
