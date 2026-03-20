# PlanosTSOL.py
# Generador de archivos TSOL (TrackSales 3.7.2.3) conectado a base de datos.
# Sigue el patrón de PlanosCDT.py - Lee de powerbi_* vía conf_sql_tsol.

import json
import logging
import os
import re
import zipfile
import ftplib
import calendar
from datetime import datetime
from io import StringIO
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


class PlanosTSOL:
    """
    Procesador de planos TSOL conectado a base de datos.

    Lee datos de las bases powerbi_* usando queries configuradas en conf_sql_tsol,
    aplica filtros de proveedor y sede, y genera los 11 archivos TSOL requeridos
    por la especificación TrackSales 3.7.2.3.
    """

    # Mapeo nmReporte -> nombre de archivo de salida
    REPORTE_A_ARCHIVO = {
        "ventas_tsol": "ventas.txt",
        "productos_tsol": "SKU (Productos).txt",
        "clientes_tsol": "Clientes.txt",
        "vendedores_tsol": "Vendedores.txt",
        "supervisores_tsol": "Supervisores.txt",
        "inventario_tsol": "Inventario.txt",
        "tipos_negocio_tsol": "Tipos De Negocio.txt",
        "municipios_tsol": "Municipios.txt",
        "rutas_tsol": "Rutas.txt",
        "facturas_tsol": "Listado de Facturas.txt",
        "totales_tsol": "Totales de Control.txt",
    }

    def __init__(
        self,
        empresa_id: int,
        fecha_ini: str,
        fecha_fin: str,
        user_id: Optional[int] = None,
        enviar_ftp: bool = False,
    ):
        import django

        django.setup()

        from apps.permisos.models import ConfEmpresas, ConfSqlTsol
        from scripts.config import ConfigBasic
        from scripts.conexion import Conexion

        self.ConfigBasic = ConfigBasic
        self.Conexion = Conexion

        # Cargar empresa
        self.empresa = ConfEmpresas.objects.get(id=empresa_id)
        if not self.empresa.envio_tsol_activo:
            raise ValueError(f"TSOL no está activo para la empresa '{self.empresa.name}'.")

        self.empresas = [self.empresa]

        # Cargar configuraciones SQL
        self.sql_configs: Dict[str, str] = {}
        if self.empresa.planos_tsol:
            try:
                sql_ids = json.loads(self.empresa.planos_tsol)
            except (json.JSONDecodeError, TypeError):
                sql_ids = [
                    int(x.strip())
                    for x in str(self.empresa.planos_tsol).split(",")
                    if x.strip().isdigit()
                ]
            configs = ConfSqlTsol.objects.filter(nbSql__in=sql_ids)
            for cfg in configs:
                self.sql_configs[cfg.nmReporte] = cfg.txSqlExtrae

        if not self.sql_configs:
            raise ValueError("No se encontraron configuraciones SQL TSOL.")

        # Parámetros
        self.fecha_ini = fecha_ini
        self.fecha_fin = fecha_fin
        self.user_id = user_id
        self.enviar_ftp = enviar_ftp

        # Configuración de sede/bodega desde la empresa
        self.bodega_map = self.empresa.get_tsol_bodega_mapping()
        self.code_to_sede_map = self.empresa.get_tsol_code_to_sede()
        self.sedes_permitidas = self.empresa.get_tsol_sedes_set()
        self.sede_default_code = self.empresa.tsol_sede_default_code or "01"
        self.sede_default_name = self.empresa.tsol_sede_default_name or "PALMIRA/CALI"

        # Filtro de proveedores
        self.proveedores_filtro = self.empresa.get_tsol_proveedores_list()

        # Datos extraídos (por empresa, luego consolidados)
        self.datos: Dict[str, pd.DataFrame] = {}

        # Log buffer
        self._log_buffer = StringIO()
        self._log_handler = logging.StreamHandler(self._log_buffer)
        self._log_handler.setLevel(logging.INFO)
        logger.addHandler(self._log_handler)

        # Directorio de salida
        fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        tsol_label = (self.empresa.tsol_nombre or self.empresa.name).replace(" ", "_")
        self.output_dir = os.path.join(
            "media", "tsol", tsol_label, fecha_str
        )
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(
            "PlanosTSOL inicializado: empresa=%s, periodo=%s a %s",
            self.empresa.name,
            self.fecha_ini,
            self.fecha_fin,
        )

    # ── Conexión a BD ──────────────────────────────────────────────

    def _get_bi_engine(self, empresa):
        """Crea engine de conexión a la BD BI de una empresa."""
        config = self.ConfigBasic(empresa.name).config
        user = config.get("nmUsrIn")
        password = config.get("txPassIn")
        host = config.get("hostServerIn")
        port = config.get("portServerIn")
        database = config.get("dbBi")

        if not all([user, password, host, port, database]):
            raise ValueError(
                f"Configuración de BD incompleta para empresa '{empresa.name}'."
            )

        return self.Conexion.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def _ejecutar_sql(self, engine, sql_text: str, params: dict) -> pd.DataFrame:
        """Ejecuta un SQL con parámetros y retorna DataFrame."""
        with engine.connect() as conn:
            result = conn.execute(text(sql_text), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    # ── Extracción de datos ────────────────────────────────────────

    def extraer_datos_empresa(self, empresa) -> Dict[str, pd.DataFrame]:
        """Ejecuta todas las queries SQL para una empresa."""
        engine = self._get_bi_engine(empresa)
        params = {"fi": self.fecha_ini, "ff": self.fecha_fin}
        datos = {}

        for nm_reporte, sql in self.sql_configs.items():
            if not sql or not sql.strip():
                logger.warning(
                    "SQL vacío para reporte '%s' - saltando.", nm_reporte
                )
                continue
            try:
                df = self._ejecutar_sql(engine, sql, params)
                datos[nm_reporte] = df
                logger.info(
                    "Empresa %s, reporte %s: %d registros extraídos.",
                    empresa.name, nm_reporte, len(df),
                )
            except Exception as exc:
                logger.error(
                    "Error extrayendo %s para %s: %s",
                    nm_reporte, empresa.name, exc,
                )
                datos[nm_reporte] = pd.DataFrame()

        return datos

    def extraer_todos(self):
        """Extrae datos de todas las empresas y consolida."""
        datos_consolidados: Dict[str, List[pd.DataFrame]] = {}

        for empresa in self.empresas:
            logger.info("Extrayendo datos de empresa: %s", empresa.name)
            datos_empresa = self.extraer_datos_empresa(empresa)
            for nm_reporte, df in datos_empresa.items():
                if not df.empty:
                    datos_consolidados.setdefault(nm_reporte, []).append(df)

        # Consolidar DataFrames de múltiples empresas
        for nm_reporte, dfs in datos_consolidados.items():
            if dfs:
                self.datos[nm_reporte] = pd.concat(dfs, ignore_index=True)
            else:
                self.datos[nm_reporte] = pd.DataFrame()

        logger.info(
            "Extracción completa. Reportes disponibles: %s",
            list(self.datos.keys()),
        )

    # ── Filtros ────────────────────────────────────────────────────

    def _inferir_codigo_sede(self, nombre_bodega) -> str:
        """Devuelve el código TSOL de la sede a partir del nombre de bodega."""
        if nombre_bodega is None or pd.isna(nombre_bodega):
            return self.sede_default_code
        normalizado = str(nombre_bodega).strip().upper()
        if normalizado in self.bodega_map:
            return self.bodega_map[normalizado]
        for etiqueta, codigo in self.bodega_map.items():
            if etiqueta.upper() in normalizado:
                return codigo
        return self.sede_default_code

    def _obtener_nombre_sede(self, codigo_sede) -> str:
        """Devuelve el nombre de sede a partir del código TSOL."""
        return self.code_to_sede_map.get(
            str(codigo_sede), self.sede_default_name
        )

    def _normalizar_codigo_bodega(self, valor) -> str:
        """Acepta nombres o códigos de bodega y retorna el código TSOL."""
        if valor is None or pd.isna(valor):
            return self.sede_default_code
        texto = str(valor).strip()
        if not texto:
            return self.sede_default_code
        if texto in self.code_to_sede_map:
            return texto
        return self._inferir_codigo_sede(texto)

    def filtrar_por_proveedor(self):
        """Aplica filtro de proveedores a ventas, inventario y productos."""
        if not self.proveedores_filtro:
            logger.info("Sin filtro de proveedores - incluyendo todos.")
            return

        regex_pattern = "|".join(
            re.escape(p) for p in self.proveedores_filtro
        )
        # Reportes que tienen columna de proveedor a filtrar
        reportes_con_proveedor = [
            "ventas_tsol", "inventario_tsol", "productos_tsol",
        ]
        # Columnas posibles donde buscar el proveedor
        columnas_proveedor = [
            "Proveedor", "proveedor", "nmProveedor", "PROVEEDOR",
        ]

        for nm_reporte in reportes_con_proveedor:
            df = self.datos.get(nm_reporte)
            if df is None or df.empty:
                continue
            col_prov = None
            for col in columnas_proveedor:
                if col in df.columns:
                    col_prov = col
                    break
            if col_prov is None:
                continue

            antes = len(df)
            df_filtrado = df[
                df[col_prov].astype(str).str.contains(
                    regex_pattern, case=False, na=False
                )
            ]
            self.datos[nm_reporte] = df_filtrado
            logger.info(
                "Filtro proveedores en %s: %d -> %d registros.",
                nm_reporte, antes, len(df_filtrado),
            )

    def filtrar_por_sedes(self):
        """Aplica mapeo de bodega->código y filtro de sedes permitidas."""
        if not self.sedes_permitidas and not self.bodega_map:
            return

        # Columnas posibles de bodega/sede
        columnas_bodega = [
            "Codigo bodega", "Código de bodega", "Código Sede",
            "codigo_bodega", "nbAlmacen", "Bodega",
        ]

        for nm_reporte, df in self.datos.items():
            if df.empty:
                continue
            col_bodega = None
            for col in columnas_bodega:
                if col in df.columns:
                    col_bodega = col
                    break
            if col_bodega is None:
                continue

            # Aplicar mapeo de bodega
            if self.bodega_map:
                df[col_bodega] = df[col_bodega].apply(
                    self._normalizar_codigo_bodega
                )

            # Filtrar por sedes permitidas
            if self.sedes_permitidas:
                antes = len(df)
                df = df[df[col_bodega].isin(self.sedes_permitidas)]
                if antes != len(df):
                    logger.info(
                        "Filtro sedes en %s: %d -> %d registros.",
                        nm_reporte, antes, len(df),
                    )
                self.datos[nm_reporte] = df

    # ── Escritura de archivos TXT ──────────────────────────────────

    def _escribir_txt_tsol(
        self,
        df: pd.DataFrame,
        nombre_archivo: str,
        columnas: Optional[List[str]] = None,
    ) -> str:
        """
        Escribe DataFrame como TXT delimitado por '{' con cabecera.
        Retorna la ruta completa del archivo generado.
        """
        filepath = os.path.join(self.output_dir, nombre_archivo)
        if columnas:
            # Solo usar columnas que existen en el DataFrame
            cols_disponibles = [c for c in columnas if c in df.columns]
            df = df[cols_disponibles]

        header = "{".join(df.columns)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(header + "\n")
            for _, row in df.iterrows():
                f.write("{".join(str(v) for v in row.values) + "\n")

        logger.info("Archivo generado: %s (%d registros)", nombre_archivo, len(df))
        return filepath

    def _formatear_decimal_coma(self, valor, decimales=2) -> str:
        """Formatea un número con coma como separador decimal."""
        try:
            return f"{float(valor):.{decimales}f}".replace(".", ",")
        except (ValueError, TypeError):
            return "0,00"

    def _formatear_codigo_producto(self, codigo) -> str:
        """Normaliza códigos de producto preservando ceros a la izquierda."""
        if codigo is None or pd.isna(codigo):
            return ""
        texto = str(codigo).strip().replace('"', "").replace("'", "")
        if not texto:
            return ""
        if re.match(r"^\d+\.0+$", texto):
            texto = texto.split(".")[0]
        return texto

    def _normalizar_codigo_vendedor(self, codigo) -> str:
        """Normaliza códigos de vendedor."""
        if codigo is None or pd.isna(codigo):
            return ""
        texto = str(codigo).strip().replace(".0", "")
        if texto.upper() == "ROVEEDOR":
            return "PROVEEDOR"
        return texto

    # ── Generadores de archivos (11 tipos) ─────────────────────────

    def generar_ventas(self) -> Optional[str]:
        """Genera ventas.txt desde datos extraídos."""
        df = self.datos.get("ventas_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para ventas_tsol.")
            return None

        # Normalizar códigos
        if "Codigo Cliente" in df.columns:
            df["Codigo Cliente"] = (
                df["Codigo Cliente"].astype(str).str.strip().str.replace("-", "999", regex=False)
            )
        if "Codigo Vendedor" in df.columns:
            df["Codigo Vendedor"] = df["Codigo Vendedor"].apply(
                self._normalizar_codigo_vendedor
            )
        if "Codigo Producto (Sku)" in df.columns:
            df["Codigo Producto (Sku)"] = df["Codigo Producto (Sku)"].apply(
                lambda x: self._formatear_codigo_producto(x).upper()
            )

        # Formatear valores monetarios con coma decimal
        for col in ["Valor Total Item Vendido", "Costo"]:
            if col in df.columns:
                df[col] = df[col].apply(self._formatear_decimal_coma)

        # Mapear bodega a código de sede
        if "Codigo bodega" in df.columns:
            df["Codigo bodega"] = df["Codigo bodega"].apply(
                self._normalizar_codigo_bodega
            )
            df["Nombre Sede"] = df["Codigo bodega"].apply(
                self._obtener_nombre_sede
            )

        columnas = [
            "Codigo Cliente", "Codigo Vendedor", "Codigo Producto (Sku)",
            "Fecha", "Numero Documento", "Cantidad",
            "Valor Total Item Vendido", "Tipo", "Costo",
            "Unidad de Medida", "Codigo bodega", "Nombre Sede",
        ]

        self.datos["ventas_tsol"] = df
        return self._escribir_txt_tsol(df, "ventas.txt", columnas)

    def generar_productos(self) -> Optional[str]:
        """Genera SKU (Productos).txt."""
        df = self.datos.get("productos_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para productos_tsol.")
            return None

        if "Codigo" in df.columns:
            df["Codigo"] = df["Codigo"].apply(self._formatear_codigo_producto)

        columnas = [
            "Codigo", "Nombre", "Tipo Referencia", "Tipo De Unidad",
            "Codigo De Barras", "Codigo Categoria", "Nombre Categoria",
            "Codigo SubCategoria", "Nombre SubCategoria",
            "Factor Conversion Unidad", "Factor Peso",
            "Codigo Sede", "Nombre Sede", "Proveedor",
        ]

        return self._escribir_txt_tsol(df, "SKU (Productos).txt", columnas)

    def generar_clientes(self) -> Optional[str]:
        """Genera Clientes.txt (15 campos según spec TSOL)."""
        df = self.datos.get("clientes_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para clientes_tsol.")
            return None

        if "Codigo" in df.columns:
            df["Codigo"] = (
                df["Codigo"].astype(str).str.strip().str.replace("-", "999", regex=False)
            )

        columnas = [
            "Codigo", "Nombre", "Fecha Ingreso", "Nit", "Direccion",
            "Telefono", "Representante Legal", "Codigo Municipio",
            "Codigo Tipo Negocio", "Estrato", "Codigo Sede", "Nombre Sede",
            "Ubicacion longitud", "Ubicacion latitud",
            "Identificador de sucursal",
        ]

        return self._escribir_txt_tsol(df, "Clientes.txt", columnas)

    def generar_vendedores(self) -> Optional[str]:
        """Genera Vendedores.txt."""
        df = self.datos.get("vendedores_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para vendedores_tsol.")
            return None

        if "Codigo" in df.columns:
            df["Codigo"] = df["Codigo"].apply(self._normalizar_codigo_vendedor)

        # Mapear bodega
        if "Codigo Sede" in df.columns:
            df["Codigo Sede"] = df["Codigo Sede"].apply(
                self._normalizar_codigo_bodega
            )
            df["Nombre Sede"] = df["Codigo Sede"].apply(
                self._obtener_nombre_sede
            )

        columnas = [
            "Codigo", "Nombre", "Ubicacion", "Cedula",
            "Codigo Supervisor", "Codigo Sede", "Nombre Sede",
        ]

        return self._escribir_txt_tsol(df, "Vendedores.txt", columnas)

    def generar_supervisores(self) -> Optional[str]:
        """Genera Supervisores.txt."""
        df = self.datos.get("supervisores_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para supervisores_tsol.")
            return None

        columnas = ["Codigo", "Nombre", "Codigo Sede", "Nombre Sede"]
        return self._escribir_txt_tsol(df, "Supervisores.txt", columnas)

    def generar_inventario(self) -> Optional[str]:
        """Genera Inventario.txt."""
        df = self.datos.get("inventario_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para inventario_tsol.")
            return None

        # Mapear bodega
        for col in ["Codigo de bodega", "Codigo Sede"]:
            if col in df.columns:
                df[col] = df[col].apply(self._normalizar_codigo_bodega)
        if "Nombre Sede" in df.columns and "Codigo Sede" in df.columns:
            df["Nombre Sede"] = df["Codigo Sede"].apply(
                self._obtener_nombre_sede
            )

        columnas = [
            "Fecha", "Codigo Producto", "Cantidad", "Unidad de Medida",
            "Codigo de bodega", "Codigo Sede", "Nombre Sede",
        ]

        return self._escribir_txt_tsol(df, "Inventario.txt", columnas)

    def generar_tipos_negocio(self) -> Optional[str]:
        """Genera Tipos De Negocio.txt."""
        df = self.datos.get("tipos_negocio_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para tipos_negocio_tsol.")
            return None

        columnas = ["Codigo", "Nombre"]
        return self._escribir_txt_tsol(df, "Tipos De Negocio.txt", columnas)

    def generar_municipios(self) -> Optional[str]:
        """Genera Municipios.txt."""
        df = self.datos.get("municipios_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para municipios_tsol.")
            return None

        columnas = ["Codigo", "Nombre"]
        return self._escribir_txt_tsol(df, "Municipios.txt", columnas)

    def generar_rutas(self) -> Optional[str]:
        """Genera Rutas.txt."""
        df = self.datos.get("rutas_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para rutas_tsol.")
            return None

        # Normalizar código cliente
        if "Codigo Cliente" in df.columns:
            df["Codigo Cliente"] = (
                df["Codigo Cliente"].astype(str).str.strip().str.replace("-", "999", regex=False)
            )

        # Mapear bodega
        if "Codigo Sede" in df.columns:
            df["Codigo Sede"] = df["Codigo Sede"].apply(
                self._normalizar_codigo_bodega
            )
            df["Nombre Sede"] = df["Codigo Sede"].apply(
                self._obtener_nombre_sede
            )

        columnas = [
            "Codigo Vendedor", "Codigo Cliente", "Mes", "Dia Semana",
            "Frecuencia", "Codigo Sede", "Nombre Sede",
            "Identificador de sucursal",
        ]

        return self._escribir_txt_tsol(df, "Rutas.txt", columnas)

    def generar_listado_facturas(self) -> Optional[str]:
        """Genera Listado de Facturas.txt (agregado desde ventas)."""
        df = self.datos.get("facturas_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para facturas_tsol.")
            return None

        columnas = [
            "Codigo Cliente", "Codigo Vendedor", "Fecha",
            "Numero Documento", "Valor_Total_Factura",
            "Valor_Facturado_Casa_Comercial",
        ]

        return self._escribir_txt_tsol(df, "Listado de Facturas.txt", columnas)

    def generar_totales_control(self) -> Optional[str]:
        """Genera Totales de Control.txt (sum desde ventas)."""
        df = self.datos.get("totales_tsol")
        if df is None or df.empty:
            logger.warning("Sin datos para totales_tsol.")
            return None

        columnas = ["Descriptor Total", "Valor"]
        return self._escribir_txt_tsol(df, "Totales de Control.txt", columnas)

    # ── ZIP ────────────────────────────────────────────────────────

    def generar_zip(self) -> str:
        """Comprime todos los archivos TXT en un ZIP con nombre TSOL."""
        # Determinar fecha para el nombre del ZIP
        try:
            fecha_fin_dt = datetime.strptime(self.fecha_fin, "%Y-%m-%d")
        except (ValueError, TypeError):
            fecha_fin_dt = datetime.now()

        dia = fecha_fin_dt.day
        mes = fecha_fin_dt.month
        ano = fecha_fin_dt.year

        zip_filename = f"{self.empresa.tsol_codigo or 'TSOL'}_{ano}6{mes:02d}{dia:02d}.zip"
        zip_path = os.path.join(self.output_dir, zip_filename)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(self.output_dir):
                for file in files:
                    if file.endswith(".txt"):
                        file_path = os.path.join(root, file)
                        zipf.write(file_path, os.path.basename(file_path))
                        logger.info("Añadido al ZIP: %s", file)

        logger.info("ZIP generado: %s", zip_path)
        return zip_path

    # ── FTP ────────────────────────────────────────────────────────

    def enviar_por_ftp(self, zip_path: str) -> bool:
        """Envía el archivo ZIP al servidor FTP configurado."""
        try:
            tsol_conn = self.empresa.tsol_conexion or {}
            ftp_host = tsol_conn.get("host", "")
            ftp_port = tsol_conn.get("port", 21)
            ftp_user = tsol_conn.get("user", "")
            ftp_pass = tsol_conn.get("pass", "")

            if not all([ftp_host, ftp_user, ftp_pass]):
                logger.warning("Credenciales FTP incompletas para %s.", self.empresa.name)
                return False

            if not os.path.exists(zip_path):
                logger.error("Archivo ZIP no encontrado: %s", zip_path)
                return False

            logger.info("Conectando a FTP: %s:%d", ftp_host, ftp_port)

            ftp = ftplib.FTP()
            ftp.connect(ftp_host, ftp_port, timeout=30)
            ftp.login(ftp_user, ftp_pass)
            logger.info("Sesión FTP iniciada como: %s", ftp_user)

            # Cambiar a directorio remoto si está configurado
            ruta_remota = tsol_conn.get("ruta_remota", "/")
            if ruta_remota and ruta_remota != "/":
                try:
                    ftp.cwd(ruta_remota)
                except ftplib.error_perm:
                    logger.warning(
                        "No se pudo acceder al directorio remoto: %s", ruta_remota
                    )

            # Subir archivo
            remote_filename = os.path.basename(zip_path)
            file_size = os.path.getsize(zip_path) / 1024 / 1024
            logger.info(
                "Subiendo: %s (%.2f MB)", remote_filename, file_size
            )

            with open(zip_path, "rb") as f:
                ftp.storbinary(f"STOR {remote_filename}", f, blocksize=262144)

            logger.info("Archivo subido correctamente.")
            ftp.quit()
            return True

        except Exception as exc:
            logger.error("Error FTP: %s: %s", exc.__class__.__name__, exc)
            return False

    # ── Orquestador principal ──────────────────────────────────────

    def procesar(self) -> dict:
        """
        Ejecuta el pipeline completo:
        1. Extrae datos de BD
        2. Filtra por proveedor y sede
        3. Genera 11 archivos TXT
        4. Comprime a ZIP
        5. Envía por FTP (opcional)

        Returns:
            dict con resultados: success, archivos, totales, zip_path, etc.
        """
        resultado = {
            "success": False,
            "archivos": [],
            "totales": {},
            "zip_path": None,
            "enviado_ftp": False,
            "log": "",
        }

        try:
            # 1. Extraer datos
            self.extraer_todos()

            # 2. Filtrar
            self.filtrar_por_proveedor()
            self.filtrar_por_sedes()

            # 3. Generar archivos
            generadores = [
                ("ventas", self.generar_ventas),
                ("productos", self.generar_productos),
                ("clientes", self.generar_clientes),
                ("vendedores", self.generar_vendedores),
                ("supervisores", self.generar_supervisores),
                ("inventario", self.generar_inventario),
                ("tipos_negocio", self.generar_tipos_negocio),
                ("municipios", self.generar_municipios),
                ("rutas", self.generar_rutas),
                ("facturas", self.generar_listado_facturas),
                ("totales_control", self.generar_totales_control),
            ]

            archivos_generados = []
            for nombre, generador in generadores:
                try:
                    filepath = generador()
                    if filepath:
                        size = os.path.getsize(filepath)
                        archivos_generados.append({
                            "nombre": os.path.basename(filepath),
                            "tamaño": size,
                        })
                except Exception as exc:
                    logger.error("Error generando %s: %s", nombre, exc)

            resultado["archivos"] = archivos_generados

            # Totales
            resultado["totales"] = {
                "ventas": len(self.datos.get("ventas_tsol", pd.DataFrame())),
                "clientes": len(self.datos.get("clientes_tsol", pd.DataFrame())),
                "productos": len(self.datos.get("productos_tsol", pd.DataFrame())),
                "vendedores": len(self.datos.get("vendedores_tsol", pd.DataFrame())),
                "inventario": len(self.datos.get("inventario_tsol", pd.DataFrame())),
            }

            # 4. ZIP
            if archivos_generados:
                zip_path = self.generar_zip()
                resultado["zip_path"] = zip_path

                # 5. FTP
                if self.enviar_ftp:
                    resultado["enviado_ftp"] = self.enviar_por_ftp(zip_path)

            resultado["success"] = True
            logger.info("Procesamiento TSOL completado exitosamente.")

        except Exception as exc:
            logger.error("Error en procesamiento TSOL: %s", exc)
            resultado["error"] = str(exc)

        finally:
            resultado["log"] = self._log_buffer.getvalue()
            logger.removeHandler(self._log_handler)

        return resultado
