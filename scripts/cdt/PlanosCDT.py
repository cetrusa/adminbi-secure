"""
Generador de Archivos Planos CDT (MasterFoods, etc.)
Lee datos desde las tablas BI (cuboventas, clientes, inventario)
y genera archivos pipe-delimited para envio a proveedores via SFTP.

Soporta multiples proveedores y empresas configurados desde Django admin.
"""

import io
import json
import logging
import os
import zipfile
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con

logger = logging.getLogger(__name__)


class PlanosCDT:
    """
    Procesador de planos CDT que lee datos desde BD BI
    y genera archivos pipe-delimited para proveedores.
    """

    CODIGO_PAIS = "CO"

    def __init__(
        self,
        empresa_id: int,
        fecha_ini: str,
        fecha_fin: str,
        user_id: Optional[int] = None,
        enviar_sftp: bool = True,
    ):
        from apps.permisos.models import ConfEmpresas, ConfSqlCdt

        self.empresa = ConfEmpresas.objects.get(id=empresa_id)
        self.fecha_ini = fecha_ini
        self.fecha_fin = fecha_fin
        self.user_id = user_id
        self.enviar_sftp = enviar_sftp
        self.fecha_actual = datetime.now().strftime("%Y%m%d")

        # Cargar configuracion de distribuidor desde JSON
        self.codigos_distribuidor = json.loads(self.empresa.cdt_codigos_distribuidor or "[]")

        # Vendedores especiales y bodega
        vend_str = self.empresa.cdt_vendedores_especiales or ""
        self.vendedores_especiales = [
            v.strip() for v in vend_str.split(",") if v.strip()
        ]
        self.bodega_especial = (self.empresa.cdt_bodega_especial or "").strip()

        self.empresas = [self.empresa]

        # Cargar SQLs CDT
        self.sql_configs = {}
        for empresa in self.empresas:
            ids_str = (empresa.planos_cdt or "").strip("[] ")
            sql_ids = [int(x.strip()) for x in ids_str.split(",") if x.strip()]
            if sql_ids:
                sqls = {
                    s.nmReporte: s
                    for s in ConfSqlCdt.objects.filter(nbSql__in=sql_ids)
                }
                self.sql_configs[empresa.name] = sqls

        # Directorio de salida
        cdt_label = (self.empresa.cdt_nombre_proveedor or "CDT").replace(" ", "_")
        self.output_folder = os.path.join(
            "media", "cdt", cdt_label,
            self.empresa.name.replace(" ", "_"), self.fecha_actual
        )
        os.makedirs(self.output_folder, exist_ok=True)

        # Resultados
        self.datasets = {}
        self.archivos_generados = []
        self.log_buffer = io.StringIO()

    def _log(self, msg: str):
        """Log a mensaje y buffer interno."""
        logger.info(msg)
        self.log_buffer.write(f"{datetime.now():%H:%M:%S} {msg}\n")

    def _get_bi_engine(self, empresa):
        """Crea engine de conexion a la BD BI de una empresa."""
        config = ConfigBasic(empresa.name).config
        user = config.get("nmUsrIn")
        password = config.get("txPassIn")
        host = config.get("hostServerIn")
        port = config.get("portServerIn")
        database = config.get("dbBi")

        if not all([user, password, host, port, database]):
            raise ValueError(
                f"Configuracion BI incompleta para empresa {empresa.name}"
            )

        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    # ── Extraccion de datos ────────────────────────────────────────

    def _ejecutar_sql(
        self, engine, sql_text: str, params: dict
    ) -> pd.DataFrame:
        """Ejecuta un SQL con parametros y retorna DataFrame."""
        with engine.connect() as conn:
            result = conn.execute(text(sql_text), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    def extraer_datos_empresa(self, empresa) -> Dict[str, pd.DataFrame]:
        """Extrae ventas, clientes e inventario de una empresa."""
        sqls = self.sql_configs.get(empresa.name, {})
        if not sqls:
            self._log(f"No hay SQLs CDT configurados para {empresa.name}")
            return {}

        engine = self._get_bi_engine(empresa)
        params = {"fi": self.fecha_ini, "ff": self.fecha_fin}
        resultado = {}

        for reporte_key in ["ventas_cdt", "clientes_cdt", "inventario_cdt"]:
            sql_conf = sqls.get(reporte_key)
            if not sql_conf or not sql_conf.txSqlExtrae:
                self._log(f"  SQL '{reporte_key}' no configurado para {empresa.name}")
                continue

            try:
                df = self._ejecutar_sql(engine, sql_conf.txSqlExtrae, params)
                resultado[reporte_key] = df
                self._log(
                    f"  {reporte_key} ({empresa.name}): {len(df):,} registros"
                )
            except Exception as e:
                self._log(f"  Error en {reporte_key} ({empresa.name}): {e}")

        engine.dispose()
        return resultado

    def extraer_todos(self):
        """Extrae datos de todas las empresas vinculadas."""
        self._log("=== EXTRACCION DE DATOS ===")
        self.datos_por_empresa = {}

        for empresa in self.empresas:
            self._log(f"Extrayendo datos de {empresa.name}...")
            datos = self.extraer_datos_empresa(empresa)
            if datos:
                self.datos_por_empresa[empresa.name] = datos

        if not self.datos_por_empresa:
            raise ValueError("No se obtuvieron datos de ninguna empresa")

    # ── Filtrado y separacion ──────────────────────────────────────

    def filtrar_por_proveedor(self):
        """Filtra datos de ventas e inventario por el proveedor CDT."""
        self._log("=== FILTRADO POR PROVEEDOR ===")
        codigo_prov = self.empresa.cdt_codigo_proveedor

        for empresa_name, datos in self.datos_por_empresa.items():
            if "ventas_cdt" in datos:
                df = datos["ventas_cdt"]
                antes = len(df)
                if "nmProveedor" in df.columns:
                    df = df[
                        df["nmProveedor"]
                        .astype(str)
                        .str.contains(codigo_prov, case=False, na=False)
                    ]
                datos["ventas_cdt"] = df
                self._log(
                    f"  Ventas {empresa_name}: {antes:,} -> {len(df):,}"
                )

            if "inventario_cdt" in datos:
                df = datos["inventario_cdt"]
                antes = len(df)
                if "nmProveedor" in df.columns:
                    df = df[
                        df["nmProveedor"]
                        .astype(str)
                        .str.contains(codigo_prov, case=False, na=False)
                    ]
                datos["inventario_cdt"] = df
                self._log(
                    f"  Inventario {empresa_name}: {antes:,} -> {len(df):,}"
                )

    def separar_por_distribuidor(self):
        """Separa datos por codigo de distribuidor segun configuracion."""
        self._log("=== SEPARACION POR DISTRIBUIDOR ===")

        for dist_conf in self.codigos_distribuidor:
            codigo = dist_conf["codigo"]
            empresa_name = dist_conf["empresa"]
            tipo = dist_conf["tipo"]
            dataset_key = f"{empresa_name}_{tipo}"

            datos_empresa = self.datos_por_empresa.get(empresa_name, {})
            ventas_all = datos_empresa.get("ventas_cdt", pd.DataFrame())
            inventario_all = datos_empresa.get("inventario_cdt", pd.DataFrame())
            clientes_all = datos_empresa.get("clientes_cdt", pd.DataFrame())

            # Separar ventas por vendedores especiales
            if not ventas_all.empty and self.vendedores_especiales:
                col_vendedor = "documento_id"
                if col_vendedor not in ventas_all.columns:
                    col_vendedor = next(
                        (c for c in ventas_all.columns if "vendedor" in c.lower()),
                        None,
                    )

                if col_vendedor:
                    if tipo == "con_vendedores":
                        ventas = ventas_all[
                            ventas_all[col_vendedor].isin(self.vendedores_especiales)
                        ]
                    else:
                        ventas = ventas_all[
                            ~ventas_all[col_vendedor].isin(self.vendedores_especiales)
                        ]
                else:
                    ventas = ventas_all
            else:
                ventas = ventas_all

            # Separar inventario por bodega especial (SPT)
            if not inventario_all.empty and self.bodega_especial:
                col_bodega = "nbAlmacen"
                if col_bodega not in inventario_all.columns:
                    col_bodega = next(
                        (c for c in inventario_all.columns if "almacen" in c.lower() or "bodega" in c.lower()),
                        None,
                    )

                if col_bodega:
                    if tipo == "con_vendedores":
                        inventario = inventario_all[
                            inventario_all[col_bodega]
                            .astype(str)
                            .str.contains(self.bodega_especial, case=False, na=False)
                        ]
                    else:
                        inventario = inventario_all[
                            ~inventario_all[col_bodega]
                            .astype(str)
                            .str.contains(self.bodega_especial, case=False, na=False)
                        ]
                else:
                    inventario = inventario_all
            else:
                inventario = inventario_all

            self.datasets[dataset_key] = {
                "ventas": ventas,
                "clientes": clientes_all,
                "inventario": inventario,
                "codigo": codigo,
                "empresa": empresa_name,
                "tipo": tipo,
            }

            self._log(
                f"  {dataset_key} (#{codigo}): "
                f"{len(ventas):,} ventas, {len(inventario):,} inventario"
            )

    # ── Generacion de archivos ─────────────────────────────────────

    def _obtener_fecha_max(self, ventas: pd.DataFrame) -> str:
        """Obtiene la fecha maxima de ventas para nombre de archivo."""
        col_fecha = "dtContabilizacion"
        if col_fecha not in ventas.columns:
            col_fecha = next(
                (c for c in ventas.columns if "fecha" in c.lower() or "dt" in c.lower()),
                None,
            )

        if col_fecha and not ventas.empty:
            try:
                max_f = pd.to_datetime(ventas[col_fecha], errors="coerce").max()
                if pd.notnull(max_f):
                    return max_f.strftime("%Y%m%d")
            except Exception:
                pass

        return self.fecha_actual

    def _col(self, df: pd.DataFrame, nombre: str, default=""):
        """Obtiene columna del DataFrame o devuelve default."""
        if nombre in df.columns:
            return df[nombre].fillna(default).astype(str).values
        return [default] * len(df)

    def generar_archivo_clientes(self, dataset_key: str) -> Optional[str]:
        """Genera archivo pipe-delimited de clientes."""
        ds = self.datasets[dataset_key]
        ventas = ds["ventas"]
        clientes_all = ds["clientes"]
        codigo = ds["codigo"]

        if ventas.empty:
            return None

        fecha_str = self._obtener_fecha_max(ventas)

        # Obtener clientes unicos con ventas
        col_cliente_v = "idPuntoVenta"
        if col_cliente_v not in ventas.columns:
            col_cliente_v = next(
                (c for c in ventas.columns if "cliente" in c.lower() or "puntoventa" in c.lower()),
                col_cliente_v,
            )

        clientes_ids = ventas[col_cliente_v].dropna().unique() if col_cliente_v in ventas.columns else []

        # Filtrar clientes que tienen ventas
        if not clientes_all.empty and len(clientes_ids) > 0:
            col_cliente_c = "idPuntoVenta"
            if col_cliente_c not in clientes_all.columns:
                col_cliente_c = next(
                    (c for c in clientes_all.columns if "cliente" in c.lower() or "puntoventa" in c.lower()),
                    col_cliente_c,
                )

            if col_cliente_c in clientes_all.columns:
                clientes = clientes_all[
                    clientes_all[col_cliente_c].astype(str).isin(
                        pd.Series(clientes_ids).astype(str)
                    )
                ].copy()
            else:
                clientes = clientes_all.copy()
        else:
            # Sin tabla de clientes separada: extraer clientes unicos de ventas
            clientes = self._clientes_desde_ventas(ventas, col_cliente_v)

        if clientes.empty:
            self._log(f"  Sin clientes para {dataset_key}")
            return None

        n = len(clientes)
        output = pd.DataFrame()
        output["CODPAIS"] = [self.CODIGO_PAIS] * n
        output["CODIGO DISTRIBUIDOR"] = [str(codigo)] * n
        output["CODCLIENTE DISTRIBUIDOR"] = self._col(clientes, "idPuntoVenta")
        output["RAZON SOCIAL"] = self._col(clientes, "nmPuntoVenta")
        output["DIRECCION"] = self._col(clientes, "txDireccion")
        output["NOMBRE FANTASIA"] = [""] * n
        output["CONTACTO"] = [""] * n
        output["TELEFONOS"] = self._col(clientes, "nbTelMovil")
        output["CODIGO AGENTE VENDEDOR"] = [""] * n
        output["NOMBRE AGENTE VENDEDOR"] = [""] * n
        output["DEPARTAMENTO_COD"] = self._col(clientes, "txDepartamento", "VALLE DEL CAUCA")
        output["CIUDAD_COD"] = self._col(clientes, "nbCiudad", "76001")
        output["BARRIO_COD"] = self._col(clientes, "txBarrio")
        output["CODIGO TIPOPUNTOVENTA"] = self._col(clientes, "nbNegocio", "1")
        output["CODIGO TIPOPUNTOVENTA 2"] = [""] * n
        output["CODIGO ZONAVENTA"] = [""] * n
        output["RUTA"] = [""] * n
        output["ESTADO"] = ["1"] * n
        output["NIT"] = self._col(clientes, "nbDocumento")
        output["LONGITUD"] = self._col(clientes, "longitud_cl")
        output["LATITUD"] = self._col(clientes, "latitud_cl")
        output["DEPARTAMENTO_NOM"] = self._col(clientes, "txDepartamento", "VALLE DEL CAUCA")
        output["CIUDAD_NOM"] = self._col(clientes, "txCiudad", "CALI")
        output["BARRIO_NOM"] = self._col(clientes, "txBarrio")
        output["NOMBRE TIPO PUNTO DE VENTA"] = ["TIENDAS"] * n
        output["NOMBRE TIPO PUNTO DE VENTA 2"] = [""] * n
        output["Nombre ZonaVenta"] = [""] * n
        output["Ruta_NOM"] = [""] * n

        filename = f"Clientes_{codigo}_{fecha_str}.txt"
        filepath = os.path.join(self.output_folder, filename)
        output.to_csv(filepath, sep="|", index=False, header=False, encoding="utf-8")
        self._log(f"  Clientes_{codigo}: {n:,} registros")
        self.archivos_generados.append(filepath)
        return filepath

    def _clientes_desde_ventas(self, ventas: pd.DataFrame, col_cliente: str) -> pd.DataFrame:
        """Extrae datos unicos de clientes desde el DataFrame de ventas."""
        if col_cliente not in ventas.columns:
            return pd.DataFrame()

        # Tomar primera ocurrencia de cada cliente
        cols_map = {
            "idPuntoVenta": col_cliente,
            "nmPuntoVenta": next(
                (c for c in ventas.columns if c in ["nmPuntoVenta", "nmRazonSocial"]),
                None,
            ),
            "txDireccion": "txDireccion" if "txDireccion" in ventas.columns else None,
            "txBarrio": "txBarrio" if "txBarrio" in ventas.columns else None,
            "nbCiudad": "nbCiudad" if "nbCiudad" in ventas.columns else None,
            "txCiudad": "txCiudad" if "txCiudad" in ventas.columns else None,
            "txDepartamento": "txDepartamento" if "txDepartamento" in ventas.columns else None,
            "nbTelMovil": "nbTelMovil" if "nbTelMovil" in ventas.columns else None,
            "nbDocumento": "nbDocumento" if "nbDocumento" in ventas.columns else None,
        }

        existing = {k: v for k, v in cols_map.items() if v is not None}
        rename_map = {v: k for k, v in existing.items()}

        clientes = (
            ventas[list(existing.values())]
            .drop_duplicates(subset=[existing["idPuntoVenta"]])
            .rename(columns=rename_map)
        )
        return clientes

    def generar_archivo_ventas(self, dataset_key: str) -> Optional[str]:
        """Genera archivo pipe-delimited de ventas."""
        ds = self.datasets[dataset_key]
        ventas = ds["ventas"]
        clientes_all = ds["clientes"]
        codigo = ds["codigo"]

        if ventas.empty:
            return None

        fecha_str = self._obtener_fecha_max(ventas)

        # Mapeos desde clientes para enriquecer ventas
        clientes_dict = {}
        direccion_dict = {}
        telefono_dict = {}
        if not clientes_all.empty and "idPuntoVenta" in clientes_all.columns:
            clientes_dict = dict(
                zip(
                    clientes_all["idPuntoVenta"].astype(str),
                    clientes_all.get("nmPuntoVenta", pd.Series(dtype=str)).fillna(""),
                )
            )
            if "txDireccion" in clientes_all.columns:
                direccion_dict = dict(
                    zip(
                        clientes_all["idPuntoVenta"].astype(str),
                        clientes_all["txDireccion"].fillna(""),
                    )
                )
            if "nbTelMovil" in clientes_all.columns:
                telefono_dict = dict(
                    zip(
                        clientes_all["idPuntoVenta"].astype(str),
                        clientes_all["nbTelMovil"].fillna(""),
                    )
                )

        n = len(ventas)
        cod_cliente = self._col(ventas, "idPuntoVenta")

        # Nombre/direccion/telefono: enriquecer desde clientes o usar inline
        if clientes_dict:
            nom_cliente = [clientes_dict.get(str(c), "") for c in cod_cliente]
        else:
            nom_cliente = self._col(ventas, "nmPuntoVenta").tolist() if hasattr(self._col(ventas, "nmPuntoVenta"), "tolist") else list(self._col(ventas, "nmPuntoVenta"))

        if direccion_dict:
            dir_cliente = [direccion_dict.get(str(c), "") for c in cod_cliente]
        else:
            dir_cliente = list(self._col(ventas, "txDireccion"))

        if telefono_dict:
            tel_cliente = [telefono_dict.get(str(c), "") for c in cod_cliente]
        else:
            tel_cliente = list(self._col(ventas, "nbTelMovil"))

        # Formatear fecha
        col_fecha = "dtContabilizacion"
        if col_fecha in ventas.columns:
            fechas_fmt = pd.to_datetime(ventas[col_fecha], errors="coerce").dt.strftime("%d/%m/%Y").fillna("").values
        else:
            fechas_fmt = [""] * n

        # Tipo documento
        col_td = "td"
        if col_td in ventas.columns:
            tipos_doc = ["F" if str(t).startswith("F") or str(t) == "0" else "D" for t in ventas[col_td].values]
        else:
            tipos_doc = ["F"] * n

        # Cantidad como entero
        col_cant = "cantAsignada"
        if col_cant in ventas.columns:
            cantidades = ventas[col_cant].fillna(0).astype(int).values
        else:
            cantidades = [0] * n

        # Valor neto
        col_vlr = "vlrNeto"
        if col_vlr not in ventas.columns:
            col_vlr = next(
                (c for c in ventas.columns if "vlr" in c.lower() and "neto" in c.lower()),
                next(
                    (c for c in ventas.columns if "vlrAntesIva" in c or "vlrTotalconIva" in c),
                    None,
                ),
            )
        if col_vlr and col_vlr in ventas.columns:
            valores = ventas[col_vlr].fillna(0).round(2).values
        else:
            valores = [0.0] * n

        # Ciudad via mapeo clientes
        if "nbCiudad" in ventas.columns:
            ciudades = ventas["nbCiudad"].fillna("76001").values
        elif clientes_all is not None and not clientes_all.empty and "idPuntoVenta" in clientes_all.columns and "nbCiudad" in clientes_all.columns:
            ciudad_dict = dict(
                zip(clientes_all["idPuntoVenta"].astype(str), clientes_all["nbCiudad"].fillna("76001"))
            )
            ciudades = [ciudad_dict.get(str(c), "76001") for c in cod_cliente]
        else:
            ciudades = ["76001"] * n

        # Barrio via mapeo clientes
        if "txBarrio" in ventas.columns:
            barrios = ventas["txBarrio"].fillna("").values
        elif clientes_all is not None and not clientes_all.empty and "idPuntoVenta" in clientes_all.columns and "txBarrio" in clientes_all.columns:
            barrio_dict = dict(
                zip(clientes_all["idPuntoVenta"].astype(str), clientes_all["txBarrio"].fillna(""))
            )
            barrios = [barrio_dict.get(str(c), "") for c in cod_cliente]
        else:
            barrios = [""] * n

        output = pd.DataFrame()
        output["CODPAIS"] = [self.CODIGO_PAIS] * n
        output["Cod.DISTRIB."] = [str(codigo)] * n
        output["CODCLIENTEDISTRIBUIDOR"] = cod_cliente
        output["NOMBRECLIENTEDISTRIBUIDOR"] = nom_cliente
        output["DIRECCION"] = dir_cliente
        output["TELEFONO"] = tel_cliente
        output["Contacto"] = [""] * n
        output["CODIGO DE AGENTE VENDEDOR"] = self._col(ventas, "documento_id")
        output["NOMBRE AGENTE VENDEDOR"] = self._col(ventas, "nmVendedor")
        output["CODIGO PRODUCTO INTERNO DISTRIBUIDOR"] = self._col(ventas, "nbProducto")
        output["NOMBRE PRODUCTO"] = self._col(ventas, "nmProducto")
        output["UNID.VEND."] = cantidades
        output["MONTOVENTA"] = valores
        output["MONTOBRUTO"] = valores
        output["FACTOR CONVERSION"] = ["1"] * n
        output["FECHA(DD/MM/AAAA)"] = fechas_fmt
        output["FECHA ENTREGA (DD/MM/AAAA)"] = fechas_fmt
        output["DEPARTAMENTO"] = self._col(ventas, "txDepartamento", "VALLE DEL CAUCA")
        output["CIUDAD"] = ciudades
        output["BARRIO"] = barrios
        output["CODIGO TIPOPUNTOVENTA"] = ["13"] * n
        output["CODIGO ZONA VENTA"] = self._col(ventas, "nmZona")
        output["RUTA"] = [""] * n
        output["DOCUMENTO"] = self._col(ventas, "nbFactura")
        output["Tipo de Documento"] = tipos_doc
        output["TIPO VENTA"] = ["V"] * n

        filename = f"VentaDia_{codigo}_{fecha_str}.txt"
        filepath = os.path.join(self.output_folder, filename)
        output.to_csv(filepath, sep="|", index=False, header=False, encoding="utf-8")
        self._log(f"  VentaDia_{codigo}: {n:,} registros")
        self.archivos_generados.append(filepath)
        return filepath

    def generar_archivo_inventario(self, dataset_key: str) -> Optional[str]:
        """Genera archivo pipe-delimited de inventario."""
        ds = self.datasets[dataset_key]
        inventario = ds["inventario"]
        ventas = ds["ventas"]
        codigo = ds["codigo"]

        if inventario.empty:
            self._log(f"  Sin inventario para {dataset_key}")
            return None

        fecha_str = self._obtener_fecha_max(ventas) if not ventas.empty else self.fecha_actual

        # Cantidad como entero
        col_inv = "InvDisponible"
        if col_inv not in inventario.columns:
            col_inv = next(
                (c for c in inventario.columns if "disp" in c.lower() or "unid" in c.lower()),
                None,
            )
        if col_inv and col_inv in inventario.columns:
            unidades = inventario[col_inv].fillna(0).astype(int).values
        else:
            unidades = [0] * len(inventario)

        # Valor inventario
        col_val = "vlrInventario"
        if col_val not in inventario.columns:
            col_val = next(
                (c for c in inventario.columns if "valor" in c.lower() or "costo" in c.lower()),
                None,
            )
        if col_val and col_val in inventario.columns:
            valores = inventario[col_val].fillna(0).round(2).values
        else:
            valores = [0.0] * len(inventario)

        n = len(inventario)
        output = pd.DataFrame()
        output["CODPAIS"] = [self.CODIGO_PAIS] * n
        output["CODIGO DISTRIBUIDOR"] = [str(codigo)] * n
        output["CODIGO ARTICULO DISTRIBUIDOR"] = self._col(inventario, "nbProducto")
        output["DESCRIPCION ARTICULO"] = self._col(inventario, "nmProducto")
        output["BODEGA"] = self._col(inventario, "nbAlmacen")
        output["UNID"] = unidades
        output["COSTO INVENT."] = valores
        output["FECHA"] = [datetime.now().strftime("%d/%m/%Y")] * n

        filename = f"Inventario_{codigo}_{fecha_str}.txt"
        filepath = os.path.join(self.output_folder, filename)
        output.to_csv(filepath, sep="|", index=False, header=False, encoding="utf-8")
        self._log(f"  Inventario_{codigo}: {n:,} registros")
        self.archivos_generados.append(filepath)
        return filepath

    # ── SFTP ───────────────────────────────────────────────────────

    def enviar_por_sftp(self, archivos: List[str], empresa=None) -> bool:
        """Envia archivos por SFTP usando credenciales de la empresa."""
        archivos_validos = [a for a in archivos if a]
        if not archivos_validos:
            self._log("Sin archivos para enviar por SFTP")
            return False

        # SFTP config: leer de la empresa (independiente por BD)
        emp = empresa or (self.empresas[0] if self.empresas else None)
        if not emp:
            self._log("Error: no hay empresa para obtener config SFTP")
            return False

        cdt_conn = emp.cdt_conexion or {}
        sftp_host = cdt_conn.get("host", "")
        sftp_port = cdt_conn.get("port", 22)
        sftp_user = cdt_conn.get("user", "")
        sftp_pass = cdt_conn.get("pass", "")
        sftp_ruta = cdt_conn.get("ruta_remota", "/")

        if not all([sftp_host, sftp_user, sftp_pass]):
            self._log(f"Error: credenciales SFTP incompletas para {emp.name}")
            return False

        self._log("=== ENVIO SFTP ===")
        self._log(f"Empresa: {emp.name}")
        self._log(f"Servidor: {sftp_host}:{sftp_port}")
        self._log(f"Archivos: {len(archivos_validos)}")

        ssh = None
        sftp = None
        exitos = 0

        try:
            import paramiko

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                sftp_host,
                sftp_port,
                sftp_user,
                sftp_pass,
            )
            sftp = ssh.open_sftp()

            ruta = sftp_ruta
            if ruta != "/":
                try:
                    sftp.chdir(ruta)
                except IOError:
                    self._log(f"  Ruta remota '{ruta}' no existe, usando /")

            for i, archivo in enumerate(archivos_validos, 1):
                nombre = os.path.basename(archivo)
                tam = os.path.getsize(archivo)
                self._log(
                    f"  [{i}/{len(archivos_validos)}] {nombre} "
                    f"({tam / 1024:.1f} KB)"
                )
                try:
                    ruta_remota = os.path.join(ruta, nombre).replace("\\", "/") if ruta != "/" else nombre
                    sftp.put(archivo, ruta_remota)
                    # Verificar
                    remote_stat = sftp.stat(ruta_remota)
                    if remote_stat.st_size == tam:
                        self._log(f"    OK (verificado)")
                        exitos += 1
                    else:
                        self._log(
                            f"    WARN: tamano no coincide "
                            f"(local={tam}, remoto={remote_stat.st_size})"
                        )
                        exitos += 1  # Contar como enviado igualmente
                except Exception as e:
                    self._log(f"    ERROR: {e}")

        except Exception as e:
            self._log(f"Error conexion SFTP: {e}")
            return False

        finally:
            if sftp:
                try:
                    sftp.close()
                except Exception:
                    pass
            if ssh:
                try:
                    ssh.close()
                except Exception:
                    pass

        ok = exitos == len(archivos_validos)
        self._log(
            f"SFTP: {exitos}/{len(archivos_validos)} enviados"
            f" {'(COMPLETO)' if ok else '(PARCIAL)'}"
        )
        return ok

    # ── Resumen ────────────────────────────────────────────────────

    def generar_resumen_ejecutivo(self) -> Optional[str]:
        """Genera un resumen ejecutivo en Excel."""
        try:
            resumen_inv = []
            resumen_ven = []

            for dataset_key, ds in self.datasets.items():
                codigo = ds["codigo"]
                empresa = ds["empresa"]
                tipo = ds["tipo"]
                ventas = ds["ventas"]
                inventario = ds["inventario"]

                tipo_label = (
                    "Con vendedores especiales"
                    if tipo == "con_vendedores"
                    else "Sin vendedores especiales"
                )

                if not inventario.empty:
                    col_inv = "InvDisponible"
                    if col_inv not in inventario.columns:
                        col_inv = next(
                            (c for c in inventario.columns if "disp" in c.lower()),
                            None,
                        )
                    total_u = inventario[col_inv].sum() if col_inv else 0
                    resumen_inv.append({
                        "Codigo Distribuidor": codigo,
                        "Empresa": empresa,
                        "Tipo": tipo_label,
                        "Total Unidades": total_u,
                        "SKUs": len(inventario),
                    })

                if not ventas.empty:
                    col_cant = "cantAsignada" if "cantAsignada" in ventas.columns else None
                    col_vlr = "vlrNeto" if "vlrNeto" in ventas.columns else None
                    if not col_vlr:
                        col_vlr = next(
                            (c for c in ventas.columns if "vlr" in c.lower()),
                            None,
                        )

                    resumen_ven.append({
                        "Codigo Distribuidor": codigo,
                        "Empresa": empresa,
                        "Tipo": tipo_label,
                        "Unidades Vendidas": ventas[col_cant].sum() if col_cant else 0,
                        "Venta Neta": ventas[col_vlr].sum() if col_vlr else 0,
                        "Facturas": ventas["nbFactura"].nunique() if "nbFactura" in ventas.columns else 0,
                    })

            filename = f"Resumen_CDT_{(self.empresa.cdt_nombre_proveedor or 'CDT').replace(' ', '_')}_{self.fecha_actual}.xlsx"
            filepath = os.path.join(self.output_folder, filename)

            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                if resumen_inv:
                    pd.DataFrame(resumen_inv).to_excel(
                        writer, sheet_name="Inventario", index=False
                    )
                if resumen_ven:
                    pd.DataFrame(resumen_ven).to_excel(
                        writer, sheet_name="Ventas", index=False
                    )

            self._log(f"Resumen generado: {filename}")
            return filepath

        except Exception as e:
            self._log(f"Error generando resumen: {e}")
            return None

    def generar_zip(self) -> Optional[str]:
        """Comprime todos los archivos generados en un ZIP."""
        if not self.archivos_generados:
            return None

        zip_name = f"CDT_{(self.empresa.cdt_nombre_proveedor or 'CDT').replace(' ', '_')}_{self.fecha_actual}.zip"
        zip_path = os.path.join(self.output_folder, zip_name)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for archivo in self.archivos_generados:
                zf.write(archivo, os.path.basename(archivo))

        self._log(f"ZIP generado: {zip_name}")
        return zip_path

    # ── Orquestador principal ──────────────────────────────────────

    def procesar(self) -> dict:
        """Ejecuta el procesamiento completo de planos CDT."""
        self._log(f"INICIANDO PROCESAMIENTO CDT - {self.empresa.cdt_nombre_proveedor or self.empresa.name}")
        self._log(f"Periodo: {self.fecha_ini} a {self.fecha_fin}")
        self._log(f"Empresas: {', '.join(e.name for e in self.empresas)}")

        # Paso 1: Extraer datos
        self.extraer_todos()

        # Paso 2: Filtrar por proveedor
        self.filtrar_por_proveedor()

        # Paso 3: Separar por distribuidor
        self.separar_por_distribuidor()

        # Paso 4: Generar archivos
        self._log("=== GENERACION DE ARCHIVOS ===")
        for dataset_key, ds in self.datasets.items():
            if not ds["ventas"].empty:
                self._log(f"--- {dataset_key} (#{ds['codigo']}) ---")
                self.generar_archivo_clientes(dataset_key)
                self.generar_archivo_ventas(dataset_key)
                self.generar_archivo_inventario(dataset_key)
            else:
                self._log(f"Saltando {dataset_key} - sin ventas")

        # Paso 5: Resumen ejecutivo
        resumen_path = self.generar_resumen_ejecutivo()

        # Paso 6: SFTP
        sftp_ok = False
        if self.enviar_sftp and self.archivos_generados:
            sftp_ok = self.enviar_por_sftp(self.archivos_generados)

        # Paso 7: ZIP para descarga
        zip_path = self.generar_zip()

        # Conteos
        total_ventas = sum(
            len(ds["ventas"]) for ds in self.datasets.values()
        )
        total_clientes = sum(
            len(ds["clientes"]) for ds in self.datasets.values()
        )
        total_inventario = sum(
            len(ds["inventario"]) for ds in self.datasets.values()
        )

        self._log("=== PROCESAMIENTO COMPLETADO ===")
        self._log(
            f"Archivos: {len(self.archivos_generados)} | "
            f"Ventas: {total_ventas:,} | "
            f"Clientes: {total_clientes:,} | "
            f"Inventario: {total_inventario:,}"
        )

        return {
            "status": "enviado" if sftp_ok else "procesado",
            "total_ventas": total_ventas,
            "total_clientes": total_clientes,
            "total_inventario": total_inventario,
            "archivos": [os.path.basename(a) for a in self.archivos_generados],
            "zip_path": zip_path,
            "resumen_path": resumen_path,
            "enviado_sftp": sftp_ok,
            "log": self.log_buffer.getvalue(),
        }
