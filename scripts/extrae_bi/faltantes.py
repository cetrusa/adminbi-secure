import os
import time
import pandas as pd
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict, Any, Optional

from scripts.config import ConfigBasic
from scripts.conexion import Conexion

# Configuración de Logs
logger = logging.getLogger(__name__)

class FaltantesReport:
    """Ejecuta el SP dinámico de Faltantes cruzando con productos_bimbo."""

    PROCEDURE_NAME = "sp_reporte_faltantes"
    PROCEDURE_PARAMS = [
        "p_ceve",
        "p_tipo_filtro",
        "p_codigo_producto",
        "p_categoria",
        "p_subcategoria",
        "p_fecha_ini",
        "p_fecha_fin",
    ]
    DEFAULT_CHUNK_SIZE = 5000
    ALLOWED_FILTER_TYPES = {"producto", "proveedor", "categoria", "subcategoria"}

    def __init__(
        self,
        database_name: str,
        ceves_code: str,
        fecha_ini: str,
        fecha_fin: str,
        filter_type: str = "proveedor",
        filter_value: str = "",
        extra_params: Optional[Dict[str, str]] = None,
        user_id: int = None,
        progress_callback=None,
        chunk_size: int = None,
    ):
        self.ceves_code = ceves_code
        self.fecha_ini = fecha_ini
        self.fecha_fin = fecha_fin
        self.filter_type = (filter_type or "").strip().lower()
        self.filter_value = (filter_value or "").strip()
        self.extra_params = extra_params or {}
        self.category_value = (self.extra_params.get("category_value") or "").strip()
        self.family_value = (self.extra_params.get("family_value") or "").strip()
        self.database_name = database_name
        self.user_id = user_id
        self.progress_callback = progress_callback
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE

        self.engine_mysql: Optional[Engine] = None
        self.file_path: Optional[str] = None
        self.file_name: Optional[str] = None
        self.total_records = 0
        self.start_time = time.time()

    def _update_progress(self, stage: str, progress_percent: int) -> None:
        if self.progress_callback:
            safe_value = max(0, min(100, int(progress_percent)))
            try:
                self.progress_callback(stage, safe_value, self.total_records, None)
            except Exception as exc:
                logger.warning("No se pudo reportar progreso: %s", exc)

    def _validate_inputs(self) -> None:
        if not self.ceves_code:
            raise ValueError("El agente (CEVES) es obligatorio")
        if not self.database_name:
            raise ValueError("La base de datos es obligatoria")
        if not self.fecha_ini or not self.fecha_fin:
            raise ValueError("El rango de fechas es obligatorio")
        if self.fecha_ini > self.fecha_fin:
            raise ValueError("La fecha inicial no puede ser mayor que la final")
        if not self.filter_type or self.filter_type not in self.ALLOWED_FILTER_TYPES:
            raise ValueError("Tipo de filtro no permitido para Faltantes")
        if self.filter_type != "proveedor" and not self.filter_value:
            raise ValueError("El filtro seleccionado requiere valor")

    def _configure_connection(self) -> None:
        config_basic = ConfigBasic(self.database_name, self.user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuración de conexión incompleta para Faltantes")

        # Abrir conexión MySQL/MariaDB usando los credenciales de ConfigBasic
        self.engine_mysql = Conexion.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _build_call(self) -> text:
        placeholders = ", ".join(f":{p}" for p in self.PROCEDURE_PARAMS)
        call_sql = f"CALL {self.PROCEDURE_NAME}({placeholders})"
        return text(call_sql)

    def _calculate_dashboard_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calcula KPIs y Matriz alineado a patrón Venta Cero."""
        if df.empty:
            return {
                "kpis": {
                    "total_valor_faltante": 0,
                    "total_und_faltante": 0,
                    "total_und_pedidas": 0,
                    "porcentaje_nivel_servicio": 100.0,
                    "top_producto": "-"
                },
                "matrix": []
            }
        
        # Columnas esperadas del SP Refactorizado:
        # fecha, zona, cliente, cod_producto, nom_producto, categoria, valor_unitario, cant_pedida, cant_faltante, valor_faltante

        # Calcular Totales KPI
        total_valor_faltante = df['valor_faltante'].sum() if 'valor_faltante' in df.columns else 0
        total_und_faltante = df['cant_faltante'].sum() if 'cant_faltante' in df.columns else 0
        total_und_pedidas = df['cant_pedida'].sum() if 'cant_pedida' in df.columns else 0
        
        # Nivel de Servicio (en Unidades)
        ns_pct = 100.0
        if total_und_pedidas > 0:
            ns_pct = ((total_und_pedidas - total_und_faltante) / total_und_pedidas) * 100

        # Top Producto Faltante (en Dinero)
        top_prod = "-"
        if 'nom_producto' in df.columns and 'valor_faltante' in df.columns:
            # Usar Nombre Producto si existe, sino Codigo
            prod_col = 'nom_producto' if df['nom_producto'].notnull().any() else 'cod_producto'
            top_series = df.groupby(prod_col)['valor_faltante'].sum().sort_values(ascending=False)
            if not top_series.empty:
                top_name = str(top_series.index[0])
                if len(top_name) > 30: top_name = top_name[:27] + "..."
                top_prod = f"{top_name} (${top_series.iloc[0]:,.0f})"

        # Preparar datos de Matriz
        matrix_df = df.copy()
        
        # Formatear fecha para display
        if 'fecha' in matrix_df.columns:
             matrix_df['fecha'] = pd.to_datetime(matrix_df['fecha'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Rellenar nulos
        matrix_df.fillna({'nom_producto': 'Desconocido', 'categoria': '-'}, inplace=True)
        matrix_df = matrix_df.where(pd.notnull(matrix_df), 0)
        
        matrix_data = matrix_df.to_dict(orient='records')

        return {
            "kpis": {
                "total_valor_faltante": float(total_valor_faltante),
                "total_und_faltante": int(total_und_faltante),
                "total_und_pedidas": int(total_und_pedidas),
                "porcentaje_nivel_servicio": float(ns_pct),
                "top_producto": top_prod
            },
            "matrix": matrix_data
        }

    def _run_to_excel(self, query: text) -> pd.DataFrame:
        assert self.engine_mysql is not None
        os.makedirs("media", exist_ok=True)
        date_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        self.file_name = f"faltantes_{self.ceves_code}_{date_str}.xlsx"
        self.file_path = os.path.join("media", self.file_name)

        tipo_filtro = (self.filter_type or "proveedor").upper()
        value_map = {
            "p_ceve": int(self.ceves_code) if str(self.ceves_code).isdigit() else self.ceves_code,
            "p_tipo_filtro": tipo_filtro,
            "p_codigo_producto": self.filter_value if tipo_filtro == "PRODUCTO" else "",
            "p_categoria": self.filter_value if tipo_filtro in ("CATEGORIA", "PROVEEDOR") else None,
            "p_subcategoria": self.filter_value if tipo_filtro == "SUBCATEGORIA" else None,
            "p_fecha_ini": self.fecha_ini,
            "p_fecha_fin": self.fecha_fin,
        }
        if tipo_filtro == "SUBCATEGORIA":
            # Permite encadenar categoría opcional si se envía desde el frontend
            value_map["p_categoria"] = self.extra_params.get("category_value") or None
        value_map.update({k: v for k, v in self.extra_params.items() if v is not None})
        params = {str(p): value_map.get(str(p), "") for p in self.PROCEDURE_PARAMS}

        self._update_progress("Consultando base de datos...", 10)

        try:
            with self.engine_mysql.connect() as conn:
                result = conn.execute(query, params)
                rows = result.fetchall()
                if not rows:
                    self._update_progress("No se encontraron registros", 100)
                    df = pd.DataFrame()
                else:
                    self.total_records = len(rows)
                    self._update_progress(f"Procesando {self.total_records} registros...", 30)
                    df = pd.DataFrame(rows, columns=result.keys())

                if not df.empty:
                    self._update_progress("Generando Excel...", 70)
                    df.to_excel(self.file_path, index=False)

                return df

        except Exception as e:
            logger.error(f"Error ejecutando SP Faltantes: {e}")
            raise

    def execute(self) -> Dict[str, Any]:
        try:
            self._validate_inputs()
            self._update_progress("Conectando...", 5)
            self._configure_connection()
            
            query = self._build_call()
            df = self._run_to_excel(query)

            dashboard_data = self._calculate_dashboard_data(df)

            return {
                "success": True,
                "file_name": self.file_name,
                "file_path": self.file_path,
                "dashboard": dashboard_data,
                "metadata": {
                    "execution_time": time.time() - self.start_time,
                    "total_records": self.total_records,
                    "filter_type": self.filter_type,
                    "filter_value": self.filter_value,
                }
            }

        except Exception as exc:
            logger.exception("Error en FaltantesReport.execute")
            return {
                "success": False,
                "error": str(exc)
            }
