import os
import time
import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic

logger = logging.getLogger(__name__)


class VentaCeroReport:
    """Ejecución del reporte de Venta Cero contra procedimientos almacenados.

    Responsable de:
    - Validar parámetros (procedimiento permitido y tipo de alcance).
    - Ejecutar el procedimiento de manera segura y parametrizada.
    - Volcar resultados a un Excel server-side en streaming (chunks) para memoria estable.
    - Entregar metadatos de trazabilidad y previsualización.
    """

    DEFAULT_CHUNK_SIZE = 20000
    PROCEDURE_SCHEMA = "powerbi_bimbo"
    ALLOWED_FILTER_TYPES = {"producto", "proveedor", "categoria", "subcategoria"}
    DEFAULT_PROCEDURES = [
        {
            "id": "venta_cero",
            "procedure": "sp_reporte_venta_cero_dinamico",
            "label": "Venta Cero Dinámico",
            "params": [
                "p_ceve",
                "p_tipo_filtro",
                "p_codigo_producto",
                "p_categoria",
                "p_familia",
                "p_fecha_ini",
                "p_fecha_fin",
            ],
        },
    ]

    @classmethod
    def get_default_catalog(cls) -> List[Dict[str, object]]:
        """Catálogo base; facilita extender con futuros procedimientos cero."""
        return cls.DEFAULT_PROCEDURES.copy()

    def __init__(
        self,
        database_name: str,
        ceves_code: str,
        fecha_desde: str,
        fecha_hasta: str,
        user_id: int,
        procedure_id: str,
        filter_type: str,
        filter_value: str,
        extra_params: Optional[Dict[str, str]] = None,
        progress_callback: Optional[Callable[..., None]] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        procedures_catalog: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.database_name = database_name
        self.ceves_code = ceves_code
        self.fecha_desde = fecha_desde
        self.fecha_hasta = fecha_hasta
        self.user_id = user_id
        self.procedure_id = procedure_id
        self.filter_type = filter_type.lower().strip() if filter_type else ""
        self.filter_value = filter_value.strip() if filter_value else ""
        self.extra_params = extra_params or {}
        self.category_value = self.extra_params.get("category_value", "").strip()
        self.family_value = self.extra_params.get("family_value", "").strip()
        self.progress_callback = progress_callback
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        self.procedures_catalog = procedures_catalog or self.DEFAULT_PROCEDURES

        self.engine_mysql: Optional[Engine] = None
        self.file_path: Optional[str] = None
        self.file_name: Optional[str] = None
        self.preview_headers: List[str] = []
        self.preview_sample: List[Dict[Any, Any]] = []
        self.total_records = 0
        self.start_time = time.time()
        self._resolved_proc: Optional[Dict[str, Any]] = None
        self._zones_seen: set[str] = set()
        self.kpis: Dict[str, object] = {}

        # Defaults de negocio: proveedor fijo (BIMBO)
        if self.filter_type == "proveedor" and not self.filter_value:
            self.filter_value = "BIMBO"

    # --- Helpers internos ---
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
        allowed_proc_ids = {p["id"] for p in self.procedures_catalog if p.get("id")}
        if self.procedure_id not in allowed_proc_ids:
            raise ValueError("Procedimiento no permitido para Venta Cero")
        if self.filter_type not in self.ALLOWED_FILTER_TYPES:
            raise ValueError("Tipo de filtro no permitido para Venta Cero")
        if self.filter_type == "producto" and not self.filter_value:
            raise ValueError("El código de producto es obligatorio")
        if self.filter_type == "proveedor" and not self.filter_value:
            raise ValueError("El proveedor es obligatorio")
        if self.filter_type == "categoria" and not self.filter_value:
            raise ValueError("La categoría es obligatoria")
        if self.filter_type == "subcategoria":
            if not self.filter_value:
                raise ValueError("La subcategoría es obligatoria")
        if not (self.fecha_desde and self.fecha_hasta):
            raise ValueError("Las fechas son obligatorias")
        if self.fecha_desde > self.fecha_hasta:
            raise ValueError("La fecha inicial no puede ser mayor que la final")

    def _resolve_procedure(self) -> Dict[str, Any]:
        if self._resolved_proc:
            return self._resolved_proc
        for proc in self.procedures_catalog:
            if proc.get("id") == self.procedure_id:
                if not proc.get("procedure"):
                    raise ValueError("El procedimiento configurado no tiene nombre SQL")
                if not proc.get("params"):
                    raise ValueError("El procedimiento configurado no define parámetros")
                self._resolved_proc = proc
                return proc
        raise ValueError("Procedimiento no encontrado en el catálogo")

    @classmethod
    def _resolve_procedure_call_name(cls, proc_name: str) -> str:
        name = str(proc_name or "").strip().strip("`")
        if not name:
            return name
        if "." in name:
            return name
        return f"{cls.PROCEDURE_SCHEMA}.{name}"

    def _configure_connection(self) -> None:
        config_basic = ConfigBasic(self.database_name, self.user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuración de conexión incompleta para Venta Cero")
        self.engine_mysql = con.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _build_call(self) -> TextClause:
        proc = self._resolve_procedure()
        proc_name = self._resolve_procedure_call_name(str(proc["procedure"]))
        params_raw = proc.get("params")
        params = list(params_raw) if isinstance(params_raw, (list, tuple)) else []
        placeholders = ", ".join([f":{p}" for p in params])
        call_sql = f"CALL {proc_name}({placeholders})"
        # Trazabilidad: útil para auditar el contrato del SP en logs del worker.
        try:
            logger.info("[venta_cero][sql] %s", call_sql)
            print(f"[venta_cero][sql] {call_sql}", flush=True)
        except Exception:
            pass
        return text(call_sql)

    def _run_to_excel(self, query: TextClause) -> None:
        assert self.engine_mysql is not None
        os.makedirs("media", exist_ok=True)
        import datetime
        date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.file_name = f"venta_cero_{self.ceves_code}_{date_str}.xlsx"
        self.file_path = os.path.join("media", self.file_name)
        proc = self._resolve_procedure()
        param_order_raw = proc.get("params")
        param_order = list(param_order_raw) if isinstance(param_order_raw, (list, tuple)) else []
        value_map = {
            "p_ceve": int(self.ceves_code) if str(self.ceves_code).isdigit() else self.ceves_code,
            "p_tipo_filtro": self.filter_type.upper(),
            "p_codigo_producto": self.filter_value if self.filter_type == "producto" else "",
            # Para proveedor, enviamos el valor en p_categoria asumiendo reutilización de parámetro
            "p_categoria": self.filter_value if self.filter_type in ("categoria", "proveedor") else "",
            "p_familia": self.filter_value if self.filter_type == "subcategoria" else "",
            "p_fecha_ini": self.fecha_desde,
            "p_fecha_fin": self.fecha_hasta,
        }
        value_map.update({k: v for k, v in self.extra_params.items() if v is not None})
        params = {str(p): value_map.get(str(p), "") for p in param_order}

        # Trazabilidad: parámetros reales enviados al SP (sin credenciales).
        try:
            logger.info(
                "[venta_cero][params] proc=%s filter_type=%s ceve=%s fechas=%s..%s params=%s",
                proc.get("procedure"),
                self.filter_type,
                self.ceves_code,
                self.fecha_desde,
                self.fecha_hasta,
                params,
            )
            print(
                f"[venta_cero][params] proc={proc.get('procedure')} filter_type={self.filter_type} ceve={self.ceves_code} fechas={self.fecha_desde}..{self.fecha_hasta} params={params}",
                flush=True,
            )
            print(
                "[venta_cero][mapping] p_codigo_producto=%s p_categoria=%s p_familia=%s p_tipo_filtro=%s"
                % (
                    value_map.get("p_codigo_producto"),
                    value_map.get("p_categoria"),
                    value_map.get("p_familia"),
                    value_map.get("p_tipo_filtro"),
                ),
                flush=True,
            )
        except Exception:
            pass

        start_row = 0
        self._update_progress("Extrayendo resultados", 10)
        with self.engine_mysql.connect() as connection:
            try:
                # Mantiene la sesión alineada con el esquema del SP (powerbi_bimbo = utf8mb4_general_ci)
                try:
                    connection.exec_driver_sql("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
                    connection.exec_driver_sql("SET collation_connection = 'utf8mb4_general_ci'")
                except Exception as exc:
                    logger.warning("No se pudo fijar la collation de la sesión: %s", exc)

                result_iter = pd.read_sql_query(
                    sql=query, con=connection, params=params, chunksize=self.chunk_size
                )
                # Recolectar chunks; si no hay datos, evitamos abrir ExcelWriter
                # (openpyxl falla si no existe al menos una hoja visible).
                has_data = False
                for idx, chunk in enumerate(result_iter):
                    if chunk.empty:
                        continue
                    if not has_data:
                        has_data = True
                        excel_writer = pd.ExcelWriter(self.file_path, engine="openpyxl")
                    # KPIs: contar zonas de forma incremental sin cargar todo el dataset.
                    try:
                        if "Zona" in chunk.columns:
                            zonas = chunk["Zona"].dropna().astype(str).unique().tolist()
                            self._zones_seen.update([z for z in zonas if z])
                    except Exception:
                        pass

                    if not self.preview_headers:
                        self.preview_headers = list(chunk.columns)
                        self.preview_sample = (
                            chunk.head(10)
                            .astype(str)
                            .to_dict(orient="records")
                        )
                    chunk.to_excel(
                        excel_writer,
                        sheet_name="VentaCero",
                        index=False,
                        header=idx == 0,
                        startrow=start_row,
                    )
                    start_row += len(chunk)
                    self.total_records += len(chunk)
                    progress = min(90, 10 + int(idx * 5))
                    self._update_progress("Procesando resultados", progress)

                if has_data:
                    excel_writer.close()
                else:
                    # No hay clientes sin compra — crear Excel con encabezados para indicar resultado vacío
                    excel_writer = pd.ExcelWriter(self.file_path, engine="openpyxl")
                    headers = [
                        "CEVE", "Codigo", "Cliente", "Negocio", "Direccion",
                        "Barrio", "Ciudad", "Zona", "Dia_Ruta", "Tipo_Filtro", "Estado"
                    ]
                    pd.DataFrame(columns=headers).to_excel(
                        excel_writer, sheet_name="VentaCero", index=False
                    )
                    excel_writer.close()
                    self.preview_headers = headers

                # KPIs finales (con datos reales del procesamiento)
                self.kpis = {
                    "clientes_sin_compra": int(self.total_records),
                    "zonas_afectadas": int(len(self._zones_seen)),
                    "ultima_fecha": self.fecha_hasta,
                    "filtro_activo": f"{self.filter_type.upper()} = {self.filter_value}",
                }
            except SQLAlchemyError as exc:
                proc_cfg = self._resolve_procedure()
                proc_qualified = self._resolve_procedure_call_name(
                    str(proc_cfg.get("procedure", ""))
                )
                if "PROCEDURE" in str(exc).upper() and "does not exist" in str(exc):
                    raise ValueError(
                        f"No existe el procedimiento {proc_qualified}. "
                        f"Verifica despliegue en el esquema {self.PROCEDURE_SCHEMA}."
                    ) from exc
                logger.error("Error de base de datos en Venta Cero: %s", exc)
                # Mensaje guiado para el caso conocido de collation mix (1267)
                try:
                    if "(1267," in str(exc) and "Illegal mix of collations" in str(exc):
                        logger.error(
                            "Venta Cero: 1267 Illegal mix of collations. "
                            "Probable choque entre columnas del esquema del agente (v_db) y tmp_productos_validos. "
                            "Recomendado: aplicar scripts/sql/fixes/fix_sp_reporte_venta_cero_dinamico_collation.sql"
                        )
                except Exception:
                    pass
                raise
            except Exception as exc:
                logger.error("Error inesperado en Venta Cero: %s", exc)
                raise

    # --- Punto de entrada principal ---
    def run(self) -> Dict[str, object]:
        try:
            self._update_progress("Inicializando", 1)
            self._validate_inputs()
            self._configure_connection()
            query = self._build_call()
            self._run_to_excel(query)
            execution_time = time.time() - self.start_time
            self._update_progress("Completado", 100)
            message = (
                "Reporte de Venta Cero generado correctamente."
                if self.total_records > 0
                else "Sin clientes con venta cero para los filtros seleccionados. Todos los clientes realizaron compras en el período."
            )
            return {
                "success": True,
                "message": message,
                "file_path": self.file_path,
                "file_name": self.file_name,
                "metadata": {
                    "total_records": self.total_records,
                    "procedure": self._resolve_procedure().get("procedure"),
                    "filter_type": self.filter_type,
                    "filter_value": self.filter_value,
                    "category_value": self.category_value,
                    "family_value": self.family_value,
                    "ceves": self.ceves_code,
                    "preview_headers": self.preview_headers,
                    "preview_sample": self.preview_sample,
                    "kpis": self.kpis,
                },
            }
        except Exception as exc:
            execution_time = time.time() - self.start_time
            logger.error("Venta Cero falló: %s", exc, exc_info=True)
            return {
                "success": False,
                "error_message": str(exc),
                "file_path": None,
                "file_name": None,
                "metadata": {
                    "procedure": self._resolve_procedure().get("procedure") if self._resolved_proc else None,
                    "filter_type": self.filter_type,
                    "filter_value": self.filter_value,
                    "category_value": self.category_value,
                    "family_value": self.family_value,
                    "ceves": self.ceves_code,
                },
                "execution_time": execution_time,
            }
