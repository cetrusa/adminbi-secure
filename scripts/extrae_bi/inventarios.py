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


class InventariosReport:
    """Ejecución del reporte de Inventarios.

    Responsable de:
    - Ejecutar el procedimiento almacenado sp_reporte_inventarios_dinamico.
    - Volcar resultados a un Excel server-side.
    """

    DEFAULT_CHUNK_SIZE = 20000
    PREVIEW_SIZE = 200
    PROCEDURE_NAME = "sp_reporte_inventarios_dinamico"

    def __init__(
        self,
        database_name: str,
        ceves_code: str,
        user_id: int,
        progress_callback: Optional[Callable[..., None]] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.database_name = database_name
        self.ceves_code = ceves_code
        self.user_id = user_id
        self.progress_callback = progress_callback
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE

        self.engine_mysql: Optional[Engine] = None
        self.file_path: Optional[str] = None
        self.file_name: Optional[str] = None
        self.preview_headers: List[str] = []
        self.preview_sample: List[Dict[Any, Any]] = []
        self.total_records = 0
        self.start_time = time.time()
        self._products_seen: set[str] = set()
        self._warehouses_seen: set[str] = set()
        self.total_inventory = 0.0
        self.kpis: Dict[str, object] = {}

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

    def _configure_connection(self) -> None:
        config_basic = ConfigBasic(self.database_name, self.user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuración de conexión incompleta para Inventarios")
        self.engine_mysql = con.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _build_call(self) -> TextClause:
        call_sql = f"CALL {self.PROCEDURE_NAME}(:p_ceve)"
        try:
            logger.info("[inventarios][sql] %s", call_sql)
            print(f"[inventarios][sql] {call_sql}", flush=True)
        except Exception:
            pass
        return text(call_sql)

    def _run_to_excel(self, query: TextClause) -> None:
        assert self.engine_mysql is not None
        os.makedirs("media", exist_ok=True)
        date_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        self.file_name = f"inventarios_{self.ceves_code}_{date_str}.xlsx"
        self.file_path = os.path.join("media", self.file_name)

        params = {
            "p_ceve": int(self.ceves_code) if str(self.ceves_code).isdigit() else self.ceves_code
        }

        try:
            logger.info("[inventarios][params] ceve=%s", self.ceves_code)
            print(f"[inventarios][params] ceve={self.ceves_code}", flush=True)
        except Exception:
            pass

        start_row = 0
        self._update_progress("Consultando base de datos", 10)

        with self.engine_mysql.connect() as connection:
            try:
                result_iter = pd.read_sql_query(
                    sql=query, con=connection, params=params, chunksize=self.chunk_size
                )

                with pd.ExcelWriter(self.file_path, engine="openpyxl") as writer:
                    has_data = False
                    for idx, chunk in enumerate(result_iter):
                        has_data = True
                        try:
                            if "Codigo_Producto" in chunk.columns:
                                self._products_seen.update(
                                    [c for c in chunk["Codigo_Producto"].dropna().astype(str).tolist() if c]
                                )
                            if "Almacen" in chunk.columns:
                                self._warehouses_seen.update(
                                    [a for a in chunk["Almacen"].dropna().astype(str).tolist() if a]
                                )
                            if "Inventario_Disponible" in chunk.columns:
                                self.total_inventory += float(chunk["Inventario_Disponible"].fillna(0).sum())
                        except Exception:
                            pass

                        if idx == 0:
                            self.preview_headers = list(chunk.columns)
                            self.preview_sample = (
                                chunk.head(self.PREVIEW_SIZE)
                                .astype(str)
                                .to_dict(orient="records")
                            )

                        chunk.to_excel(
                            writer,
                            sheet_name="Inventarios",
                            index=False,
                            header=(idx == 0),
                            startrow=start_row,
                        )
                        start_row += len(chunk)
                        self.total_records += len(chunk)

                        progress = min(90, 10 + int(idx * 5))
                        self._update_progress(f"Procesando lote {idx+1}", progress)

                    if not has_data:
                        pd.DataFrame(columns=["Mensaje"]).to_excel(
                            writer, sheet_name="Inventarios", index=False
                        )

                self.kpis = {
                    "productos": int(len(self._products_seen)),
                    "almacenes": int(len(self._warehouses_seen)),
                    "filas": int(self.total_records),
                    "inventario_total": float(self.total_inventory),
                }

            except SQLAlchemyError as exc:
                logger.error("Error de base de datos en Inventarios: %s", exc)
                raise
            except Exception as exc:
                logger.error("Error generando Excel Inventarios: %s", exc)
                raise

    def execute(self) -> Dict[str, Any]:
        try:
            self._update_progress("Iniciando validación", 5)
            self._validate_inputs()
            self._configure_connection()

            query = self._build_call()
            self._run_to_excel(query)

            self._update_progress("Finalizado", 100)
            return {
                "success": True,
                "message": "Reporte de Inventarios generado correctamente.",
                "file_path": self.file_path,
                "file_name": self.file_name,
                "metadata": {
                    "ceves": self.ceves_code,
                    "total_records": self.total_records,
                    "preview_headers": self.preview_headers,
                    "preview_sample": self.preview_sample,
                    "kpis": self.kpis,
                },
            }

        except Exception as e:
            logger.error("Fallo ejecución Inventarios: %s", e)
            return {
                "success": False,
                "error_message": str(e),
            }
