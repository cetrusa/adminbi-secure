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


class RuteroReport:
    """Ejecución del reporte de Rutero (Maestro de Rutas y Clientes).

    Responsable de:
    - Ejecutar el procedimiento almacenado sp_reporte_rutero_dinamico.
    - Volcar resultados a un Excel server-side.
    """

    DEFAULT_CHUNK_SIZE = 20000
    PROCEDURE_NAME = "sp_reporte_rutero_dinamico"

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
        self._clients_seen: set[str] = set()
        self._zones_seen: set[str] = set()
        self._days_seen: set[str] = set()
        self.kpis: Dict[str, object] = {}

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

    def _configure_connection(self) -> None:
        # Usa la misma configuración de conexión que Venta Cero (base powerbi_bimbo para catálogos)
        # o la base seleccionada si ConfigBasic maneja credenciales globales.
        # Asumimos que ConfigBasic resuelve las credenciales del usuario/empresa actual.
        config_basic = ConfigBasic(self.database_name, self.user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuración de conexión incompleta para Rutero")
        self.engine_mysql = con.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _build_call(self) -> TextClause:
        # El SP solo recibe p_ceve
        call_sql = f"CALL {self.PROCEDURE_NAME}(:p_ceve)"
        try:
            logger.info("[rutero][sql] %s", call_sql)
            print(f"[rutero][sql] {call_sql}", flush=True)
        except Exception:
            pass
        return text(call_sql)

    def _run_to_excel(self, query: TextClause) -> None:
        assert self.engine_mysql is not None
        os.makedirs("media", exist_ok=True)
        date_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        self.file_name = f"rutero_{self.ceves_code}_{date_str}.xlsx"
        self.file_path = os.path.join("media", self.file_name)

        params = {
            "p_ceve": int(self.ceves_code) if str(self.ceves_code).isdigit() else self.ceves_code
        }

        try:
            logger.info("[rutero][params] ceve=%s", self.ceves_code)
            print(f"[rutero][params] ceve={self.ceves_code}", flush=True)
        except Exception:
            pass

        start_row = 0
        self._update_progress("Consultando base de datos", 10)
        
        with self.engine_mysql.connect() as connection:
            try:
                # pandas read_sql_query con chunksize para manejar grandes volúmenes
                result_iter = pd.read_sql_query(
                    sql=query, con=connection, params=params, chunksize=self.chunk_size
                )
                
                with pd.ExcelWriter(self.file_path, engine="openpyxl") as writer:
                    has_data = False
                    for idx, chunk in enumerate(result_iter):
                        has_data = True
                        # KPIs incrementales
                        try:
                            if "Codigo_Cliente" in chunk.columns:
                                self._clients_seen.update(
                                    [c for c in chunk["Codigo_Cliente"].dropna().astype(str).tolist() if c]
                                )
                            if "Zona" in chunk.columns:
                                self._zones_seen.update(
                                    [z for z in chunk["Zona"].dropna().astype(str).tolist() if z]
                                )
                            if "Nombre_Dia" in chunk.columns:
                                self._days_seen.update(
                                    [d for d in chunk["Nombre_Dia"].dropna().astype(str).tolist() if d]
                                )
                        except Exception:
                            pass

                        if idx == 0:
                            self.preview_headers = list(chunk.columns)
                            self.preview_sample = (
                                chunk.head(10).astype(str).to_dict(orient="records")
                            )

                        chunk.to_excel(
                            writer,
                            sheet_name="Rutero",
                            index=False,
                            header=(idx == 0),
                            startrow=start_row,
                        )
                        start_row += len(chunk)
                        self.total_records += len(chunk)
                        
                        # Progreso ficticio pero informativo basado en chunks
                        progress = min(90, 10 + int(idx * 5))
                        self._update_progress(f"Procesando lote {idx+1}", progress)
                    
                    if not has_data:
                         # Si no hubo datos, creamos un excel vacío con headers genéricos o avisamos
                         pd.DataFrame(columns=["Mensaje"]).to_excel(writer, sheet_name="Rutero", index=False)

                # KPIs finales
                self.kpis = {
                    "clientes_en_ruta": int(len(self._clients_seen)) if self._clients_seen else int(self.total_records),
                    "zonas": int(len(self._zones_seen)),
                    "dias_ruta": int(len(self._days_seen)),
                    "filas": int(self.total_records),
                }

            except SQLAlchemyError as exc:
                logger.error("Error de base de datos en Rutero: %s", exc)
                raise
            except Exception as exc:
                logger.error("Error generando Excel Rutero: %s", exc)
                raise

    def execute(self) -> Dict[str, Any]:
        """Método principal de orquestación."""
        try:
            self._update_progress("Iniciando validación", 5)
            self._validate_inputs()
            self._configure_connection()
            
            query = self._build_call()
            self._run_to_excel(query)

            self._update_progress("Finalizado", 100)
            return {
                "success": True,
                "message": "Reporte de Rutero generado correctamente.",
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
            logger.error("Fallo ejecución Rutero: %s", e)
            return {
                "success": False,
                "error_message": str(e),
            }
