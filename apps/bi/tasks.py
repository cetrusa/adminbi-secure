from scripts.extrae_bi.apipowerbi import Api_PowerBi, Api_PowerBi_Config
import apps.home.tasks as home_tasks
import os
import time
import logging
import traceback
from functools import wraps
from typing import Dict, Any, Optional, Callable, TypeVar

# RQ Imports
from django_rq import job
from rq import get_current_job

# Configuración de logging
logger = logging.getLogger(__name__)

from django.conf import settings

# --- Constantes y Tipos (Ajustar según necesidad) ---
# Tomar timeout desde entorno o settings RQ_QUEUES; fallback 7200
DEFAULT_TIMEOUT = int(
    os.getenv(
        "RQ_TASK_TIMEOUT",
        getattr(getattr(settings, "RQ_QUEUES", {}), "get", lambda *_: {})(
            "default", {}
        ).get("DEFAULT_TIMEOUT", 7200),
    )
)
DEFAULT_BATCH_SIZE = 50000
# DEFAULT_RETRY_COUNT = 3 # No usado directamente
# JOB_PROGRESS_KEY_PREFIX = "job_progress_" # No usado directamente
# JOB_META_KEY_PREFIX = "job_meta_" # No usado directamente

# Tipos para tipado
T = TypeVar("T")
ResultDict = Dict[str, Any]


@job("default", timeout=DEFAULT_TIMEOUT)
def actualiza_bi_task(
    database_name: str,
    IdtReporteIni: str,
    IdtReporteFin: str,
    user_id: Optional[int] = None,
    id_reporte: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    """
    Tarea RQ: Ejecuta la extracción y procesamiento de datos BI (Extrae_Bi).
    """
    job = get_current_job()
    job_id = job.id if job else None

    logger.info(
        "[actualiza_bi_task] INICIO: database_name=%s, periodo=%s..%s, user_id=%s, id_reporte=%s, batch_size=%s",
        database_name, IdtReporteIni, IdtReporteFin, user_id, id_reporte, batch_size,
    )

    def rq_update_progress(meta_dict, progress_percent):
        logger.debug(
            "[actualiza_bi_task] Progress callback: meta=%s, progress=%s",
            meta_dict, progress_percent,
        )
        # meta_dict contiene: stage, tabla, nmReporte, progress
        home_tasks.update_job_progress(job_id, int(progress_percent), meta=meta_dict)

    logger.debug("[actualiza_bi_task] Instanciando ExtraeBiConfig y ExtraeBiExtractor...")
    home_tasks.update_job_progress(job_id, 5, meta={"stage": "Iniciando Extrae_Bi"})
    logger.info(
        f"Iniciando actualiza_bi_task (RQ Job: {job_id}) para {database_name}, Periodo: {IdtReporteIni}-{IdtReporteFin}, user_id={user_id}, id_reporte={id_reporte}, batch_size={batch_size}"
    )

    config = Api_PowerBi_Config(database_name)
    extractor = Api_PowerBi(
        config,
        IdtReporteIni,
        IdtReporteFin,
        user_id=user_id,
        id_reporte=id_reporte,
        batch_size=batch_size,
        progress_callback=rq_update_progress,
    )
    logger.debug("[actualiza_bi_task] Ejecutando run() de ExtraeBiExtractor...")
    home_tasks.update_job_progress(
        job_id, 15, meta={"stage": "Ejecutando extractor principal"}
    )
    result = extractor.run()
    logger.info("[actualiza_bi_task] RESULTADO: %s", result)
    home_tasks.update_job_progress(
        job_id, 95, meta={"stage": "Finalizando extracción BI"}
    )
    logger.info("[actualiza_bi_task] FIN")
    return result
