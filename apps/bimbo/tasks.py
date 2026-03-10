import logging

from django.db import connection
from django_rq import job
from rq import get_current_job

from apps.home.tasks import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_TIMEOUT,
    task_handler,
    update_job_progress,
)
from scripts.bimbo.reportes.faltantes import FaltantesReport
from scripts.bimbo.reportes.rutero import RuteroReport
from scripts.bimbo.reportes.venta_cero import VentaCeroReport
from scripts.extrae_bi.inventarios import InventariosReport

logger = logging.getLogger(__name__)


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def venta_cero_task(
    database_name,
    ceves_code,
    IdtReporteIni,
    IdtReporteFin,
    user_id,
    procedure_name,
    filter_type,
    filter_value,
    extra_params=None,
    batch_size=DEFAULT_BATCH_SIZE,
):
    """Tarea RQ para ejecutar el reporte de Venta Cero via SP dinamico."""
    try:
        filter_type_norm = (str(filter_type) or "").strip().lower()
    except Exception:
        filter_type_norm = filter_type
    if filter_type_norm == "proveedor" and not filter_value:
        filter_value = "BIMBO"

    if not all(
        [
            database_name,
            ceves_code,
            IdtReporteIni,
            IdtReporteFin,
            procedure_name,
            filter_type,
            filter_value,
        ]
    ):
        return {
            "success": False,
            "error_message": "Parametros incompletos para Venta Cero",
            "metadata": {
                "procedure": procedure_name,
                "filter_type": filter_type,
                "filter_value": filter_value,
            },
        }
    if IdtReporteIni > IdtReporteFin:
        return {
            "success": False,
            "error_message": "La fecha inicial no puede ser mayor que la final",
        }
    try:
        connection.close()
    except Exception:
        pass

    job = get_current_job()
    job_id = job.id if job else None
    logger.info(
        "Iniciando venta_cero_task (RQ Job ID: %s) para DB: %s, Periodo: %s-%s",
        job_id,
        database_name,
        IdtReporteIni,
        IdtReporteFin,
    )

    def rq_update_progress(stage, progress_percent, current_rec=None, total_rec=None, *_args):
        meta = {"stage": stage}
        if current_rec is not None:
            meta["records_processed"] = current_rec
        if total_rec is not None:
            meta["total_records_estimate"] = total_rec
        update_job_progress(job_id, int(progress_percent), status="processing", meta=meta)

    report = VentaCeroReport(
        database_name,
        ceves_code,
        IdtReporteIni,
        IdtReporteFin,
        user_id,
        procedure_name,
        filter_type,
        filter_value,
        extra_params=extra_params or {},
        progress_callback=rq_update_progress,
        chunk_size=batch_size,
    )

    result_data = report.run()

    try:
        connection.close()
    except Exception:
        pass

    return result_data


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def rutero_task(database_name, ceves_code, user_id, batch_size=DEFAULT_BATCH_SIZE):
    """Tarea RQ para generar el Rutero."""
    try:
        connection.close()
    except Exception:
        pass

    job = get_current_job()
    job_id = job.id if job else None

    def rq_update_progress(stage, progress_percent, current_rec=None, total_rec=None, *_args):
        meta = {"stage": stage}
        if current_rec is not None:
            meta["records_processed"] = current_rec
        if total_rec is not None:
            meta["total_records_estimate"] = total_rec
        update_job_progress(job_id, int(progress_percent), status="processing", meta=meta)

    report = RuteroReport(
        database_name,
        ceves_code,
        user_id,
        progress_callback=rq_update_progress,
        chunk_size=batch_size,
    )

    result_data = report.execute()

    try:
        connection.close()
    except Exception:
        pass

    return result_data


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def inventarios_task(database_name, ceves_code, user_id, batch_size=DEFAULT_BATCH_SIZE):
    """Tarea RQ para generar Inventarios."""
    try:
        connection.close()
    except Exception:
        pass

    job = get_current_job()
    job_id = job.id if job else None

    def rq_update_progress(stage, progress_percent, current_rec=None, total_rec=None, *_args):
        meta = {"stage": stage}
        if current_rec is not None:
            meta["records_processed"] = current_rec
        if total_rec is not None:
            meta["total_records_estimate"] = total_rec
        update_job_progress(job_id, int(progress_percent), status="processing", meta=meta)

    report = InventariosReport(
        database_name,
        ceves_code,
        user_id,
        progress_callback=rq_update_progress,
        chunk_size=batch_size,
    )

    result_data = report.execute()

    try:
        connection.close()
    except Exception:
        pass

    return result_data


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def preventa_task(
    database_name,
    ceves_code,
    IdtReporteIni,
    IdtReporteFin,
    user_id,
    batch_size=DEFAULT_BATCH_SIZE,
):
    """Tarea RQ para generar Preventa."""
    from scripts.extrae_bi.preventa import PreventaReport

    try:
        connection.close()
    except Exception:
        pass

    job = get_current_job()
    job_id = job.id if job else None

    def rq_update_progress(stage, progress_percent, current_rec=None, total_rec=None, *_args):
        meta = {"stage": stage}
        if current_rec is not None:
            meta["records_processed"] = current_rec
        if total_rec is not None:
            meta["total_records_estimate"] = total_rec
        update_job_progress(job_id, int(progress_percent), status="processing", meta=meta)

    report = PreventaReport(
        database_name,
        ceves_code,
        IdtReporteIni,
        IdtReporteFin,
        user_id,
        progress_callback=rq_update_progress,
        chunk_size=batch_size,
    )

    result_data = report.execute()

    try:
        connection.close()
    except Exception:
        pass

    return result_data


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def faltantes_task(
    database_name,
    ceves_code,
    IdtReporteIni,
    IdtReporteFin,
    user_id,
    filter_type,
    filter_value,
    extra_params=None,
    batch_size=DEFAULT_BATCH_SIZE,
):
    """Tarea RQ para generar Faltantes."""
    try:
        connection.close()
    except Exception:
        pass

    job = get_current_job()
    job_id = job.id if job else None

    filter_type_norm = (filter_type or "").strip().lower()
    if filter_type_norm == "proveedor" and not filter_value:
        filter_value = "BIMBO"

    def rq_update_progress(stage, progress_percent, current_rec=None, total_rec=None, *_args):
        meta = {"stage": stage}
        if current_rec is not None:
            meta["records_processed"] = current_rec
        if total_rec is not None:
            meta["total_records_estimate"] = total_rec
        update_job_progress(job_id, int(progress_percent), status="processing", meta=meta)

    report = FaltantesReport(
        database_name=database_name,
        ceves_code=ceves_code,
        fecha_ini=IdtReporteIni,
        fecha_fin=IdtReporteFin,
        filter_type=filter_type_norm or "proveedor",
        filter_value=filter_value,
        extra_params=extra_params or {},
        user_id=user_id,
        progress_callback=rq_update_progress,
        chunk_size=batch_size,
    )

    result_data = report.execute()

    try:
        connection.close()
    except Exception:
        pass

    return result_data


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def bimbo_discovery_task(database_name: str):
    """
    Tarea RQ: Discovery de proveedor BIMBO para UNA agencia.
    """
    try:
        connection.close()
    except Exception:
        pass

    job_obj = get_current_job()
    job_id = job_obj.id if job_obj else None
    logger.info("bimbo_discovery_task (Job %s) db=%s", job_id, database_name)
    update_job_progress(job_id, 5, "processing", meta={"stage": "Iniciando discovery"})

    from scripts.bimbo.discovery_inicial import BimboDiscovery

    service = BimboDiscovery(database_name)
    resultado = service.run()

    try:
        connection.close()
    except Exception:
        pass

    return {
        "success": resultado.get("estado") == "OK",
        "message": f"Discovery '{database_name}': {resultado.get('estado')} | prov={resultado.get('id_proveedor_bimbo')} | fvp={resultado.get('id_proveedor_fvp')}",
        **resultado,
    }


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def bimbo_discovery_todas_task():
    """
    Tarea RQ: Discovery de proveedor BIMBO para TODAS las agencias.
    """
    try:
        connection.close()
    except Exception:
        pass

    job_obj = get_current_job()
    job_id = job_obj.id if job_obj else None
    logger.info("bimbo_discovery_todas_task (Job %s)", job_id)
    update_job_progress(
        job_id,
        5,
        "processing",
        meta={"stage": "Iniciando discovery todas agencias"},
    )

    from scripts.bimbo.discovery_inicial import run_discovery_todas

    resultados = run_discovery_todas()

    ok = sum(1 for r in resultados if r.get("estado") == "OK")
    errores = sum(1 for r in resultados if r.get("estado") == "ERROR")

    try:
        connection.close()
    except Exception:
        pass

    return {
        "success": errores == 0,
        "message": f"Discovery todas: {ok} OK | {errores} errores | {len(resultados)} total",
        "total": len(resultados),
        "ok": ok,
        "errores": errores,
        "detalle": resultados,
    }


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def bimbo_snapshot_task(database_name: str, solo_discovery: bool = False):
    """
    Tarea RQ: Snapshot diario de productos BIMBO para UNA agencia.
    """
    try:
        connection.close()
    except Exception:
        pass

    job_obj = get_current_job()
    job_id = job_obj.id if job_obj else None
    logger.info("bimbo_snapshot_task (Job %s) db=%s", job_id, database_name)
    update_job_progress(
        job_id,
        5,
        "processing",
        meta={"stage": "Iniciando snapshot BIMBO"},
    )

    from scripts.bimbo.bz_bimbo_snapshot import run_bimbo_snapshot

    resultado = run_bimbo_snapshot(
        database_name=database_name,
        solo_discovery=solo_discovery,
    )

    try:
        connection.close()
    except Exception:
        pass

    estado = resultado.get("estado", "ERROR")
    return {
        "success": estado == "OK",
        "message": f"Snapshot '{database_name}': {estado} | nuevos={resultado.get('nuevos', 0)} | auto={resultado.get('auto_asignados', 0)}",
        **resultado,
    }


@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)
@task_handler
def bimbo_homologacion_task(
    database_name: str,
    id_agencia: int = None,
):
    """
    Tarea RQ: Auto-homologa equivalencias PENDIENTES para una agencia.
    """
    try:
        connection.close()
    except Exception:
        pass

    job_obj = get_current_job()
    job_id = job_obj.id if job_obj else None
    logger.info(
        "bimbo_homologacion_task (Job %s) db=%s agencia=%s",
        job_id,
        database_name,
        id_agencia,
    )

    def rq_progress(stage, pct, *_args, **_kw):
        update_job_progress(job_id, int(pct), "processing", meta={"stage": stage})

    update_job_progress(job_id, 5, "processing", meta={"stage": "Iniciando homologacion"})

    from scripts.bimbo.homologacion_updater import HomologacionConfig, HomologacionUpdater

    config = HomologacionConfig(database_name)
    updater = HomologacionUpdater(
        config=config,
        id_agencia=id_agencia,
        progress_callback=rq_progress,
    )
    resultado = updater.run()

    try:
        connection.close()
    except Exception:
        pass

    return resultado
