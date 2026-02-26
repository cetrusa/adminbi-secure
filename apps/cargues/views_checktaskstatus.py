from django.views import View
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rq.job import Job, NoSuchJobError
from django_rq import get_connection
import time
import os
import traceback


@method_decorator(csrf_exempt, name='dispatch')
class CheckCargueTaskStatusView(View):
    """
    Vista para comprobar el estado de tareas de cargue masivo y recuperar resultados/progreso.
    Compatible con tasks que retornan dict estándar (success, message, warnings, etc).
    """

    def get(self, request, *args, **kwargs):
        """Maneja peticiones GET para polling de estado"""
        task_id = request.GET.get("task_id") or request.session.get("task_id")
        return self._check_task_status(task_id)

    def post(self, request, *args, **kwargs):
        """Maneja peticiones POST para verificar estado"""
        task_id = request.POST.get("task_id") or request.session.get("task_id")
        return self._check_task_status(task_id)

    def _check_task_status(self, task_id):
        """Lógica común para verificar el estado de una tarea"""
        print(f"[CHECKTASKSTATUS] Verificando estado. task_id={task_id}")
        if not task_id:
            print("[CHECKTASKSTATUS][ERROR] No task ID provided")
            return JsonResponse({"error": "No task ID provided"}, status=400)
        
        connection = get_connection()
        try:
            job = Job.fetch(task_id, connection=connection)
            print(f"[CHECKTASKSTATUS] Job status: {job.get_status()} | job_id={job.id}")
            if job.is_finished:
                result = job.result
                print(f"[CHECKTASKSTATUS] Job terminado. Resultado: {result}")
                job_info = {
                    "execution_time": (
                        result.get("execution_time", 0)
                        if isinstance(result, dict)
                        else 0
                    ),
                    "completed_at": time.time(),
                    "started_at": (
                        job.started_at.timestamp() if job.started_at else None
                    ),
                    "enqueued_at": (
                        job.enqueued_at.timestamp() if job.enqueued_at else None
                    ),
                    "task_name": job.func_name,
                    "task_args": job.args,
                }
                if isinstance(result, dict):
                    result.update(
                        {
                            "job_info": job_info,
                            "summary": result.get("summary", ""),
                        }
                    )
                return JsonResponse(
                    {
                        "status": (
                            "completed" if result.get("success", False) else "failed"
                        ),
                        "result": result,
                        "progress": 100,
                        "meta": (
                            result.get("metadata", {})
                            if isinstance(result, dict)
                            else {}
                        ),
                    }
                )
            elif job.is_failed:
                print(f"[CHECKTASKSTATUS][ERROR] Job fallido. job_id={job.id}")
                error_info = {
                    "job_id": job.id,
                    "exception": (
                        str(job.exc_info)
                        if hasattr(job, "exc_info") and job.exc_info
                        else "Error desconocido"
                    ),
                    "started_at": (
                        job.started_at.timestamp() if job.started_at else None
                    ),
                    "enqueued_at": (
                        job.enqueued_at.timestamp() if job.enqueued_at else None
                    ),
                }
                return JsonResponse(
                    {
                        "status": "failed",
                        "result": job.result,
                        "error_info": error_info,
                    },
                    status=500,
                )
            else:
                progress = 0
                stage = "En cola"
                meta = {}
                if hasattr(job, "meta") and job.meta:
                    meta = job.meta.copy()
                    if "progress" in job.meta:
                        progress = job.meta.get("progress")
                    if "stage" in job.meta:
                        stage = job.meta.get("stage")
                    if "status" in job.meta:
                        stage = job.meta.get("status")
                elapsed_time = 0
                if job.started_at:
                    elapsed_time = time.time() - job.started_at.timestamp()
                eta = None
                if progress > 5 and elapsed_time > 0:
                    try:
                        eta = (elapsed_time / progress) * (100 - progress)
                    except:
                        eta = None
                meta["last_update"] = time.time()
                print(
                    f"[CHECKTASKSTATUS] Job en progreso. progress={progress}, stage={stage}, meta={meta}"
                )
                return JsonResponse(
                    {
                        "status": job.get_status(),
                        "progress": progress,
                        "stage": stage,
                        "meta": meta,
                        "elapsed_time": elapsed_time,
                        "eta": eta,
                    }
                )
        except NoSuchJobError:
            print(f"[CHECKTASKSTATUS][ERROR] Tarea no encontrada: {task_id}")
            return JsonResponse(
                {
                    "status": "notfound",
                    "error": "Tarea no encontrada. Puede que haya expirado o se haya completado hace mucho tiempo.",
                }
            )
        except Exception as e:
            print(f"[CHECKTASKSTATUS][ERROR] Excepción: {e}\n{traceback.format_exc()}")
            return JsonResponse({"status": "error", "error": str(e)}, status=500)
