from django.apps import AppConfig


class HomeConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.home'

    def ready(self):
        import logging
        logger = logging.getLogger(__name__)

        # Programar limpieza periódica de media/ con django-rq-scheduler
        try:
            from django_rq import get_scheduler
            from datetime import datetime, timedelta
            from apps.home.views import clean_old_media_files

            scheduler = get_scheduler('default')
            # Evita duplicados: elimina trabajos previos de limpieza
            for job in scheduler.get_jobs():
                if job.func_name == 'apps.home.views.clean_old_media_files':
                    scheduler.cancel(job)
            # Programa la tarea cada hora
            scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=clean_old_media_files,
                args=[4],
                interval=3600,
                repeat=None,
            )
        except Exception as e:
            logger.error(f"No se pudo programar limpieza periódica de media/: {e}")

        # Programar envio nocturno de reportes por correo (2:00 AM Bogota = 07:00 UTC)
        try:
            from django_rq import get_scheduler
            from datetime import datetime, timedelta
            from apps.home.tasks import enviar_reportes_email_todas_empresas_task

            scheduler = get_scheduler('default')
            func_name = 'apps.home.tasks.enviar_reportes_email_todas_empresas_task'
            for job in scheduler.get_jobs():
                if job.func_name == func_name:
                    scheduler.cancel(job)

            # Calcular proxima ejecucion a las 07:00 UTC (2:00 AM Bogota UTC-5)
            now_utc = datetime.utcnow()
            next_run = now_utc.replace(hour=7, minute=0, second=0, microsecond=0)
            if next_run <= now_utc:
                next_run += timedelta(days=1)

            scheduler.schedule(
                scheduled_time=next_run,
                func=enviar_reportes_email_todas_empresas_task,
                interval=86400,  # cada 24 horas
                repeat=None,
            )
            logger.info("Envio nocturno de reportes programado para %s UTC", next_run)
        except Exception as e:
            logger.error(f"No se pudo programar envio nocturno de reportes: {e}")
