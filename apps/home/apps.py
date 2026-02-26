from django.apps import AppConfig


class HomeConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.home'

    def ready(self):
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
                scheduled_time=datetime.utcnow(),  # Inicia inmediatamente
                func=clean_old_media_files,
                args=[4],  # 4 horas de antigüedad
                interval=3600,  # cada 3600 segundos (1 hora)
                repeat=None,  # infinito
            )
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"No se pudo programar limpieza periódica de media/: {e}")
