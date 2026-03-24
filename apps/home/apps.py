from django.apps import AppConfig


class HomeConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.home'

    def ready(self):
        import logging
        logger = logging.getLogger(__name__)

        try:
            from apps.home.views_email_config import reprogramar_tareas
            reprogramar_tareas(logger)
            logger.info("Tareas programadas desde BD (ProgramacionTarea).")
        except Exception as e:
            logger.warning("ProgramacionTarea no disponible, usando fallback: %s", e)
            self._programar_limpieza_media(logger)

    @staticmethod
    def _programar_limpieza_media(logger):
        """Fallback: solo Limpieza Media (global). Tareas per-empresa se crean desde la UI."""
        try:
            from django_rq import get_scheduler
            from datetime import datetime
            from apps.home.views import clean_old_media_files

            scheduler = get_scheduler('default')
            for job in scheduler.get_jobs():
                if job.func_name == 'apps.home.views.clean_old_media_files':
                    scheduler.cancel(job)
            scheduler.schedule(
                scheduled_time=datetime.utcnow(),
                func=clean_old_media_files,
                args=[4],
                interval=3600,
                repeat=None,
            )
            logger.info("Limpieza media programada (fallback).")
        except Exception as e:
            logger.error("No se pudo programar limpieza media (fallback): %s", e)
