import logging
from django.core.management.base import BaseCommand
from apps.home.views import clean_old_media_files

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Elimina archivos antiguos de la carpeta media/ con extensiones temporales (.xlsx, .db, .zip, .csv, .txt) que tengan más de N horas."

    def add_arguments(self, parser):
        parser.add_argument(
            "--hours",
            type=int,
            default=4,
            help="Antigüedad en horas para eliminar archivos (default: 4)",
        )

    def handle(self, *args, **options):
        hours = options["hours"]
        removed = clean_old_media_files(hours=hours)
        if removed:
            self.stdout.write(
                self.style.SUCCESS(f"Archivos eliminados ({len(removed)}):")
            )
            for f in removed:
                self.stdout.write(f" - {f}")
        else:
            self.stdout.write(self.style.WARNING("No se eliminaron archivos."))
