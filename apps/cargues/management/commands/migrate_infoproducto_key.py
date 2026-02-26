"""
Management command para migrar la clave única de fact_infoproducto

Uso:
    python manage.py migrate_infoproducto_key [--dry-run] [--backup]
"""

from django.core.management.base import BaseCommand
from scripts.sql.migrate_fix_infoproducto_unique_key import MigracionInfoProducto


class Command(BaseCommand):
    help = 'Migración de clave única para fact_infoproducto'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Solo muestra lo que haría, sin hacer cambios',
        )
        parser.add_argument(
            '--backup',
            action='store_true',
            help='Crea tabla de backup antes de eliminar duplicados',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        backup = options['backup']

        migracion = MigracionInfoProducto(dry_run=dry_run, backup=backup)
        migracion.ejecutar()
