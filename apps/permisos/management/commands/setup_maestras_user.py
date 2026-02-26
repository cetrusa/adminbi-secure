from django.core.management.base import BaseCommand
from django.contrib.auth.models import User, Permission
from django.contrib.contenttypes.models import ContentType
from apps.permisos.models import PermisosBarra


class Command(BaseCommand):
    help = 'Configurar permisos de cargue de maestras para un usuario'

    def add_arguments(self, parser):
        parser.add_argument(
            'username',
            type=str,
            help='Username del usuario para asignar permisos de cargue maestras',
        )

    def handle(self, *args, **options):
        username = options['username']
        
        try:
            # Buscar el usuario
            user = User.objects.get(username=username)
            self.stdout.write(
                self.style.SUCCESS(f'👤 Usuario encontrado: {user.get_full_name() or username}')
            )
            
            # Obtener content type
            content_type = ContentType.objects.get_for_model(PermisosBarra)
            
            # Permisos necesarios para cargue de maestras
            permisos_maestras = [
                'nav_bar',              # Ver menú
                'panel_actualizacion',  # Acceso al panel
                'cargue_maestras',      # Permiso específico
            ]
            
            permissions_added = 0
            for permiso_code in permisos_maestras:
                try:
                    permission = Permission.objects.get(
                        codename=permiso_code,
                        content_type=content_type
                    )
                    user.user_permissions.add(permission)
                    permissions_added += 1
                    self.stdout.write(
                        self.style.SUCCESS(f'  ✅ Permiso asignado: {permiso_code}')
                    )
                except Permission.DoesNotExist:
                    self.stdout.write(
                        self.style.ERROR(f'  ❌ Permiso no encontrado: {permiso_code}')
                    )
            
            user.save()
            self.stdout.write(
                self.style.SUCCESS(f'🎉 {permissions_added} permisos asignados a {username}')
            )
            self.stdout.write(
                self.style.SUCCESS('📋 El usuario ahora puede acceder a "Cargue Maestras"')
            )
            
        except User.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f'❌ Usuario no encontrado: {username}')
            )
            self.stdout.write('💡 Usuarios disponibles:')
            for u in User.objects.all()[:10]:
                self.stdout.write(f'   - {u.username} ({u.get_full_name() or "Sin nombre"})')