from django.core.management.base import BaseCommand
from django.contrib.auth.models import User, Group, Permission
from django.contrib.contenttypes.models import ContentType
from apps.permisos.models import PermisosBarra


class Command(BaseCommand):
    help = 'Configura permisos automáticamente para los usuarios'

    def add_arguments(self, parser):
        parser.add_argument(
            '--user',
            type=str,
            help='Username específico para asignar permisos (opcional)',
        )
        parser.add_argument(
            '--create-groups',
            action='store_true',
            help='Crear grupos de permisos predefinidos',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('🚀 Configurando permisos del sistema...'))

        # Obtener content type para los permisos personalizados
        try:
            content_type = ContentType.objects.get_for_model(PermisosBarra)
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error obteniendo ContentType para PermisosBarra: {e}')
            )
            return

        # Definir permisos disponibles
        permisos_disponibles = [
            ('nav_bar', 'Ver la barra de menú'),
            ('panel_cubo', 'Panel de cubo'),
            ('panel_bi', 'Panel de BI'),
            ('panel_actualizacion', 'Panel de Actualización de datos'),
            ('panel_interface', 'Panel de Interfaces Contables'),
            ('cubo', 'Generar cubo de ventas'),
            ('proveedor', 'Generar cubo de ventas para proveedor'),
            ('matrix', 'Generar Matrix de Ventas'),
            ('interface', 'Generar interface contable'),
            ('interface_siigo', 'Generar interface Siigo'),
            ('plano', 'Generar archivo plano'),
            ('cargue_plano', 'Cargar archivo plano'),
            ('cargue_tsol', 'Cargue archivo plano TSOL'),
            ('informe_bi', 'Informe BI'),
            ('informe_bi_embed', 'Informe BI Embed'),
            ('actualizar_base', 'Actualización de datos'),
            ('actualizacion_bi', 'Actualizar BI'),
            ('admin', 'Ir al Administrador'),
            ('amovildesk', 'Puede ver Informe Amovildesk'),
            ('reportes', 'Puede ver Reportes'),
            ('reportes_bimbo', 'Puede ver Reportes Bimbo (Venta Cero, Ruteros)'),
            ('reporte_preventa_bimbo', 'Puede ver Reporte Preventa Bimbo'),
            ('cargue_infoventas', 'Cargar Archivo Infoventas'),
            ('cargue_maestras', 'Cargar Tablas Maestras'),
            ('cargue_infoproducto', 'Cargar Información de Producto'),
            ('cargue_infoproveedor', 'Cargar Información de Proveedor'),
            ('faltantes', 'Generar informe de Faltantes'),
            ('preventa', 'Generar informe de Preventa'),
            ('config_email_reportes', 'Configurar correos para reportes programados'),
        ]

        # Crear o actualizar permisos
        permisos_creados = []
        for codename, name in permisos_disponibles:
            permission, created = Permission.objects.get_or_create(
                codename=codename,
                content_type=content_type,
                defaults={'name': name}
            )
            if created:
                permisos_creados.append(codename)
                self.stdout.write(
                    self.style.SUCCESS(f'  ✅ Permiso creado: {codename} - {name}')
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'  ℹ️  Permiso ya existe: {codename}')
                )

        if permisos_creados:
            self.stdout.write(
                self.style.SUCCESS(f'🎉 Se crearon {len(permisos_creados)} nuevos permisos')
            )

        # Crear grupos predefinidos si se solicita
        if options['create_groups']:
            self.create_permission_groups(content_type)

        # Asignar permisos a usuario específico si se proporciona
        username = options.get('user')
        if username:
            self.assign_permissions_to_user(username, content_type)

        self.stdout.write(self.style.SUCCESS('✅ Configuración de permisos completada'))

    def create_permission_groups(self, content_type):
        """Crear grupos de permisos predefinidos"""
        self.stdout.write(self.style.SUCCESS('📁 Creando grupos de permisos...'))

        grupos_config = {
            'Administradores': [
                'nav_bar', 'admin', 'panel_cubo', 'panel_bi', 'panel_actualizacion',
                'panel_interface', 'cubo', 'proveedor', 'matrix', 'interface', 'interface_siigo',
                'plano', 'cargue_plano', 'cargue_tsol', 'informe_bi', 'informe_bi_embed',
                'actualizar_base', 'actualizacion_bi', 'amovildesk', 'reportes',
                'reportes_bimbo', 'reporte_preventa_bimbo',
                'cargue_infoventas', 'cargue_maestras', 'cargue_infoproducto', 'cargue_infoproveedor',
                'faltantes', 'preventa', 'config_email_reportes'
            ],
            'Usuarios BI': [
                'nav_bar', 'panel_cubo', 'panel_bi', 'informe_bi', 'informe_bi_embed',
                'cubo', 'proveedor', 'matrix', 'amovildesk', 'reportes',
                'faltantes', 'preventa'
            ],
            'Usuarios Cargue': [
                'nav_bar', 'panel_actualizacion', 'cargue_plano', 'cargue_tsol',
                'cargue_infoventas', 'cargue_maestras', 'cargue_infoproducto',
                'cargue_infoproveedor', 'actualizar_base'
            ],
            'Usuarios Interface': [
                'nav_bar', 'panel_interface', 'interface', 'interface_siigo', 'plano'
            ]
        }

        for grupo_name, permisos_list in grupos_config.items():
            grupo, created = Group.objects.get_or_create(name=grupo_name)
            
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f'  ✅ Grupo creado: {grupo_name}')
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'  ℹ️  Grupo ya existe: {grupo_name}')
                )

            # Asignar permisos al grupo
            for permiso_code in permisos_list:
                try:
                    permission = Permission.objects.get(
                        codename=permiso_code,
                        content_type=content_type
                    )
                    grupo.permissions.add(permission)
                except Permission.DoesNotExist:
                    self.stdout.write(
                        self.style.ERROR(f'    ❌ Permiso no encontrado: {permiso_code}')
                    )

            grupo.save()
            self.stdout.write(
                self.style.SUCCESS(f'    📋 {len(permisos_list)} permisos asignados a {grupo_name}')
            )

    def assign_permissions_to_user(self, username, content_type):
        """Asignar permisos específicos a un usuario"""
        try:
            user = User.objects.get(username=username)
            self.stdout.write(
                self.style.SUCCESS(f'👤 Asignando permisos a usuario: {username}')
            )

            # Permisos básicos para cualquier usuario
            permisos_basicos = [
                'nav_bar', 'panel_cubo', 'panel_bi', 'cubo', 'proveedor', 'informe_bi'
            ]

            # Si es superusuario, dar todos los permisos
            if user.is_superuser:
                permisos_basicos = [
                    'nav_bar', 'admin', 'panel_cubo', 'panel_bi', 'panel_actualizacion',
                    'panel_interface', 'cubo', 'proveedor', 'matrix', 'interface', 'interface_siigo',
                    'plano', 'cargue_plano', 'cargue_tsol', 'informe_bi', 'informe_bi_embed',
                    'actualizar_base', 'actualizacion_bi', 'amovildesk', 'reportes',
                    'reportes_bimbo', 'reporte_preventa_bimbo',
                    'cargue_infoventas', 'cargue_maestras', 'cargue_infoproducto',
                    'cargue_infoproveedor', 'faltantes', 'preventa', 'config_email_reportes'
                ]
                self.stdout.write(
                    self.style.SUCCESS(f'  🔑 Usuario {username} es superusuario - asignando todos los permisos')
                )

            permissions_added = 0
            for permiso_code in permisos_basicos:
                try:
                    permission = Permission.objects.get(
                        codename=permiso_code,
                        content_type=content_type
                    )
                    user.user_permissions.add(permission)
                    permissions_added += 1
                except Permission.DoesNotExist:
                    self.stdout.write(
                        self.style.ERROR(f'    ❌ Permiso no encontrado: {permiso_code}')
                    )

            user.save()
            self.stdout.write(
                self.style.SUCCESS(f'  ✅ {permissions_added} permisos asignados a {username}')
            )

        except User.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f'❌ Usuario no encontrado: {username}')
            )

    def list_available_permissions(self):
        """Listar permisos disponibles"""
        self.stdout.write(self.style.SUCCESS('📋 Permisos disponibles:'))
        content_type = ContentType.objects.get_for_model(PermisosBarra)
        permissions = Permission.objects.filter(content_type=content_type)
        
        for perm in permissions:
            self.stdout.write(f'  - {perm.codename}: {perm.name}')