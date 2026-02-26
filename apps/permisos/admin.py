from django.contrib import admin
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django.urls import reverse
from .models import ConfDt, ConfEmpresas, ConfServer, ConfSql, ConfTipo


@admin.register(ConfDt)
class ConfDtAdmin(admin.ModelAdmin):
    """Administrador para configuración de rangos de fechas."""

    def get_verbose_fields(self, obj):
        """Muestra todos los campos con valores en una línea legible."""
        fields = []
        for field in obj._meta.fields:
            value = getattr(obj, field.name)
            if value and field.name != "id":  # Excluir el ID para claridad
                fields.append(
                    format_html(
                        "<strong>{}:</strong> {}", field.verbose_name, str(value)
                    )
                )
        return format_html(
            '<div style="line-height: 1.5em;">{}</div>', " | ".join(fields)
        )

    get_verbose_fields.short_description = _("Rangos de Fechas")

    list_display = ("get_verbose_fields",)
    search_fields = ("txDtIni", "txDtFin")
    list_per_page = 20

    fieldsets = (
        (
            None,
            {
                "fields": ("txDtIni", "txDtFin"),
                "description": _(
                    "Configure los rangos de fechas para los datos que se mostrarán en el sistema."
                ),
            },
        ),
    )


@admin.register(ConfEmpresas)
class ConfEmpresasAdmin(admin.ModelAdmin):
    """Administrador para configuración de empresas."""

    list_display = ("id", "name_display", "nmEmpresa", "get_actions")
    list_display_links = ("id", "name_display")
    search_fields = ("name", "nmEmpresa")
    list_filter = ("activo",) if hasattr(ConfEmpresas, "activo") else ()
    list_per_page = 25

    def name_display(self, obj):
        """Resalta el nombre de la empresa."""
        return format_html("<strong>{}</strong>", obj.name)

    name_display.short_description = _("Nombre")
    name_display.admin_order_field = "name"

    def get_actions(self, obj):
        """Muestra acciones para cada empresa."""
        buttons = []

        # Botón para ver detalles (ajusta los enlaces según las URLs de tu aplicación)
        buttons.append(
            format_html(
                '<a class="button" href="{}" style="background-color: #447e9b; color: white; '
                'padding: 5px 10px; border-radius: 4px; text-decoration: none; margin-right: 5px;">'
                '<i class="fa fa-eye"></i> Ver</a>',
                reverse("admin:permisos_confempresas_change", args=[obj.pk]),
            )
        )

        return format_html(" ".join(buttons))

    get_actions.short_description = _("Acciones")


@admin.register(ConfServer)
class ConfServerAdmin(admin.ModelAdmin):
    """Administrador para configuración de servidores."""

    list_display = ("nbServer", "nmServer", "get_status")
    search_fields = ("nbServer", "nmServer")
    list_per_page = 20

    def get_status(self, obj):
        """Muestra estado del servidor - Este es un método de ejemplo, ajusta según tu lógica."""
        # Implementa tu propia lógica para determinar si el servidor está activo
        is_active = True  # Por defecto asumimos que está activo

        if is_active:
            return format_html('<span style="color: green;">●</span> Activo')
        else:
            return format_html('<span style="color: red;">●</span> Inactivo')

    get_status.short_description = _("Estado")


@admin.register(ConfSql)
class ConfSqlAdmin(admin.ModelAdmin):
    """Administrador para configuración de SQL."""

    list_display = ("txDescripcion", "get_sql_preview")
    search_fields = ("txDescripcion", "txSql")
    list_per_page = 15

    def get_sql_preview(self, obj):
        """Muestra una vista previa del SQL con formato."""
        if hasattr(obj, "txSql") and obj.txSql:
            if len(obj.txSql) > 70:
                return format_html("<code>{}</code>...", obj.txSql[:70])
            return format_html("<code>{}</code>", obj.txSql)
        return "-"

    get_sql_preview.short_description = _("Vista previa SQL")


@admin.register(ConfTipo)
class ConfTipoAdmin(admin.ModelAdmin):
    """Administrador para configuración de tipos."""

    list_display = ("nbTipo", "get_description")
    search_fields = ("nbTipo",)
    list_per_page = 20

    def get_description(self, obj):
        """Muestra descripción si existe."""
        if hasattr(obj, "txDescripcion") and obj.txDescripcion:
            return obj.txDescripcion
        return "-"

    get_description.short_description = _("Descripción")


# Registros de modelos (Ya no son necesarios porque usamos decoradores)
# admin.site.register(ConfDt, ConfDtAdmin)
# admin.site.register(ConfEmpresas, ConfEmpresasAdmin)
# admin.site.register(ConfServer, ConfServerAdmin)
# admin.site.register(ConfSql, ConfSqlAdmin)
# admin.site.register(ConfTipo, ConfTipoAdmin)

# Personalización del sitio de administración
admin.site.site_header = _("DataZenith BI - Administración")
admin.site.site_title = _("DataZenith BI")
admin.site.index_title = _("Panel de Control")
