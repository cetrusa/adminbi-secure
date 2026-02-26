from django import forms
from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from .models import Reporte


class ReporteAdminForm(forms.ModelForm):
    """Formulario personalizado para la administración de Reportes."""

    class Meta:
        model = Reporte
        fields = "__all__"
        widgets = {
            "nombre": forms.TextInput(
                attrs={"class": "vTextField", "style": "width: 400px;"}
            ),
            "descripcion": forms.Textarea(
                attrs={
                    "class": "vLargeTextField",
                    "style": "width: 600px; height: 100px;",
                }
            ),
            "sql_text": forms.Textarea(
                attrs={
                    "class": "vLargeTextField",
                    "style": "width: 600px; height: 300px; font-family: monospace;",
                    "spellcheck": "false",
                }
            ),
        }

    def clean_sql_text(self):
        """Validación adicional para el campo SQL."""
        sql_text = self.cleaned_data.get("sql_text")
        if sql_text and ("--" in sql_text or "/*" in sql_text):
            self.add_warning(
                _(
                    "La consulta contiene comentarios. Esto podría afectar la ejecución en algunos motores de base de datos."
                )
            )
        return sql_text

    def add_warning(self, message):
        """Agrega una advertencia sin bloquear el guardado del formulario."""
        if not hasattr(self, "warnings"):
            self.warnings = []
        self.warnings.append(message)


@admin.register(Reporte)
class ReporteAdmin(admin.ModelAdmin):
    """Configuración del administrador para el modelo Reporte."""

    form = ReporteAdminForm
    list_display = (
        "id",
        "nombre_display",
        "descripcion_truncada",
        "activo_icon",
        "fecha_actualizacion",
    )
    list_display_links = ("id", "nombre_display")
    list_filter = ("activo", "fecha_creacion", "fecha_actualizacion")
    search_fields = ("nombre", "descripcion", "sql_text")
    readonly_fields = ("fecha_creacion", "fecha_actualizacion")
    fieldsets = (
        (None, {"fields": ("nombre", "descripcion", "activo")}),
        (
            _("Consulta SQL"),
            {
                "fields": ("sql_text",),
                "classes": ("wide",),
                "description": _(
                    "Ingrese la consulta SQL utilizando sintaxis de SQLAlchemy. No incluya operaciones de escritura (INSERT, UPDATE, DELETE)."
                ),
            },
        ),
        (
            _("Información de sistema"),
            {
                "fields": ("fecha_creacion", "fecha_actualizacion"),
                "classes": ("collapse",),
            },
        ),
    )

    list_per_page = 25
    save_on_top = True

    def nombre_display(self, obj):
        """Muestra el nombre con formato para resaltar en el listado."""
        return format_html("<strong>{}</strong>", obj.nombre)

    nombre_display.short_description = _("Nombre")
    nombre_display.admin_order_field = "nombre"

    def descripcion_truncada(self, obj):
        """Trunca la descripción si es muy larga."""
        if obj.descripcion and len(obj.descripcion) > 100:
            return obj.descripcion[:97] + "..."
        return obj.descripcion

    descripcion_truncada.short_description = _("Descripción")

    def activo_icon(self, obj):
        """Muestra un icono según el estado activo/inactivo."""
        if obj.activo:
            return format_html('<span style="color:green;">✓</span>')
        return format_html('<span style="color:red;">✗</span>')

    activo_icon.short_description = _("Activo")
    activo_icon.admin_order_field = "activo"

    def save_model(self, request, obj, form, change):
        """Personaliza el guardado para mostrar mensajes."""
        super().save_model(request, obj, form, change)
        if hasattr(form, "warnings"):
            for warning in form.warnings:
                self.message_user(request, warning, level="WARNING")

    class Media:
        """Recursos adicionales para mejorar la interfaz de administración."""

        css = {"all": ("css/admin-extra.css",)}
        js = ("js/admin-reporte.js",)
