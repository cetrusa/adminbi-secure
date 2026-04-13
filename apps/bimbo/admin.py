from django.contrib import admin
from django.core.cache import cache
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _

from .models import AgenciaBimbo, PermisoBimboAgente


@admin.register(AgenciaBimbo)
class AgenciaBimboAdmin(admin.ModelAdmin):
    """Administración de agencias BIMBO en powerbi_bimbo.agencias_bimbo."""

    list_display = (
        "CEVE",
        "Nombre",
        "db_powerbi",
        "estado_badge",
        "es_bimbo",
        "id_proveedor_bimbo",
        "id_proveedor_fvp",
        "fecha_ultimo_snapshot",
    )
    list_display_links = ("CEVE", "Nombre")
    list_filter = ("estado", "es_bimbo")
    list_editable = ("es_bimbo", "id_proveedor_bimbo")
    search_fields = ("Nombre", "CEVE", "db_powerbi")
    readonly_fields = ("fecha_ultimo_snapshot", "fecha_alta")
    list_per_page = 25

    fieldsets = (
        (
            _("Identificación"),
            {"fields": ("id_agente", "Nombre", "CEVE")},
        ),
        (
            _("Configuración BIMBO"),
            {
                "fields": (
                    "es_bimbo",
                    "id_proveedor_bimbo",
                    "id_proveedor_fvp",
                    "db_powerbi",
                    "estado",
                ),
            },
        ),
        (
            _("SIDIS"),
            {
                "fields": ("nbOficinaV", "SIDIS", "nmOficinaV"),
                "classes": ("collapse",),
            },
        ),
        (
            _("Auditoría"),
            {
                "fields": ("fecha_alta", "fecha_ultimo_snapshot"),
                "classes": ("collapse",),
            },
        ),
    )

    def estado_badge(self, obj):
        colors = {
            "ACTIVO": "#28a745",
            "PENDIENTE": "#ffc107",
            "INACTIVO": "#dc3545",
        }
        color = colors.get(obj.estado, "#6c757d")
        return format_html(
            '<span style="background:{}; color:#fff; padding:3px 8px; '
            'border-radius:4px; font-size:0.8em;">{}</span>',
            color,
            obj.estado,
        )

    estado_badge.short_description = _("Estado")
    estado_badge.admin_order_field = "estado"


class PermisoBimboAgenteInline(admin.TabularInline):
    """Inline para asignar agencias BIMBO a un usuario desde su edición."""

    model = PermisoBimboAgente
    extra = 1
    fields = ("agencia_id", "puede_ejecutar", "puede_editar")


@admin.register(PermisoBimboAgente)
class PermisoBimboAgenteAdmin(admin.ModelAdmin):
    """Administración de permisos por agente BIMBO."""

    list_display = (
        "user",
        "agencia_id",
        "agencia_nombre",
        "puede_ejecutar",
        "puede_editar",
    )
    list_filter = ("puede_ejecutar", "puede_editar")
    list_editable = ("puede_ejecutar", "puede_editar")
    search_fields = ("user__username", "user__nombres")
    autocomplete_fields = ["user"]
    list_select_related = ("user",)
    list_per_page = 50

    def _get_agencias_map(self):
        cache_key = "admin_bimbo_agencias_nombre_map"
        agencias_map = cache.get(cache_key)
        if agencias_map is None:
            agencias_map = dict(
                AgenciaBimbo.objects.using("bimbo").values_list("id", "Nombre")
            )
            cache.set(cache_key, agencias_map, 300)
        return agencias_map

    def agencia_nombre(self, obj):
        return self._get_agencias_map().get(obj.agencia_id, f"(id={obj.agencia_id})")

    agencia_nombre.short_description = _("Nombre Agencia")

