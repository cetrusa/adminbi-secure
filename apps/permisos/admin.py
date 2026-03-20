from django.contrib import admin
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django.urls import reverse
from .models import (
    ConfDt, ConfEmpresas, ConfServer, ConfSql, ConfTipo,
    ConfSqlCdt, CdtEnvio,
    ConfSqlTsol, TsolEnvio,
    ConfSqlCosmos, CosmosEnvio,
)


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

    list_display = (
        "id",
        "name_display",
        "nmEmpresa",
        "es_bimbo_badge",
        "envio_email_badge",
        "idProveedorBimbo",
        "display_actions",
    )
    list_display_links = ("id", "name_display")
    search_fields = ("name", "nmEmpresa")
    list_filter = ("es_bimbo", "envio_email_activo", "estado")
    list_per_page = 25

    def name_display(self, obj):
        """Resalta el nombre de la empresa."""
        return format_html("<strong>{}</strong>", obj.name)

    name_display.short_description = _("Nombre")
    name_display.admin_order_field = "name"

    def es_bimbo_badge(self, obj):
        if obj.es_bimbo:
            return format_html(
                '<span style="color:white;background:#28a745;padding:3px 8px;border-radius:4px;">SI</span>'
            )
        return format_html(
            '<span style="color:#666;background:#eee;padding:3px 8px;border-radius:4px;">NO</span>'
        )

    es_bimbo_badge.short_description = _("Bimbo")
    es_bimbo_badge.admin_order_field = "es_bimbo"

    def envio_email_badge(self, obj):
        if obj.envio_email_activo:
            return format_html(
                '<span style="color:white;background:#007bff;padding:3px 8px;border-radius:4px;">SI</span>'
            )
        return format_html(
            '<span style="color:#666;background:#eee;padding:3px 8px;border-radius:4px;">NO</span>'
        )

    envio_email_badge.short_description = _("Email")
    envio_email_badge.admin_order_field = "envio_email_activo"

    def display_actions(self, obj):
        """Muestra acciones para cada empresa."""
        buttons = []

        buttons.append(
            format_html(
                '<a class="button" href="{}" style="background-color: #447e9b; color: white; '
                'padding: 5px 10px; border-radius: 4px; text-decoration: none; margin-right: 5px;">'
                '<i class="fa fa-eye"></i> Ver</a>',
                reverse("admin:permisos_confempresas_change", args=[obj.pk]),
            )
        )

        return format_html(" ".join(buttons))

    display_actions.short_description = _("Acciones")

    fieldsets = (
        (
            _("Información General"),
            {
                "fields": ("id", "nmEmpresa", "name", "estado"),
            },
        ),
        (
            _("Conexión a Base de Datos"),
            {
                "fields": ("nbServerSidis", "dbSidis", "nbServerBi", "dbBi"),
                "description": _("Configuración de servidores y bases de datos origen/destino."),
            },
        ),
        (
            _("Configuración BIMBO"),
            {
                "fields": ("es_bimbo", "ceve", "idProveedorBimbo"),
                "description": _("Marque 'Es Bimbo' para habilitar la integración con BIMBO."),
            },
        ),
        (
            _("Reportes por Correo"),
            {
                "fields": ("envio_email_activo",),
                "description": _("Habilite para incluir esta empresa en el envío nocturno automático de reportes."),
            },
        ),
        (
            _("PowerBI"),
            {
                "fields": ("report_id_powerbi", "dataset_id_powerbi", "url_powerbi"),
                "classes": ("collapse",),
            },
        ),
        (
            _("Configuración CDT"),
            {
                "fields": (
                    "envio_cdt_activo", "planos_cdt",
                    "cdt_nombre_proveedor", "cdt_codigo_proveedor",
                    "cdt_codigos_distribuidor", "cdt_vendedores_especiales",
                    "cdt_bodega_especial", "cdt_conexion",
                ),
                "description": _("Configuración para generación de planos CDT: reglas de negocio y credenciales SFTP (JSON)."),
                "classes": ("collapse",),
            },
        ),
        (
            _("Configuración TSOL"),
            {
                "fields": (
                    "envio_tsol_activo", "planos_tsol",
                    "tsol_nombre", "tsol_codigo",
                    "tsol_filtro_proveedores", "tsol_bodega_to_code",
                    "tsol_code_to_sede", "tsol_sedes_permitidas",
                    "tsol_sede_default_code", "tsol_sede_default_name",
                    "tsol_conexion",
                ),
                "description": _("Configuración para generación de planos TSOL (TrackSales): mapeos y credenciales FTP (JSON)."),
                "classes": ("collapse",),
            },
        ),
        (
            _("Configuración Cosmos"),
            {
                "fields": (
                    "envio_cosmos_activo", "cosmos_empresa_id", "planos_cosmos",
                    "cosmos_conexion",
                ),
                "description": _("Configuración para generación de planos Cosmos y credenciales FTPS (JSON)."),
                "classes": ("collapse",),
            },
        ),
        (
            _("Procesos SQL"),
            {
                "fields": (
                    "txProcedureExtrae",
                    "txProcedureCargue",
                    "nmProcedureInterface",
                    "txProcedureInterface",
                    "nmProcedureExcel",
                    "txProcedureExcel",
                    "nmProcedureExcel2",
                    "txProcedureExcel2",
                    "nmProcedureCsv",
                    "txProcedureCsv",
                    "nmProcedureCsv2",
                    "txProcedureCsv2",
                    "nmProcedureSql",
                    "txProcedureSql",
                ),
                "classes": ("collapse",),
                "description": _("Configuración avanzada de procedimientos SQL."),
            },
        ),
    )
    readonly_fields = ("fecha_actualizacion",)

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        self._sync_agencia_bimbo(obj)

    def _sync_agencia_bimbo(self, empresa):
        """
        Sincroniza datos de conf_empresas hacia agencias_bimbo
        y proveedores_agencia_bimbo (multi-proveedor).
        """
        from apps.bimbo.models import AgenciaBimbo

        if not empresa.es_bimbo:
            AgenciaBimbo.objects.using("bimbo").filter(
                id_agente=empresa.id
            ).update(es_bimbo=False)
            return

        # Parsear CSV de proveedores (ej: "6,40" -> ["6", "40"])
        ids_csv = str(empresa.idProveedorBimbo or "").strip()
        prov_ids = [p.strip() for p in ids_csv.split(",") if p.strip()] if ids_csv else []

        # Sync agencias_bimbo: primer proveedor en campo legacy
        defaults = {
            "Nombre": empresa.nmEmpresa or empresa.name,
            "db_powerbi": empresa.name,
            "es_bimbo": True,
            "id_proveedor_bimbo": prov_ids[0] if prov_ids else None,
        }
        agencia, _ = AgenciaBimbo.objects.using("bimbo").update_or_create(
            id_agente=empresa.id,
            defaults=defaults,
        )

        # Sync proveedores_agencia_bimbo: insertar cada ID
        if prov_ids:
            self._sync_proveedores_junction(agencia.id, prov_ids)

    def _sync_proveedores_junction(self, id_agencia, prov_ids):
        """Inserta/actualiza proveedores en proveedores_agencia_bimbo via SQL."""
        try:
            from apps.bimbo.permissions import _get_bimbo_engine
            from sqlalchemy import text

            engine = _get_bimbo_engine()
            sql = text("""
                INSERT INTO powerbi_bimbo.proveedores_agencia_bimbo
                    (id_agencia, id_proveedor_sidis, nm_proveedor_sidis,
                     es_confirmado, fecha_confirmacion, confirmado_por)
                VALUES (:id_ag, :id_prov, 'SYNC_ADMIN', 1, NOW(), 'ADMIN')
                ON DUPLICATE KEY UPDATE
                    confirmado_por = 'ADMIN'
            """)
            with engine.connect() as conn:
                for id_prov in prov_ids:
                    conn.execute(sql, {"id_ag": id_agencia, "id_prov": id_prov})
                conn.commit()
        except Exception as exc:
            import logging
            logging.getLogger(__name__).error(
                "Error sincronizando proveedores junction para agencia %s: %s",
                id_agencia, exc,
            )


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


# ══════════════════════════════════════════════════════════════════
# Admin CDT
# ══════════════════════════════════════════════════════════════════


@admin.register(ConfSqlCdt)
class ConfSqlCdtAdmin(admin.ModelAdmin):
    """Administrador para configuración de SQL CDT."""

    list_display = ("nbSql", "nmReporte", "txTabla", "get_sql_preview")
    search_fields = ("nmReporte", "txDescripcion", "txTabla")
    list_per_page = 20

    fieldsets = (
        (
            None,
            {
                "fields": ("nbSql", "nmReporte", "txTabla", "txDescripcion"),
            },
        ),
        (
            _("SQL de Extracción"),
            {
                "fields": ("txSqlExtrae",),
                "description": _(
                    "Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin). "
                    "Los datos se extraen de la BD BI de la empresa."
                ),
            },
        ),
    )

    def get_sql_preview(self, obj):
        if obj.txSqlExtrae:
            preview = obj.txSqlExtrae[:80]
            return format_html("<code>{}</code>…", preview) if len(obj.txSqlExtrae) > 80 else format_html("<code>{}</code>", preview)
        return "-"

    get_sql_preview.short_description = _("Vista previa SQL")


@admin.register(CdtEnvio)
class CdtEnvioAdmin(admin.ModelAdmin):
    """Administrador para historial de envíos CDT (solo lectura)."""

    list_display = (
        "fecha_ejecucion",
        "empresa",
        "periodo_display",
        "estado_badge",
        "total_ventas",
        "total_clientes",
        "total_inventario",
        "enviado_sftp_badge",
    )
    list_filter = ("estado", "enviado_sftp", "empresa")
    search_fields = ("empresa__name", "empresa__nmEmpresa")
    date_hierarchy = "fecha_ejecucion"
    list_per_page = 25
    readonly_fields = (
        "empresa", "fecha_inicio", "fecha_fin",
        "estado", "total_ventas", "total_clientes", "total_inventario",
        "archivos_generados", "archivo_descarga", "enviado_sftp",
        "log_ejecucion", "usuario", "fecha_ejecucion", "fecha_actualizacion",
    )

    def periodo_display(self, obj):
        return f"{obj.fecha_inicio} → {obj.fecha_fin}"

    periodo_display.short_description = _("Periodo")

    def estado_badge(self, obj):
        colors = {
            "pendiente": "#ffa800",
            "procesando": "#3699ff",
            "enviado": "#28a745",
            "error": "#f64e60",
        }
        color = colors.get(obj.estado, "#666")
        return format_html(
            '<span style="color:white;background:{};padding:3px 8px;border-radius:4px;">{}</span>',
            color, obj.get_estado_display(),
        )

    estado_badge.short_description = _("Estado")
    estado_badge.admin_order_field = "estado"

    def enviado_sftp_badge(self, obj):
        if obj.enviado_sftp:
            return format_html('<span style="color:green;">✓</span>')
        return format_html('<span style="color:#ccc;">—</span>')

    enviado_sftp_badge.short_description = _("SFTP")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

# ══════════════════════════════════════════════════════════════════
# Admin TSOL
# ══════════════════════════════════════════════════════════════════


@admin.register(ConfSqlTsol)
class ConfSqlTsolAdmin(admin.ModelAdmin):
    """Administrador para configuración de SQL TSOL."""

    list_display = ("nbSql", "nmReporte", "txTabla", "get_sql_preview")
    search_fields = ("nmReporte", "txDescripcion", "txTabla")
    list_per_page = 20

    fieldsets = (
        (
            None,
            {
                "fields": ("nbSql", "nmReporte", "txTabla", "txDescripcion"),
            },
        ),
        (
            _("SQL de Extracción"),
            {
                "fields": ("txSqlExtrae",),
                "description": _(
                    "Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin). "
                    "Los datos se extraen de la BD BI de la empresa."
                ),
            },
        ),
    )

    def get_sql_preview(self, obj):
        if obj.txSqlExtrae:
            preview = obj.txSqlExtrae[:80]
            if len(obj.txSqlExtrae) > 80:
                return format_html("<code>{}</code>…", preview)
            return format_html("<code>{}</code>", preview)
        return "-"

    get_sql_preview.short_description = _("Vista previa SQL")


@admin.register(TsolEnvio)
class TsolEnvioAdmin(admin.ModelAdmin):
    """Administrador para historial de envíos TSOL (solo lectura)."""

    list_display = (
        "fecha_ejecucion",
        "empresa",
        "periodo_display",
        "estado_badge",
        "total_ventas",
        "total_clientes",
        "total_productos",
        "total_vendedores",
        "total_inventario",
        "enviado_ftp_badge",
    )
    list_filter = ("estado", "enviado_ftp", "empresa")
    search_fields = ("empresa__name", "empresa__nmEmpresa")
    date_hierarchy = "fecha_ejecucion"
    list_per_page = 25
    readonly_fields = (
        "empresa", "fecha_inicio", "fecha_fin",
        "estado", "total_ventas", "total_clientes", "total_productos",
        "total_vendedores", "total_inventario",
        "archivos_generados", "archivo_descarga", "enviado_ftp",
        "log_ejecucion", "usuario", "fecha_ejecucion", "fecha_actualizacion",
    )

    def periodo_display(self, obj):
        return f"{obj.fecha_inicio} → {obj.fecha_fin}"

    periodo_display.short_description = _("Periodo")

    def estado_badge(self, obj):
        colors = {
            "pendiente": "#ffa800",
            "procesando": "#3699ff",
            "enviado": "#28a745",
            "error": "#f64e60",
        }
        color = colors.get(obj.estado, "#666")
        return format_html(
            '<span style="color:white;background:{};padding:3px 8px;border-radius:4px;">{}</span>',
            color, obj.get_estado_display(),
        )

    estado_badge.short_description = _("Estado")
    estado_badge.admin_order_field = "estado"

    def enviado_ftp_badge(self, obj):
        if obj.enviado_ftp:
            return format_html('<span style="color:green;">✓</span>')
        return format_html('<span style="color:#ccc;">—</span>')

    enviado_ftp_badge.short_description = _("FTP")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


# ══════════════════════════════════════════════════════════════════
# Admin Cosmos
# ══════════════════════════════════════════════════════════════════


@admin.register(ConfSqlCosmos)
class ConfSqlCosmosAdmin(admin.ModelAdmin):
    """Administrador para configuración de SQL Cosmos."""

    list_display = ("nbSql", "nmReporte", "txTabla", "get_sql_preview")
    search_fields = ("nmReporte", "txDescripcion", "txTabla")
    list_per_page = 20

    fieldsets = (
        (
            None,
            {
                "fields": ("nbSql", "nmReporte", "txTabla", "txDescripcion"),
            },
        ),
        (
            _("SQL de Extracción"),
            {
                "fields": ("txSqlExtrae",),
                "description": _(
                    "Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin), "
                    ":IdDs (ID empresa Cosmos)."
                ),
            },
        ),
    )

    def get_sql_preview(self, obj):
        if obj.txSqlExtrae:
            preview = obj.txSqlExtrae[:80]
            if len(obj.txSqlExtrae) > 80:
                return format_html("<code>{}</code>…", preview)
            return format_html("<code>{}</code>", preview)
        return "-"

    get_sql_preview.short_description = _("Vista previa SQL")


@admin.register(CosmosEnvio)
class CosmosEnvioAdmin(admin.ModelAdmin):
    """Administrador para historial de envíos Cosmos (solo lectura)."""

    list_display = (
        "fecha_ejecucion",
        "empresa",
        "periodo_display",
        "estado_badge",
        "total_registros",
        "enviado_ftps_badge",
    )
    list_filter = ("estado", "enviado_ftps", "empresa")
    search_fields = ("empresa__name", "empresa__nmEmpresa")
    date_hierarchy = "fecha_ejecucion"
    list_per_page = 25
    readonly_fields = (
        "empresa", "fecha_inicio", "fecha_fin",
        "estado", "total_registros",
        "archivos_generados", "archivo_descarga", "enviado_ftps",
        "log_ejecucion", "usuario", "fecha_ejecucion", "fecha_actualizacion",
    )

    def periodo_display(self, obj):
        return f"{obj.fecha_inicio} → {obj.fecha_fin}"

    periodo_display.short_description = _("Periodo")

    def estado_badge(self, obj):
        colors = {
            "pendiente": "#ffa800",
            "procesando": "#3699ff",
            "enviado": "#28a745",
            "error": "#f64e60",
        }
        color = colors.get(obj.estado, "#666")
        return format_html(
            '<span style="color:white;background:{};padding:3px 8px;border-radius:4px;">{}</span>',
            color, obj.get_estado_display(),
        )

    estado_badge.short_description = _("Estado")
    estado_badge.admin_order_field = "estado"

    def enviado_ftps_badge(self, obj):
        if obj.enviado_ftps:
            return format_html('<span style="color:green;">✓</span>')
        return format_html('<span style="color:#ccc;">—</span>')

    enviado_ftps_badge.short_description = _("FTPS")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


# Personalización del sitio de administración
admin.site.site_header = _("DataZenith BI - Administración")
admin.site.site_title = _("DataZenith BI")
admin.site.index_title = _("Panel de Control")
