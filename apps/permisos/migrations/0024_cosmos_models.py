import django.core.validators
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0023_confempresas_sftp_cdt_host_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # ── Campos Cosmos en ConfEmpresas ─────────────────────────────
        migrations.AddField(
            model_name="confempresas",
            name="envio_cosmos_activo",
            field=models.BooleanField(
                default=False,
                help_text="Habilita el envío nocturno automático de planos Cosmos para esta empresa",
                verbose_name="Envío Cosmos activo",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="cosmos_empresa_id",
            field=models.CharField(
                blank=True,
                help_text="Identificador Cosmos de la empresa (ej: CO-CBIA-DTR-0093)",
                max_length=100,
                null=True,
                verbose_name="ID Empresa Cosmos",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="planos_cosmos",
            field=models.CharField(
                blank=True,
                help_text="Lista de IDs de conf_sql_cosmos (ej: [1,2,3])",
                max_length=200,
                null=True,
                verbose_name="Planos Cosmos (IDs)",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="ftps_cosmos_host",
            field=models.CharField(
                blank=True,
                help_text="Servidor FTPS para envío Cosmos",
                max_length=200,
                null=True,
                verbose_name="FTPS Host Cosmos",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="ftps_cosmos_port",
            field=models.IntegerField(
                blank=True,
                default=990,
                null=True,
                verbose_name="FTPS Puerto Cosmos",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="ftps_cosmos_user",
            field=models.CharField(
                blank=True,
                max_length=100,
                null=True,
                verbose_name="FTPS Usuario Cosmos",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="ftps_cosmos_pass",
            field=models.CharField(
                blank=True,
                max_length=200,
                null=True,
                verbose_name="FTPS Contraseña Cosmos",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="ftps_cosmos_ruta_remota",
            field=models.CharField(
                blank=True,
                default="/",
                help_text="Directorio remoto donde se depositan los archivos",
                max_length=200,
                null=True,
                verbose_name="Ruta Remota FTPS Cosmos",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="ftps_cosmos_certificate",
            field=models.CharField(
                blank=True,
                help_text="Fingerprint del certificado SSL para la conexión FTPS",
                max_length=500,
                null=True,
                verbose_name="Fingerprint Certificado FTPS Cosmos",
            ),
        ),
        # ── Modelo ConfSqlCosmos ──────────────────────────────────────
        migrations.CreateModel(
            name="ConfSqlCosmos",
            fields=[
                (
                    "nbSql",
                    models.BigIntegerField(
                        primary_key=True,
                        serialize=False,
                        validators=[
                            django.core.validators.MinValueValidator(
                                1, "El ID debe ser un número positivo"
                            )
                        ],
                        verbose_name="ID",
                    ),
                ),
                (
                    "nmReporte",
                    models.CharField(
                        help_text="Nombre identificador del reporte Cosmos",
                        max_length=100,
                        verbose_name="Nombre del Reporte",
                    ),
                ),
                (
                    "txTabla",
                    models.CharField(
                        blank=True,
                        help_text="Tabla de donde se extraen los datos",
                        max_length=100,
                        null=True,
                        verbose_name="Tabla Fuente",
                    ),
                ),
                (
                    "txDescripcion",
                    models.CharField(
                        blank=True,
                        help_text="Descripción del propósito de esta consulta Cosmos",
                        max_length=255,
                        null=True,
                        verbose_name="Descripción",
                    ),
                ),
                (
                    "txSqlExtrae",
                    models.TextField(
                        blank=True,
                        help_text="Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin), :IdDs (empresa)",
                        null=True,
                        verbose_name="SQL de Extracción",
                    ),
                ),
                (
                    "fecha_creacion",
                    models.DateTimeField(
                        auto_now_add=True, blank=True, null=True
                    ),
                ),
                (
                    "fecha_actualizacion",
                    models.DateTimeField(
                        auto_now=True, blank=True, null=True
                    ),
                ),
            ],
            options={
                "verbose_name": "Config SQL Cosmos",
                "verbose_name_plural": "Config SQL Cosmos",
                "db_table": "conf_sql_cosmos",
                "ordering": ["nbSql"],
                "indexes": [
                    models.Index(
                        fields=["nmReporte"], name="sqlcosmos_nmreporte_idx"
                    ),
                ],
            },
        ),
        # ── Modelo CosmosEnvio ────────────────────────────────────────
        migrations.CreateModel(
            name="CosmosEnvio",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "fecha_inicio",
                    models.DateField(verbose_name="Fecha Inicio Periodo"),
                ),
                (
                    "fecha_fin",
                    models.DateField(verbose_name="Fecha Fin Periodo"),
                ),
                (
                    "estado",
                    models.CharField(
                        choices=[
                            ("pendiente", "Pendiente"),
                            ("procesando", "Procesando"),
                            ("enviado", "Enviado"),
                            ("error", "Error"),
                        ],
                        default="pendiente",
                        max_length=20,
                        verbose_name="Estado",
                    ),
                ),
                (
                    "total_registros",
                    models.IntegerField(
                        default=0, verbose_name="Total Registros"
                    ),
                ),
                (
                    "archivos_generados",
                    models.TextField(
                        blank=True,
                        help_text="JSON con lista de archivos generados",
                        null=True,
                        verbose_name="Archivos Generados",
                    ),
                ),
                (
                    "archivo_descarga",
                    models.CharField(
                        blank=True,
                        help_text="Ruta al archivo ZIP para descarga",
                        max_length=500,
                        null=True,
                        verbose_name="Archivo Descarga",
                    ),
                ),
                (
                    "enviado_ftps",
                    models.BooleanField(
                        default=False,
                        verbose_name="Enviado por FTPS",
                    ),
                ),
                (
                    "log_ejecucion",
                    models.TextField(
                        blank=True,
                        null=True,
                        verbose_name="Log de Ejecución",
                    ),
                ),
                (
                    "fecha_ejecucion",
                    models.DateTimeField(
                        auto_now_add=True,
                        verbose_name="Fecha de Ejecución",
                    ),
                ),
                (
                    "fecha_actualizacion",
                    models.DateTimeField(
                        auto_now=True,
                        verbose_name="Última Actualización",
                    ),
                ),
                (
                    "empresa",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="envios_cosmos",
                        to="permisos.confempresas",
                        verbose_name="Empresa",
                    ),
                ),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to=settings.AUTH_USER_MODEL,
                        verbose_name="Usuario",
                    ),
                ),
            ],
            options={
                "verbose_name": "Envío Cosmos",
                "verbose_name_plural": "Envíos Cosmos",
                "db_table": "cosmos_envio",
                "ordering": ["-fecha_ejecucion"],
                "indexes": [
                    models.Index(
                        fields=["estado"], name="cosmosenvio_estado_idx"
                    ),
                    models.Index(
                        fields=["fecha_inicio", "fecha_fin"],
                        name="cosmosenvio_periodo_idx",
                    ),
                ],
                "permissions": [
                    (
                        "ejecutar_cosmos",
                        "Puede ejecutar generación de planos Cosmos",
                    ),
                    (
                        "reenviar_cosmos",
                        "Puede re-enviar planos Cosmos por FTPS",
                    ),
                ],
            },
        ),
        # ── Actualizar permisos en PermisosBarra ─────────────────────
        migrations.AlterModelOptions(
            name="permisosbarra",
            options={
                "managed": False,
                "permissions": (
                    ("nav_bar", "Ver la barra de menú"),
                    ("panel_cubo", "Panel de cubo"),
                    ("panel_bi", "Panel de BI"),
                    ("panel_actualizacion", "Panel de Actualización de datos"),
                    ("panel_interface", "Panel de Interfaces Contables"),
                    ("cubo", "Generar cubo de ventas"),
                    ("proveedor", "Generar cubo de ventas para proveedor"),
                    ("matrix", "Generar Matrix de Ventas"),
                    ("interface", "Generar interface contable"),
                    ("interface_siigo", "Generar interface Siigo"),
                    ("plano", "Generar archivo plano"),
                    ("cargue_plano", "Cargar archivo plano"),
                    ("cargue_tsol", "Cargue archivo plano TSOL"),
                    ("informe_bi", "Informe BI"),
                    ("informe_bi_embed", "Informe BI Embed"),
                    ("actualizar_base", "Actualización de datos"),
                    ("actualizacion_bi", "Actualizar BI"),
                    ("admin", "Ir al Administrador"),
                    ("amovildesk", "Puede ver Informe Amovildesk"),
                    ("reportes", "Puede ver Reportes"),
                    ("reportes_bimbo", "Puede ver Reportes Bimbo (Venta Cero, Ruteros)"),
                    ("reporte_preventa_bimbo", "Puede ver Reporte Preventa Bimbo"),
                    ("cargue_infoventas", "Cargar Archivo Infoventas"),
                    ("cargue_maestras", "Cargar Tablas Maestras"),
                    ("cargue_infoproducto", "Cargar Información de Producto"),
                    ("cargue_infoproveedor", "Cargar Información de Proveedor"),
                    ("faltantes", "Generar informe de Faltantes"),
                    ("preventa", "Generar informe de Preventa"),
                    ("trazabilidad", "Generar informe de Trazabilidad Preventa"),
                    ("config_email_reportes", "Configurar correos para reportes programados"),
                    ("ejecutar_tsol", "Ejecutar planos TSOL"),
                    ("ejecutar_cosmos", "Ejecutar planos Cosmos"),
                ),
                "verbose_name": "Permiso",
                "verbose_name_plural": "Permisos",
            },
        ),
    ]
