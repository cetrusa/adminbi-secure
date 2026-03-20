# Generated manually for TSOL models

import django.core.validators
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        (
            "permisos",
            "0020_confcdtproveedor_alter_permisosbarra_options_and_more",
        ),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # ── ConfSqlTsol ────────────────────────────────────────────
        migrations.CreateModel(
            name="ConfSqlTsol",
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
                        help_text="Identificador del reporte TSOL (ej: ventas_tsol, clientes_tsol, productos_tsol)",
                        max_length=100,
                        verbose_name="Nombre del Reporte",
                    ),
                ),
                (
                    "txTabla",
                    models.CharField(
                        blank=True,
                        help_text="Tabla BI fuente (ej: cuboventas, clientes, productos, inventario)",
                        max_length=100,
                        null=True,
                        verbose_name="Tabla Fuente",
                    ),
                ),
                (
                    "txDescripcion",
                    models.CharField(
                        blank=True,
                        help_text="Descripción del propósito de esta consulta TSOL",
                        max_length=255,
                        null=True,
                        verbose_name="Descripción",
                    ),
                ),
                (
                    "txSqlExtrae",
                    models.TextField(
                        blank=True,
                        help_text="Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin)",
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
                "verbose_name": "Config SQL TSOL",
                "verbose_name_plural": "Config SQL TSOL",
                "db_table": "conf_sql_tsol",
                "ordering": ["nbSql"],
                "indexes": [
                    models.Index(
                        fields=["nmReporte"],
                        name="sqltsol_nmreporte_idx",
                    ),
                ],
            },
        ),
        # ── ConfTsolProveedor ──────────────────────────────────────
        migrations.CreateModel(
            name="ConfTsolProveedor",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "nombre",
                    models.CharField(
                        help_text="Nombre legible (ej: Distrijass Cali)",
                        max_length=100,
                        verbose_name="Nombre Proveedor TSOL",
                    ),
                ),
                (
                    "codigo",
                    models.CharField(
                        help_text="Código para el nombre del ZIP (ej: DISTRIJASS_211688)",
                        max_length=100,
                        verbose_name="Código TSOL",
                    ),
                ),
                (
                    "filtro_proveedores",
                    models.TextField(
                        blank=True,
                        help_text='JSON array de nombres de proveedor para filtrar ventas/inventario. Vacío = todos. Ej: ["023-COLGATE","024-PAPELES","018-COLOMBIANA"]',
                        null=True,
                        verbose_name="Filtro de Proveedores",
                    ),
                ),
                (
                    "bodega_to_code",
                    models.TextField(
                        blank=True,
                        help_text='JSON dict nombre_bodega -> código_sede. Ej: {"CALI":"01","PALMIRA":"01","TULUA":"02","POPAYAN":"04","PEREIRA":"06"}',
                        null=True,
                        verbose_name="Mapeo Bodega a Código",
                    ),
                ),
                (
                    "sedes_permitidas",
                    models.CharField(
                        blank=True,
                        help_text="Códigos de sedes permitidos separados por coma (ej: 01,04,06). Vacío = todas las sedes.",
                        max_length=200,
                        null=True,
                        verbose_name="Sedes Permitidas",
                    ),
                ),
                (
                    "sede_default_code",
                    models.CharField(
                        default="01",
                        max_length=10,
                        verbose_name="Código Sede Default",
                    ),
                ),
                (
                    "sede_default_name",
                    models.CharField(
                        default="PALMIRA/CALI",
                        max_length=100,
                        verbose_name="Nombre Sede Default",
                    ),
                ),
                (
                    "code_to_sede",
                    models.TextField(
                        blank=True,
                        help_text='JSON dict código_sede -> nombre_sede. Ej: {"01":"PALMIRA/CALI","02":"TULUÁ","04":"POPAYÁN","06":"PEREIRA"}',
                        null=True,
                        verbose_name="Mapeo Código a Nombre Sede",
                    ),
                ),
                (
                    "ftp_host",
                    models.CharField(
                        help_text="Servidor FTP (ej: apps.grupobit.net)",
                        max_length=200,
                        verbose_name="FTP Host",
                    ),
                ),
                (
                    "ftp_port",
                    models.IntegerField(
                        default=21, verbose_name="FTP Puerto"
                    ),
                ),
                (
                    "ftp_user",
                    models.CharField(
                        max_length=100, verbose_name="FTP Usuario"
                    ),
                ),
                (
                    "ftp_pass",
                    models.CharField(
                        max_length=200, verbose_name="FTP Contraseña"
                    ),
                ),
                (
                    "ftp_ruta_remota",
                    models.CharField(
                        default="/",
                        help_text="Directorio remoto donde se depositan los archivos",
                        max_length=200,
                        verbose_name="Ruta Remota FTP",
                    ),
                ),
                (
                    "activo",
                    models.BooleanField(
                        default=True, verbose_name="Activo"
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
                "verbose_name": "Proveedor TSOL",
                "verbose_name_plural": "Proveedores TSOL",
                "db_table": "conf_tsol_proveedor",
                "ordering": ["nombre"],
            },
        ),
        # ── TsolEnvio ─────────────────────────────────────────────
        migrations.CreateModel(
            name="TsolEnvio",
            fields=[
                (
                    "id",
                    models.BigAutoField(
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
                    "total_ventas",
                    models.IntegerField(
                        default=0, verbose_name="Total Ventas"
                    ),
                ),
                (
                    "total_clientes",
                    models.IntegerField(
                        default=0, verbose_name="Total Clientes"
                    ),
                ),
                (
                    "total_productos",
                    models.IntegerField(
                        default=0, verbose_name="Total Productos"
                    ),
                ),
                (
                    "total_vendedores",
                    models.IntegerField(
                        default=0, verbose_name="Total Vendedores"
                    ),
                ),
                (
                    "total_inventario",
                    models.IntegerField(
                        default=0, verbose_name="Total Inventario"
                    ),
                ),
                (
                    "archivos_generados",
                    models.TextField(
                        blank=True,
                        help_text="JSON con lista de archivos generados y sus tamaños",
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
                    "enviado_ftp",
                    models.BooleanField(
                        default=False, verbose_name="Enviado por FTP"
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
                        related_name="envios_tsol",
                        to="permisos.confempresas",
                        verbose_name="Empresa",
                    ),
                ),
                (
                    "proveedor",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="envios",
                        to="permisos.conftsolproveedor",
                        verbose_name="Proveedor",
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
                "verbose_name": "Envío TSOL",
                "verbose_name_plural": "Envíos TSOL",
                "db_table": "tsol_envio",
                "ordering": ["-fecha_ejecucion"],
                "indexes": [
                    models.Index(
                        fields=["estado"], name="tsolenvio_estado_idx"
                    ),
                    models.Index(
                        fields=["fecha_inicio", "fecha_fin"],
                        name="tsolenvio_periodo_idx",
                    ),
                ],
                "permissions": [
                    (
                        "ejecutar_tsol",
                        "Puede ejecutar generación de planos TSOL",
                    ),
                    (
                        "reenviar_tsol",
                        "Puede re-enviar planos TSOL por FTP",
                    ),
                ],
            },
        ),
        # ── Campos TSOL en ConfEmpresas ────────────────────────────
        migrations.AddField(
            model_name="confempresas",
            name="envio_tsol_activo",
            field=models.BooleanField(
                default=False,
                help_text="Habilita el envío nocturno automático de planos TSOL para esta empresa",
                verbose_name="Envío TSOL activo",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="proveedor_tsol",
            field=models.ForeignKey(
                blank=True,
                help_text="Proveedor TSOL asociado a esta empresa",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                to="permisos.conftsolproveedor",
                verbose_name="Proveedor TSOL",
            ),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="planos_tsol",
            field=models.CharField(
                blank=True,
                help_text="Lista de IDs de conf_sql_tsol (ej: [1,2,3,4,5,6,7,8,9,10,11])",
                max_length=200,
                null=True,
                verbose_name="Planos TSOL (IDs)",
            ),
        ),
        # ── Permisos actualizados ──────────────────────────────────
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
                    (
                        "reportes_bimbo",
                        "Puede ver Reportes Bimbo (Venta Cero, Ruteros)",
                    ),
                    (
                        "reporte_preventa_bimbo",
                        "Puede ver Reporte Preventa Bimbo",
                    ),
                    ("cargue_infoventas", "Cargar Archivo Infoventas"),
                    ("cargue_maestras", "Cargar Tablas Maestras"),
                    ("cargue_infoproducto", "Cargar Información de Producto"),
                    (
                        "cargue_infoproveedor",
                        "Cargar Información de Proveedor",
                    ),
                    ("faltantes", "Generar informe de Faltantes"),
                    ("preventa", "Generar informe de Preventa"),
                    (
                        "trazabilidad",
                        "Generar informe de Trazabilidad Preventa",
                    ),
                    (
                        "config_email_reportes",
                        "Configurar correos para reportes programados",
                    ),
                    ("ejecutar_tsol", "Ejecutar planos TSOL"),
                ),
                "verbose_name": "Permiso",
                "verbose_name_plural": "Permisos",
            },
        ),
    ]
