"""
Migración: Consolidar ConfCdtProveedor y ConfTsolProveedor en ConfEmpresas.
- Agrega campos de negocio CDT/TSOL y JSONFields de conexión a ConfEmpresas
- Copia datos desde las tablas proveedor a los nuevos campos
- Consolida credenciales SFTP/FTPS individuales en JSONFields
- Elimina campos obsoletos, FKs y modelos proveedor
"""

from django.db import migrations, models
import json


# ── Data migrations ──────────────────────────────────────────────

def migrate_cdt_proveedor_to_empresa(apps, schema_editor):
    """Copia datos de ConfCdtProveedor → campos CDT en ConfEmpresas."""
    ConfEmpresas = apps.get_model("permisos", "ConfEmpresas")
    ConfCdtProveedor = apps.get_model("permisos", "ConfCdtProveedor")

    for empresa in ConfEmpresas.objects.filter(proveedor_cdt__isnull=False):
        prov = empresa.proveedor_cdt
        empresa.cdt_nombre_proveedor = prov.nombre
        empresa.cdt_codigo_proveedor = prov.codigo_proveedor
        empresa.cdt_codigos_distribuidor = prov.codigos_distribuidor
        empresa.cdt_vendedores_especiales = prov.vendedores_especiales
        empresa.cdt_bodega_especial = prov.bodega_especial
        empresa.save(update_fields=[
            "cdt_nombre_proveedor", "cdt_codigo_proveedor",
            "cdt_codigos_distribuidor", "cdt_vendedores_especiales",
            "cdt_bodega_especial",
        ])


def migrate_tsol_proveedor_to_empresa(apps, schema_editor):
    """Copia datos de ConfTsolProveedor → campos TSOL en ConfEmpresas."""
    ConfEmpresas = apps.get_model("permisos", "ConfEmpresas")

    for empresa in ConfEmpresas.objects.filter(proveedor_tsol__isnull=False):
        prov = empresa.proveedor_tsol
        empresa.tsol_nombre = prov.nombre
        empresa.tsol_codigo = prov.codigo
        empresa.tsol_filtro_proveedores = prov.filtro_proveedores
        empresa.tsol_bodega_to_code = prov.bodega_to_code
        empresa.tsol_code_to_sede = prov.code_to_sede
        empresa.tsol_sedes_permitidas = prov.sedes_permitidas
        empresa.tsol_sede_default_code = prov.sede_default_code
        empresa.tsol_sede_default_name = prov.sede_default_name
        # Credenciales FTP → tsol_conexion JSON
        empresa.tsol_conexion = {
            "host": prov.ftp_host or "",
            "port": prov.ftp_port or 21,
            "user": prov.ftp_user or "",
            "pass": prov.ftp_pass or "",
            "ruta_remota": prov.ftp_ruta_remota or "/",
        }
        empresa.save(update_fields=[
            "tsol_nombre", "tsol_codigo", "tsol_filtro_proveedores",
            "tsol_bodega_to_code", "tsol_code_to_sede",
            "tsol_sedes_permitidas", "tsol_sede_default_code",
            "tsol_sede_default_name", "tsol_conexion",
        ])


def migrate_sftp_cdt_to_json(apps, schema_editor):
    """Consolida sftp_cdt_* → cdt_conexion JSON."""
    ConfEmpresas = apps.get_model("permisos", "ConfEmpresas")

    for empresa in ConfEmpresas.objects.all():
        host = getattr(empresa, "sftp_cdt_host", None) or ""
        if host:
            empresa.cdt_conexion = {
                "host": host,
                "port": getattr(empresa, "sftp_cdt_port", None) or 22,
                "user": getattr(empresa, "sftp_cdt_user", None) or "",
                "pass": getattr(empresa, "sftp_cdt_pass", None) or "",
                "ruta_remota": getattr(empresa, "sftp_cdt_ruta_remota", None) or "/",
            }
            empresa.save(update_fields=["cdt_conexion"])


def migrate_ftps_cosmos_to_json(apps, schema_editor):
    """Consolida ftps_cosmos_* → cosmos_conexion JSON."""
    ConfEmpresas = apps.get_model("permisos", "ConfEmpresas")

    for empresa in ConfEmpresas.objects.all():
        host = getattr(empresa, "ftps_cosmos_host", None) or ""
        if host:
            empresa.cosmos_conexion = {
                "host": host,
                "port": getattr(empresa, "ftps_cosmos_port", None) or 990,
                "user": getattr(empresa, "ftps_cosmos_user", None) or "",
                "pass": getattr(empresa, "ftps_cosmos_pass", None) or "",
                "ruta_remota": getattr(empresa, "ftps_cosmos_ruta_remota", None) or "/",
                "certificate": getattr(empresa, "ftps_cosmos_certificate", None) or "",
            }
            empresa.save(update_fields=["cosmos_conexion"])


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0025_populate_conf_sql_cdt"),
    ]

    operations = [
        # ═══ Fase 1: Agregar campos nuevos ═══════════════════════════

        # CDT campos de negocio
        migrations.AddField(
            model_name="confempresas",
            name="cdt_nombre_proveedor",
            field=models.CharField(blank=True, max_length=100, null=True, verbose_name="Nombre Proveedor CDT"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="cdt_codigo_proveedor",
            field=models.CharField(blank=True, max_length=100, null=True, verbose_name="Código Proveedor CDT"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="cdt_codigos_distribuidor",
            field=models.TextField(blank=True, null=True, verbose_name="Códigos Distribuidor CDT"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="cdt_vendedores_especiales",
            field=models.CharField(blank=True, max_length=200, null=True, verbose_name="Vendedores Especiales CDT"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="cdt_bodega_especial",
            field=models.CharField(blank=True, max_length=100, null=True, verbose_name="Patrón Bodega Especial CDT"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="cdt_conexion",
            field=models.JSONField(blank=True, default=dict, null=True, verbose_name="Conexión SFTP CDT"),
        ),

        # TSOL campos de negocio
        migrations.AddField(
            model_name="confempresas",
            name="tsol_nombre",
            field=models.CharField(blank=True, max_length=100, null=True, verbose_name="Nombre TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_codigo",
            field=models.CharField(blank=True, max_length=100, null=True, verbose_name="Código TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_filtro_proveedores",
            field=models.TextField(blank=True, null=True, verbose_name="Filtro Proveedores TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_bodega_to_code",
            field=models.TextField(blank=True, null=True, verbose_name="Mapeo Bodega→Código TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_code_to_sede",
            field=models.TextField(blank=True, null=True, verbose_name="Mapeo Código→Sede TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_sedes_permitidas",
            field=models.CharField(blank=True, max_length=200, null=True, verbose_name="Sedes Permitidas TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_sede_default_code",
            field=models.CharField(blank=True, default="01", max_length=10, null=True, verbose_name="Código Sede Default TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_sede_default_name",
            field=models.CharField(blank=True, default="PALMIRA/CALI", max_length=100, null=True, verbose_name="Nombre Sede Default TSOL"),
        ),
        migrations.AddField(
            model_name="confempresas",
            name="tsol_conexion",
            field=models.JSONField(blank=True, default=dict, null=True, verbose_name="Conexión FTP TSOL"),
        ),

        # Cosmos conexion JSON
        migrations.AddField(
            model_name="confempresas",
            name="cosmos_conexion",
            field=models.JSONField(blank=True, default=dict, null=True, verbose_name="Conexión FTPS Cosmos"),
        ),

        # ═══ Fase 2: Copiar datos ════════════════════════════════════

        migrations.RunPython(
            migrate_cdt_proveedor_to_empresa,
            migrations.RunPython.noop,
        ),
        migrations.RunPython(
            migrate_tsol_proveedor_to_empresa,
            migrations.RunPython.noop,
        ),
        migrations.RunPython(
            migrate_sftp_cdt_to_json,
            migrations.RunPython.noop,
        ),
        migrations.RunPython(
            migrate_ftps_cosmos_to_json,
            migrations.RunPython.noop,
        ),

        # ═══ Fase 3: Eliminar campos individuales de conexión ════════

        # SFTP CDT (5 campos)
        migrations.RemoveField(model_name="confempresas", name="sftp_cdt_host"),
        migrations.RemoveField(model_name="confempresas", name="sftp_cdt_port"),
        migrations.RemoveField(model_name="confempresas", name="sftp_cdt_user"),
        migrations.RemoveField(model_name="confempresas", name="sftp_cdt_pass"),
        migrations.RemoveField(model_name="confempresas", name="sftp_cdt_ruta_remota"),

        # FTPS Cosmos (6 campos)
        migrations.RemoveField(model_name="confempresas", name="ftps_cosmos_host"),
        migrations.RemoveField(model_name="confempresas", name="ftps_cosmos_port"),
        migrations.RemoveField(model_name="confempresas", name="ftps_cosmos_user"),
        migrations.RemoveField(model_name="confempresas", name="ftps_cosmos_pass"),
        migrations.RemoveField(model_name="confempresas", name="ftps_cosmos_ruta_remota"),
        migrations.RemoveField(model_name="confempresas", name="ftps_cosmos_certificate"),

        # ═══ Fase 4: Eliminar FKs proveedor ═════════════════════════

        # FK proveedor en CdtEnvio y TsolEnvio
        migrations.RemoveField(model_name="cdtenvio", name="proveedor"),
        migrations.RemoveField(model_name="tsolenvio", name="proveedor"),

        # FK proveedor_cdt y proveedor_tsol en ConfEmpresas
        migrations.RemoveField(model_name="confempresas", name="proveedor_cdt"),
        migrations.RemoveField(model_name="confempresas", name="proveedor_tsol"),

        # ═══ Fase 5: Eliminar modelos proveedor ═════════════════════

        migrations.DeleteModel(name="ConfCdtProveedor"),
        migrations.DeleteModel(name="ConfTsolProveedor"),
    ]
