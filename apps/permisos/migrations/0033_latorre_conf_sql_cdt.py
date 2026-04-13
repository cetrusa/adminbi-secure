# Migración: Configurar CDT/Mars para la empresa Latorre
#
# Problemas corregidos:
#   1. planos_cdt=[17686171] apuntaba a un ID inexistente → corregido a [1,4,3]
#   2. cdt_codigos_distribuidor tenía "empresa": "La_Torre" que no coincide con
#      empresa.name="latorre" → corregido a "empresa": "latorre"
#   3. Crea ConfSqlCdt ID=4: clientes_cdt adaptado al esquema real de latorre
#      (telefono, dane, tipologia, nombre en lugar de telefono_celular, ciudad_id,
#       negocio_nm, rsocial que no existen en la tabla clientes de latorre)

from django.db import migrations

CLIENTES_CDT_LATORRE = """SELECT
    c.cliente_id          AS idPuntoVenta,
    c.nombre              AS nmPuntoVenta,
    c.direccion           AS txDireccion,
    c.telefono            AS nbTelMovil,
    c.barrio              AS txBarrio,
    c.ciudad              AS txCiudad,
    c.dane                AS nbCiudad,
    c.nit                 AS nbDocumento,
    c.tipologia           AS nbNegocio,
    c.latitud_cl,
    c.longitud_cl
FROM clientes c"""


def apply(apps, schema_editor):
    ConfSqlCdt = apps.get_model("permisos", "ConfSqlCdt")
    ConfEmpresas = apps.get_model("permisos", "ConfEmpresas")

    # 1. Crear query de clientes adaptada al esquema de latorre
    ConfSqlCdt.objects.update_or_create(
        nbSql=4,
        defaults={
            "nmReporte": "clientes_cdt",
            "txTabla": "clientes",
            "txDescripcion": "Clientes CDT - Latorre (columnas adaptadas al esquema real de latorre)",
            "txSqlExtrae": CLIENTES_CDT_LATORRE,
        },
    )

    # 2. Corregir planos_cdt y cdt_codigos_distribuidor en latorre
    #    - planos_cdt: ventas genérico(1) + clientes latorre(4) + inventario genérico(3)
    #    - cdt_codigos_distribuidor: "empresa" debe ser "latorre" (= empresa.name)
    ConfEmpresas.objects.filter(name="latorre").update(
        planos_cdt="[1,4,3]",
        cdt_codigos_distribuidor='[{"codigo": "17686171", "empresa": "latorre", "tipo": "con_vendedores"}]',
    )


def reverse(apps, schema_editor):
    apps.get_model("permisos", "ConfSqlCdt").objects.filter(nbSql=4).delete()
    apps.get_model("permisos", "ConfEmpresas").objects.filter(name="latorre").update(
        planos_cdt="[17686171]",
        cdt_codigos_distribuidor='[{"codigo": "17686171", "empresa": "La_Torre", "tipo": "con_vendedores"}]',
    )


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0032_add_admin_bimbo_permission"),
    ]

    operations = [
        migrations.RunPython(apply, reverse),
    ]
