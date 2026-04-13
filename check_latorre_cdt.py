import django
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "adminbi.settings.prod")
django.setup()

from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con
from sqlalchemy import text
from apps.permisos.models import ConfEmpresas, ConfSqlCdt

# Config actual de latorre
e = ConfEmpresas.objects.get(name="latorre")
print("=== CONFIG LATORRE ===")
print("planos_cdt:", e.planos_cdt)
print("cdt_codigo_proveedor:", repr(e.cdt_codigo_proveedor))
print("cdt_codigos_distribuidor:", e.cdt_codigos_distribuidor)

# SQLs cargados
ids_str = (e.planos_cdt or "").strip("[] ")
sql_ids = [int(x.strip()) for x in ids_str.split(",") if x.strip()]
print("\n=== CONF_SQL_CDT CARGADOS ===")
for s in ConfSqlCdt.objects.filter(nbSql__in=sql_ids):
    print(f"  ID={s.nbSql} nmReporte={s.nmReporte}")
    print(f"  SQL: {s.txSqlExtrae[:120].strip()}...")

# Consulta BD
config = ConfigBasic("latorre").config
engine = con.ConexionMariadb3(
    str(config["nmUsrIn"]), str(config["txPassIn"]),
    str(config["hostServerIn"]), int(config["portServerIn"]),
    str(config["dbBi"])
)
with engine.connect() as conn:
    print("\n=== nmProveedor en productos (idProveedor=6) ===")
    rows = conn.execute(text(
        "SELECT DISTINCT nmProveedor FROM productos WHERE idProveedor=6 LIMIT 5"
    )).fetchall()
    for r in rows:
        print(" ", repr(r[0]))

    print("\n=== nmProveedor en cuboventas (idProveedor=6, marzo 2026) ===")
    rows = conn.execute(text(
        "SELECT DISTINCT nmProveedor FROM cuboventas "
        "WHERE idProveedor=6 AND dtContabilizacion>='2026-03-01' "
        "AND dtContabilizacion<='2026-03-31' LIMIT 5"
    )).fetchall()
    for r in rows:
        print(" ", repr(r[0]))

    print("\n=== Columnas de cuboventas ===")
    rows = conn.execute(text(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='cuboventas' "
        "ORDER BY ORDINAL_POSITION"
    )).fetchall()
    cols = [r[0] for r in rows]
    print(" ", cols)

engine.dispose()
