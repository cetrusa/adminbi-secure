import json
import os
import sys

from sqlalchemy import text

# Asegura importación de paquetes del proyecto al ejecutar desde /tools
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con


def main() -> None:
    database_name = os.environ.get("DZ_DATABASE_NAME", "bimbo_unificado")
    user_id = int(os.environ.get("DZ_USER_ID", "1"))
    proc = os.environ.get("DZ_PROC", "sp_reporte_venta_cero_dinamico")
    ceve = int(os.environ.get("DZ_CEVE", "34203"))

    cfg = ConfigBasic(database_name, user_id).config
    engine = con.ConexionMariadb3(
        str(cfg["nmUsrIn"]),
        str(cfg["txPassIn"]),
        str(cfg["hostServerIn"]),
        int(cfg["portServerIn"]),
        str(cfg["dbBi"]),
    )

    out_dir = os.path.join("output", "diagnosticos")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"show_create_{proc}.txt")

    with engine.connect() as c:
        ver = c.execute(text("SELECT VERSION()")) .scalar()
        vars_rows = c.execute(
            text(
                "SHOW VARIABLES WHERE Variable_name IN ("
                "'character_set_client','character_set_connection','character_set_results',"
                "'collation_connection','collation_database','collation_server'"
                ")"
            )
        ).fetchall()
        vars_dict = {k: v for (k, v) in vars_rows}
        db = c.execute(text("SELECT DATABASE()")) .scalar()
        db_coll = c.execute(
            text(
                "SELECT DEFAULT_COLLATION_NAME "
                "FROM information_schema.SCHEMATA "
                "WHERE SCHEMA_NAME=DATABASE()"
            )
        ).scalar()
        row = c.execute(text(f"SHOW CREATE PROCEDURE {proc}")) .fetchone()

        # Resuelve v_db como lo hace el SP (para poder inspeccionar collation en cuboventas/clientes/rutas)
        v_db = c.execute(
            text(
                "SELECT CASE CEVE "
                "WHEN 34204 THEN 'powerbi_olpar_mariquita' "
                "WHEN 34209 THEN 'powerbi_distripasto_b' "
                "WHEN 34202 THEN 'powerbi_victor_alvarez_b' "
                "WHEN 34201 THEN 'powerbi_sidimat_b' "
                "WHEN 34206 THEN 'powerbi_jjrestrepo_v' "
                "WHEN 34203 THEN 'powerbi_cima_b' "
                "WHEN 34205 THEN 'powerbi_olpar_ibague' "
                "WHEN 34211 THEN 'powerbi_supernovarp' "
                "WHEN 34212 THEN 'powerbi_bobadilla' "
                "WHEN 34213 THEN 'powerbi_caribe_cartagena' "
                "ELSE NULL END "
                "FROM powerbi_bimbo.agencias_bimbo WHERE CEVE=:ceve"
            ),
            {"ceve": ceve},
        ).scalar()

        cols_cuboventas = []
        cols_clientes = []
        cols_rutas = []
        if v_db:
            cols_cuboventas = c.execute(
                text(
                    "SELECT COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, DATA_TYPE "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=:schema AND TABLE_NAME='cuboventas' "
                    "AND COLUMN_NAME IN ('idhmlProdProv','idPuntoVenta','dtContabilizacion')"
                ),
                {"schema": v_db},
            ).fetchall()
            cols_clientes = c.execute(
                text(
                    "SELECT COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, DATA_TYPE "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=:schema AND TABLE_NAME='clientes' "
                    "AND COLLATION_NAME IS NOT NULL"
                ),
                {"schema": v_db},
            ).fetchall()
            cols_rutas = c.execute(
                text(
                    "SELECT COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, DATA_TYPE "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=:schema AND TABLE_NAME='rutas' "
                    "AND COLLATION_NAME IS NOT NULL"
                ),
                {"schema": v_db},
            ).fetchall()

        # Collations relevantes (tablas maestras usadas en el SP)
        cols_productos = c.execute(
            text(
                "SELECT COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA='powerbi_bimbo' "
                "AND TABLE_NAME='productos_bimbo' "
                "AND COLLATION_NAME IS NOT NULL "
                "ORDER BY ORDINAL_POSITION"
            )
        ).fetchall()

    content: list[str] = []
    content.append(f"version={ver}\n")
    content.append(f"database={db} default_collation={db_coll}\n")
    content.append("variables\n" + json.dumps(vars_dict, indent=2, ensure_ascii=False) + "\n\n")
    content.append("show_create_procedure\n")
    for i, col in enumerate(row or []):
        content.append(f"--- col[{i}] ---\n{col}\n")

    content.append("\ncolumn_collations: powerbi_bimbo.productos_bimbo\n")
    for (column_name, collation_name, charset_name) in cols_productos or []:
        content.append(f"{column_name}\t{charset_name}\t{collation_name}\n")

    content.append(f"\nresolved_agent_schema (v_db) for CEVE={ceve}: {v_db}\n")
    content.append("column_collations: v_db.cuboventas (subset)\n")
    for (column_name, collation_name, charset_name, data_type) in cols_cuboventas or []:
        content.append(f"{column_name}\t{data_type}\t{charset_name}\t{collation_name}\n")
    content.append("\ncolumn_collations: v_db.clientes (all text columns)\n")
    for (column_name, collation_name, charset_name, data_type) in cols_clientes or []:
        content.append(f"{column_name}\t{data_type}\t{charset_name}\t{collation_name}\n")
    content.append("\ncolumn_collations: v_db.rutas (all text columns)\n")
    for (column_name, collation_name, charset_name, data_type) in cols_rutas or []:
        content.append(f"{column_name}\t{data_type}\t{charset_name}\t{collation_name}\n")

    with open(path, "w", encoding="utf-8") as f:
        f.write("".join(content))

    print(path)


if __name__ == "__main__":
    main()
