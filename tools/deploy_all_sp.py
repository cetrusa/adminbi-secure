"""
Despliega todos los stored procedures de Bimbo al esquema powerbi_bimbo.

Uso:
    python tools/deploy_all_sp.py
    python tools/deploy_all_sp.py --dry-run          # solo muestra, no ejecuta
    python tools/deploy_all_sp.py --only rutero       # despliega solo uno
    python tools/deploy_all_sp.py --smoke-test        # ejecuta smoke test despues del deploy

Variables de entorno opcionales:
    DZ_DATABASE_NAME  (default: bimbo_unificado)
    DZ_USER_ID        (default: 1)
    DZ_CEVE           (default: 34203)   — para smoke test
"""
import argparse
import os
import re
import sys
import textwrap

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import text

from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con

# ── Catálogo de SPs ────────────────────────────────────────────────
SP_CATALOG = [
    {
        "key": "venta_cero",
        "file": "scripts/bimbo/sql/sp_reporte_venta_cero_dinamico.sql",
        "name": "sp_reporte_venta_cero_dinamico",
        "smoke_sql": (
            "CALL powerbi_bimbo.sp_reporte_venta_cero_dinamico"
            "(:p_ceve, 'PROVEEDOR', '', 'BIMBO', '', :p_fecha_ini, :p_fecha_fin)"
        ),
        "smoke_params": lambda ceve: {
            "p_ceve": ceve, "p_fecha_ini": "2026-01-01", "p_fecha_fin": "2026-01-31",
        },
    },
    {
        "key": "rutero",
        "file": "scripts/bimbo/sql/sp_reporte_rutero_dinamico.sql",
        "name": "sp_reporte_rutero_dinamico",
        "smoke_sql": "CALL powerbi_bimbo.sp_reporte_rutero_dinamico(:p_ceve)",
        "smoke_params": lambda ceve: {"p_ceve": ceve},
    },
    {
        "key": "preventa",
        "file": "scripts/bimbo/sql/sp_reporte_preventa_diaria.sql",
        "name": "sp_reporte_preventa_diaria",
        "smoke_sql": (
            "CALL powerbi_bimbo.sp_reporte_preventa_diaria"
            "(:p_ceve, :p_fecha_ini, :p_fecha_fin)"
        ),
        "smoke_params": lambda ceve: {
            "p_ceve": ceve, "p_fecha_ini": "2026-01-01", "p_fecha_fin": "2026-01-31",
        },
    },
    {
        "key": "faltantes",
        "file": "scripts/bimbo/sql/sp_reporte_faltantes.sql",
        "name": "sp_reporte_faltantes",
        "smoke_sql": (
            "CALL powerbi_bimbo.sp_reporte_faltantes"
            "(:p_ceve, 'PROVEEDOR', '', 'BIMBO', '', :p_fecha_ini, :p_fecha_fin)"
        ),
        "smoke_params": lambda ceve: {
            "p_ceve": ceve, "p_fecha_ini": "2026-01-01", "p_fecha_fin": "2026-01-31",
        },
    },
    {
        "key": "inventarios",
        "file": "scripts/bimbo/sql/sp_reporte_inventarios_dinamico.sql",
        "name": "sp_reporte_inventarios_dinamico",
        "smoke_sql": "CALL powerbi_bimbo.sp_reporte_inventarios_dinamico(:p_ceve)",
        "smoke_params": lambda ceve: {"p_ceve": ceve},
    },
]


# ── Utilidades de SQL ──────────────────────────────────────────────
def load_sql_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def parse_sp_statements(raw_sql: str):
    """Extrae las sentencias DROP y CREATE de un archivo SP.

    Retorna (drop_sql, create_sql).
    """
    # Limpiar directivas DELIMITER y marcas $$
    sql = raw_sql.replace("DELIMITER $$", "")
    sql = sql.replace("DELIMITER ;", "")
    sql = sql.replace("$$", "")

    lines = sql.splitlines()

    # Extraer DROP PROCEDURE IF EXISTS
    drop_sql = None
    create_lines = []
    in_create = False

    for line in lines:
        stripped = line.strip()
        upper = stripped.upper()

        # Omitir USE (la conexion ya apunta al schema correcto)
        if upper.startswith("USE "):
            continue

        if upper.startswith("DROP PROCEDURE IF EXISTS"):
            # Asegurar que termina con ;
            drop_sql = stripped if stripped.endswith(";") else stripped + ";"
            continue

        if upper.startswith("CREATE PROCEDURE") or upper.startswith("CREATE DEFINER"):
            in_create = True

        if in_create:
            create_lines.append(line)

    create_sql = "\n".join(create_lines).strip()

    if not create_sql:
        raise ValueError("No se encontro sentencia CREATE PROCEDURE en el archivo")

    return drop_sql, create_sql


def strip_definer(create_sql: str) -> str:
    """Elimina la clausula DEFINER=`x`@`y` del CREATE PROCEDURE."""
    return re.sub(
        r"CREATE\s+DEFINER\s*=\s*`[^`]*`\s*@\s*`[^`]*`\s+PROCEDURE",
        "CREATE PROCEDURE",
        create_sql,
        count=1,
        flags=re.IGNORECASE,
    )


def escape_pymysql_percents(sql: str) -> str:
    """Escapa % literales para PyMySQL (LIKE 'BIMBO%' -> 'BIMBO%%')."""
    return sql.replace("%", "%%")


# ── Despliegue ─────────────────────────────────────────────────────
def deploy_sp(engine, sp_entry: dict, dry_run: bool = False) -> bool:
    """Despliega un SP desde su archivo SQL al schema powerbi_bimbo."""
    key = sp_entry["key"]
    file_path = os.path.join(PROJECT_ROOT, sp_entry["file"])
    name = sp_entry["name"]

    if not os.path.isfile(file_path):
        print(f"  [{key}] ERROR: archivo no encontrado: {file_path}")
        return False

    raw_sql = load_sql_file(file_path)
    drop_sql, create_sql = parse_sp_statements(raw_sql)
    create_sql = strip_definer(create_sql)

    if dry_run:
        print(f"  [{key}] DRY-RUN: se desplegaría {name}")
        if drop_sql:
            print(f"    DROP: {drop_sql[:80]}...")
        print(f"    CREATE: {create_sql[:80]}...")
        return True

    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
            conn.exec_driver_sql("SET collation_connection = 'utf8mb4_general_ci'")

            if drop_sql:
                conn.exec_driver_sql(escape_pymysql_percents(drop_sql))

            conn.exec_driver_sql(escape_pymysql_percents(create_sql))

        print(f"  [{key}] OK: {name} desplegado")
        return True

    except Exception as exc:
        print(f"  [{key}] ERROR al desplegar {name}: {exc}")
        return False


def smoke_test_sp(engine, sp_entry: dict, ceve: int) -> bool:
    """Ejecuta una consulta minima para verificar que el SP funciona."""
    key = sp_entry["key"]
    name = sp_entry["name"]
    smoke_sql = sp_entry.get("smoke_sql")
    smoke_params_fn = sp_entry.get("smoke_params")

    if not smoke_sql or not smoke_params_fn:
        print(f"  [{key}] SKIP smoke test (no configurado)")
        return True

    params = smoke_params_fn(ceve)

    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
            conn.exec_driver_sql("SET collation_connection = 'utf8mb4_general_ci'")

            result = conn.execute(text(smoke_sql), params)
            row = result.first()

        print(f"  [{key}] SMOKE OK: {name} ejecuto sin error (sample: {str(row)[:120]})")
        return True

    except Exception as exc:
        err_str = str(exc)
        # "No existen productos" o "CEVE no existe" son errores validos del SP,
        # no del deploy. El SP funciona, solo no hay datos para el CEVE/filtro.
        if "45000" in err_str:
            print(f"  [{key}] SMOKE OK (SP respondio correctamente con SIGNAL): {err_str[:100]}")
            return True
        print(f"  [{key}] SMOKE FAIL: {err_str[:200]}")
        return False


def get_engine():
    database_name = os.environ.get("DZ_DATABASE_NAME", "bimbo_unificado")
    user_id = int(os.environ.get("DZ_USER_ID", "1"))
    cfg = ConfigBasic(database_name, user_id).config

    required = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        print(f"ERROR: faltan claves de configuracion: {missing}")
        sys.exit(1)

    return con.ConexionMariadb3(
        str(cfg["nmUsrIn"]),
        str(cfg["txPassIn"]),
        str(cfg["hostServerIn"]),
        int(cfg["portServerIn"]),
        "powerbi_bimbo",  # conectar directo al schema de los SPs
    )


def main():
    parser = argparse.ArgumentParser(
        description="Despliega stored procedures de Bimbo a powerbi_bimbo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Ejemplos:
              python tools/deploy_all_sp.py                     # despliega todos
              python tools/deploy_all_sp.py --dry-run           # vista previa
              python tools/deploy_all_sp.py --only venta_cero   # solo uno
              python tools/deploy_all_sp.py --smoke-test        # con verificacion
        """),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra lo que se haria sin ejecutar",
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Despliega solo un SP por su key (venta_cero, rutero, preventa, faltantes, inventarios)",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Ejecuta smoke test despues del despliegue",
    )
    args = parser.parse_args()

    # Filtrar catalogo si --only
    catalog = SP_CATALOG
    if args.only:
        catalog = [sp for sp in SP_CATALOG if sp["key"] == args.only]
        if not catalog:
            valid_keys = ", ".join(sp["key"] for sp in SP_CATALOG)
            print(f"ERROR: '{args.only}' no encontrado. Keys validos: {valid_keys}")
            sys.exit(1)

    print("=" * 60)
    print("  Deploy Stored Procedures — powerbi_bimbo")
    print("=" * 60)
    print(f"  SPs a desplegar: {len(catalog)}")
    print(f"  Dry-run: {args.dry_run}")
    print(f"  Smoke-test: {args.smoke_test}")
    print()

    engine = get_engine()

    # ── Deploy ──
    print(">> Desplegando SPs...")
    results = {}
    for sp in catalog:
        ok = deploy_sp(engine, sp, dry_run=args.dry_run)
        results[sp["key"]] = ok

    # ── Smoke test ──
    if args.smoke_test and not args.dry_run:
        print()
        print(">> Smoke tests...")
        ceve = int(os.environ.get("DZ_CEVE", "34203"))
        for sp in catalog:
            if results.get(sp["key"]):
                smoke_test_sp(engine, sp, ceve)

    # ── Resumen ──
    print()
    print("=" * 60)
    ok_count = sum(1 for v in results.values() if v)
    fail_count = sum(1 for v in results.values() if not v)
    print(f"  Resultado: {ok_count} OK, {fail_count} fallidos de {len(catalog)} total")
    if fail_count:
        failed = [k for k, v in results.items() if not v]
        print(f"  Fallidos: {', '.join(failed)}")
    print("=" * 60)

    sys.exit(1 if fail_count else 0)


if __name__ == "__main__":
    main()
