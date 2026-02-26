import os
import sys
from sqlalchemy import text

# Permite ejecutar desde /tools
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con


FIX_SQL_PATH = os.path.join(
    PROJECT_ROOT,
    "scripts",
    "sql",
    "fixes",
    "fix_sp_reporte_venta_cero_dinamico_collation.sql",
)


def load_create_procedure_sql(path: str) -> str:
    sql = open(path, "r", encoding="utf-8").read()
    # Quita directivas DELIMITER y marcas $$ para ejecución vía driver
    sql = sql.replace("DELIMITER $$", "")
    sql = sql.replace("DELIMITER ;", "")
    sql = sql.replace("$$", "")
    # Recorta a partir de la PRIMERA línea que realmente inicia con CREATE OR REPLACE
    lines = sql.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue
        if stripped.upper().startswith("CREATE OR REPLACE"):
            start_idx = i
            break
    if start_idx is None:
        raise ValueError("No se encontró un 'CREATE OR REPLACE' como sentencia en el SQL de fix")
    return "\n".join(lines[start_idx:]).strip()


def strip_definer(create_sql: str) -> str:
    # Convierte: CREATE OR REPLACE DEFINER=`x`@`y` PROCEDURE ... -> CREATE OR REPLACE PROCEDURE ...
    upper = create_sql.upper()
    key = "CREATE OR REPLACE DEFINER="
    if upper.startswith(key):
        # Encuentra la palabra PROCEDURE después del DEFINER
        proc_idx = upper.find(" PROCEDURE")
        if proc_idx != -1:
            return "CREATE OR REPLACE" + create_sql[proc_idx:]
    return create_sql


def escape_pymysql_percents(sql: str) -> str:
    # PyMySQL usa el operador % para interpolar parámetros incluso cuando no se pasan args.
    # Cualquier % literal en el SQL (ej: LIKE 'BIMBO%') debe escaparse como %%.
    return sql.replace("%", "%%")


def main() -> None:
    database_name = os.environ.get("DZ_DATABASE_NAME", "bimbo_unificado")
    user_id = int(os.environ.get("DZ_USER_ID", "1"))

    cfg = ConfigBasic(database_name, user_id).config
    engine = con.ConexionMariadb3(
        str(cfg["nmUsrIn"]),
        str(cfg["txPassIn"]),
        str(cfg["hostServerIn"]),
        int(cfg["portServerIn"]),
        str(cfg["dbBi"]),
    )

    create_sql = load_create_procedure_sql(FIX_SQL_PATH)

    # Aplicamos SIN DEFINER para evitar:
    # - Falta de privilegios por DEFINER
    # - Problemas de PyMySQL con el literal % en `cetrusa`@`%`
    sql_variant = strip_definer(create_sql)
    sql_to_exec = escape_pymysql_percents(sql_variant)
    with engine.begin() as c:
        c.exec_driver_sql(sql_to_exec)
    print("OK: applied procedure (without_definer)")

    # Smoke test: ejecutar el CALL con los parámetros del caso reportado.
    ceve = int(os.environ.get("DZ_CEVE", "34203"))
    fecha_ini = os.environ.get("DZ_FECHA_INI", "2026-01-01")
    fecha_fin = os.environ.get("DZ_FECHA_FIN", "2026-01-31")
    tipo = os.environ.get("DZ_TIPO", "PROVEEDOR")
    codigo_producto = os.environ.get("DZ_CODIGO_PRODUCTO", "")
    categoria = os.environ.get("DZ_CATEGORIA", "BIMBO")
    familia = os.environ.get("DZ_FAMILIA", "")

    call = text(
        "CALL sp_reporte_venta_cero_dinamico("
        ":p_ceve, :p_tipo_filtro, :p_codigo_producto, :p_categoria, :p_familia, :p_fecha_ini, :p_fecha_fin"
        ")"
    )

    with engine.connect() as c:
        # Mantén la collation igual al esquema powerbi_bimbo
        try:
            c.exec_driver_sql("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
            c.exec_driver_sql("SET collation_connection = 'utf8mb4_general_ci'")
        except Exception:
            pass

        res = c.execute(
            call,
            {
                "p_ceve": ceve,
                "p_tipo_filtro": tipo,
                "p_codigo_producto": codigo_producto,
                "p_categoria": categoria,
                "p_familia": familia,
                "p_fecha_ini": fecha_ini,
                "p_fecha_fin": fecha_fin,
            },
        )
        row = res.first()

    print("OK: CALL ejecutó sin error")
    print("sample_row:", row)


if __name__ == "__main__":
    main()
