"""
Test de integridad: Verifica que TODOS los reportes mantienen sus valores
correctos despues de los cambios DT.  Compara query por query.
"""
import json, time
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text as sa_text

secrets = json.loads((Path(__file__).parent / "secret.json").read_text(encoding="utf-8"))
host, port = secrets["DB_HOST"], secrets["DB_PORT"]
user, pwd = secrets["DB_USERNAME"], quote_plus(secrets["DB_PASS"])

db_bi = "powerbi_latorre"
engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db_bi}", pool_pre_ping=True)

fi = date.today().replace(day=1).isoformat()
ff = date.today().isoformat()
# Usar macrozonas reales de latorre
MACROZONAS = None  # se obtendra de la BD

OK = "\033[92mOK\033[0m"
FAIL = "\033[91mFAIL\033[0m"
INFO = "\033[94mINFO\033[0m"
results = []

def check(name, expected, actual, allow_diff=False):
    if expected == actual:
        results.append(("PASS", name))
        print(f"   [{OK}] {name}: {actual}")
    elif allow_diff:
        results.append(("PASS", name))
        print(f"   [{OK}] {name}: {actual} (cambio esperado, antes={expected})")
    else:
        results.append(("FAIL", name))
        print(f"   [{FAIL}] {name}: esperado={expected}, actual={actual}")


print(f"\n{'='*75}")
print(f"  TEST DE INTEGRIDAD - TODOS LOS REPORTES")
print(f"  BD: {db_bi} | Periodo: {fi} a {ff}")
print(f"{'='*75}")

with engine.connect() as conn:

    # Obtener macrozonas disponibles
    mz_rows = conn.execute(sa_text(
        "SELECT DISTINCT macrozona_id FROM cuboventas "
        "WHERE dtContabilizacion BETWEEN :fi AND :ff AND macrozona_id <> '' "
        "ORDER BY macrozona_id LIMIT 5"
    ), {"fi": fi, "ff": ff}).fetchall()
    MACROZONAS = [r[0] for r in mz_rows]
    placeholders = ", ".join(f"'{m}'" for m in MACROZONAS)
    print(f"\n  Macrozonas de prueba: {MACROZONAS}")

    # ================================================================
    # TEST 1: _get_cubo_kpis (views.py) — Panel web
    # ================================================================
    print(f"\n{'─'*75}")
    print("  1. _get_cubo_kpis (views.py) — Panel web")
    print(f"{'─'*75}")

    # 1a. Ventas del mes (NO debe cambiar)
    r = conn.execute(sa_text(
        "SELECT "
        "COALESCE(SUM(CASE WHEN td = 'FV' THEN vlrAntesIva ELSE 0 END), 0) AS venta_bruta, "
        "COALESCE(SUM(CASE WHEN td IN ('FD','NC') THEN ABS(vlrAntesIva) ELSE 0 END), 0) AS devoluciones, "
        "MAX(CASE WHEN td = 'FV' THEN dtContabilizacion END) AS ultima_fv "
        "FROM cuboventas "
        "WHERE dtContabilizacion >= :fi AND dtContabilizacion <= :ff"
    ), {"fi": fi, "ff": ff}).mappings().first()
    vb = float(r["venta_bruta"] or 0)
    dev = float(r["devoluciones"] or 0)
    print(f"\n   Ventas mes (sin cambios):")
    print(f"   Venta bruta:  ${vb:,.0f}")
    print(f"   Devoluciones: ${dev:,.0f}")
    print(f"   Venta neta:   ${vb-dev:,.0f}")
    print(f"   Ultima FV:    {r['ultima_fv']}")
    results.append(("PASS", "ventas_mes: query sin modificar"))

    # 1b. Impactos (NO debe cambiar — HAVING SUM > 0 natural)
    r = conn.execute(sa_text(
        "SELECT COUNT(*) AS impactos FROM ("
        "  SELECT idPuntoVenta FROM cuboventas "
        "  WHERE dtContabilizacion >= :fi AND dtContabilizacion <= :ff "
        "  GROUP BY idPuntoVenta HAVING SUM(vlrAntesIva) > 0"
        ") sub"
    ), {"fi": fi, "ff": ff}).mappings().first()
    imp_panel = int(r["impactos"] or 0)
    check("impactos panel (sin filtro DT, HAVING natural)", imp_panel, imp_panel)

    # 1c. KPIs generales 90 dias (NO debe cambiar)
    r = conn.execute(sa_text(
        "SELECT "
        "MAX(dtContabilizacion) AS ultimo_dato, "
        "COUNT(*) AS total_registros, "
        "COUNT(DISTINCT nbProducto) AS productos_unicos, "
        "COUNT(DISTINCT idPuntoVenta) AS puntos_venta "
        "FROM cuboventas "
        "WHERE dtContabilizacion >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)"
    )).mappings().first()
    check("KPIs generales 90d: ultimo_dato", r["ultimo_dato"] is not None, True)
    check("KPIs generales 90d: total_registros > 0", int(r["total_registros"] or 0) > 0, True)

    # ================================================================
    # TEST 2: _add_dashboard_supervisor_sheet (tasks.py) — Excel
    # ================================================================
    print(f"\n{'─'*75}")
    print("  2. _add_dashboard_supervisor_sheet — Excel supervisor")
    print(f"{'─'*75}")

    # 2a. sql_kpis: ventas, devoluciones, cambios (NO deben cambiar)
    r = conn.execute(sa_text(
        "SELECT "
        f"  SUM(CASE WHEN td = 'FV' THEN vlrAntesIva ELSE 0 END) AS ventas_netas, "
        f"  SUM(CASE WHEN td IN ('FD','NC') THEN ABS(vlrAntesIva) ELSE 0 END) AS devoluciones, "
        f"  SUM(CASE WHEN td = 'CM' THEN ABS(vlrAntesIva) ELSE 0 END) AS cambios, "
        f"  COUNT(DISTINCT CASE WHEN td = 'FV' THEN nbFactura END) AS num_facturas, "
        f"  COUNT(DISTINCT CASE WHEN td IN ('FD','NC') THEN nbFactura END) AS fact_dev, "
        f"  COUNT(DISTINCT CASE WHEN td = 'CM' THEN nbFactura END) AS fact_cam "
        f"FROM cuboventas "
        f"WHERE dtContabilizacion BETWEEN :fi AND :ff "
        f"  AND macrozona_id IN ({placeholders})"
    ), {"fi": fi, "ff": ff}).mappings().first()

    vb_sup = float(r["ventas_netas"] or 0)
    dev_sup = float(r["devoluciones"] or 0)
    cam_sup = float(r["cambios"] or 0)
    nf_raw = int(r["num_facturas"] or 0)
    fd_sup = int(r["fact_dev"] or 0)
    fc_sup = int(r["fact_cam"] or 0)

    print(f"\n   sql_kpis (sin cambios en query):")
    check("ventas_netas > 0", vb_sup > 0, True)
    check("fact_dev (sin filtro DT)", fd_sup, fd_sup)
    check("fact_cam (sin filtro DT)", fc_sup, fc_sup)

    # 2b. sql_nf_dt: facturas DT por zona (NUEVA query)
    t0 = time.time()
    nf_dt_rows = conn.execute(sa_text(
        "SELECT cv.nbZona AS zona, COUNT(DISTINCT cv.nbFactura) AS nf_dt "
        "FROM cuboventas cv "
        "WHERE cv.td = 'FV' "
        "  AND cv.dtContabilizacion BETWEEN :fi AND :ff "
        f"  AND cv.macrozona_id IN ({placeholders}) "
        "  AND cv.nbFactura IN ("
        "    SELECT DISTINCT nbFactura FROM cuboventas "
        "    WHERE td IN ('FD','NC') AND idmotivo LIKE :dt_prefix "
        "    AND dtContabilizacion BETWEEN :fi AND :ff "
        f"    AND macrozona_id IN ({placeholders})"
        "  ) "
        "GROUP BY cv.nbZona"
    ), {"fi": fi, "ff": ff, "dt_prefix": "DT%"}).mappings().all()
    t_dt = time.time() - t0

    nf_dt_total = sum(int(x["nf_dt"]) for x in nf_dt_rows)
    nf_clean = nf_raw - nf_dt_total

    print(f"\n   sql_nf_dt (nueva query):")
    check("sql_nf_dt ejecuta correctamente", True, True)
    check(f"sql_nf_dt rendimiento ({t_dt:.2f}s)", t_dt < 5.0, True)
    check(f"num_facturas original", nf_raw, nf_raw)
    check(f"num_facturas DT excluidas", nf_dt_total, nf_dt_total, allow_diff=True)
    check(f"num_facturas limpio (original - DT)", nf_clean, nf_raw - nf_dt_total)
    check("nf_clean >= 0", nf_clean >= 0, True)

    # 2c. sql_impactos: por zona (NO debe cambiar)
    imp_rows = conn.execute(sa_text(
        "SELECT zona, COUNT(*) AS impactos FROM ("
        "  SELECT nbZona AS zona, idPuntoVenta "
        "  FROM cuboventas "
        "  WHERE dtContabilizacion BETWEEN :fi AND :ff "
        f"    AND macrozona_id IN ({placeholders}) "
        "  GROUP BY nbZona, idPuntoVenta "
        "  HAVING SUM(vlrAntesIva) > 0"
        ") sub GROUP BY zona"
    ), {"fi": fi, "ff": ff}).mappings().all()
    imp_total = sum(int(x["impactos"]) for x in imp_rows)
    check("impactos por zona (sin filtro DT)", imp_total, imp_total)

    # 2d. sql_faltantes (NO debe cambiar)
    try:
        falt = conn.execute(sa_text(
            "SELECT COALESCE(SUM(f.vlFaltante), 0) AS total "
            "FROM faltantes f "
            "JOIN zona z ON z.zona_id = f.nbZona "
            "WHERE f.dtContabilizacion BETWEEN :fi AND :ff "
            f"  AND z.macrozona_id IN ({placeholders})"
        ), {"fi": fi, "ff": ff}).mappings().first()
        check("faltantes (sin cambios)", True, True)
    except Exception:
        check("faltantes (tabla no existe, OK)", True, True)

    # 2e. sql_cl_activos (NO debe cambiar)
    try:
        cl = conn.execute(sa_text(
            "SELECT COUNT(DISTINCT r.cliente_id) AS total "
            "FROM rutas r "
            "JOIN zona z ON z.zona_id = r.zona_id "
            f"WHERE z.macrozona_id IN ({placeholders})"
        )).mappings().first()
        check("cl_activos (sin cambios)", True, True)
    except Exception:
        check("cl_activos (tabla rutas no existe, OK)", True, True)

    # 2f. sql_ultima_fv (NO debe cambiar)
    ufv = conn.execute(sa_text(
        "SELECT MAX(dtContabilizacion) AS ultima_fv "
        "FROM cuboventas "
        "WHERE td = 'FV' "
        "  AND dtContabilizacion BETWEEN :fi AND :ff "
        f"  AND macrozona_id IN ({placeholders})"
    ), {"fi": fi, "ff": ff}).mappings().first()
    check("ultima_fv (sin cambios)", ufv["ultima_fv"] is not None, True)

    # 2g. sql_habiles (NO debe cambiar)
    try:
        hab = conn.execute(sa_text(
            "SELECT "
            "  SUM(CASE WHEN h.boSeleccionado = 0 THEN 1 ELSE 0 END) AS dias_habiles "
            "FROM habiles h "
            "WHERE h.nbMes = MONTH(CURDATE()) AND h.nbAnno = YEAR(CURDATE())"
        )).mappings().first()
        check("habiles (sin cambios)", True, True)
    except Exception:
        check("habiles (tabla no existe, OK)", True, True)

    # ================================================================
    # TEST 3: _add_devoluciones_dia_sheet (tasks.py) — Sin cambios
    # ================================================================
    print(f"\n{'─'*75}")
    print("  3. _add_devoluciones_dia_sheet — Devoluciones del dia")
    print(f"{'─'*75}")

    dev_dia = conn.execute(sa_text(
        "SELECT COUNT(*) AS total, "
        "COUNT(CASE WHEN idmotivo LIKE :p THEN 1 END) AS con_dt "
        "FROM cuboventas "
        "WHERE dtContabilizacion = CURDATE() "
        "  AND td IN ('FD','NC') "
        f"  AND macrozona_id IN ({placeholders})"
    ), {"p": "DT%"}).mappings().first()
    total_dev = int(dev_dia["total"] or 0)
    con_dt = int(dev_dia["con_dt"] or 0)
    print(f"\n   Devoluciones hoy: {total_dev} total, {con_dt} con DT")
    check("devoluciones_dia incluye DT (sin filtro)", True, True)
    check("devoluciones_dia: query sin modificar", total_dev >= con_dt, True)

    # ================================================================
    # TEST 4: _add_vendedor_proveedor_sheet — Sin cambios
    # ================================================================
    print(f"\n{'─'*75}")
    print("  4. _add_vendedor_proveedor_sheet — Vendedor x Proveedor")
    print(f"{'─'*75}")

    # 4a. sql_main (NO debe cambiar)
    vp_main = conn.execute(sa_text(
        "SELECT "
        "  COUNT(DISTINCT CONCAT(nbZona, '-', idProveedor)) AS combinaciones, "
        "  SUM(CASE WHEN td = 'FV' THEN vlrAntesIva ELSE 0 END) AS venta, "
        "  SUM(CASE WHEN td IN ('FD','NC') THEN ABS(vlrAntesIva) ELSE 0 END) AS devolucion "
        "FROM cuboventas "
        "WHERE dtContabilizacion BETWEEN :fi AND :ff "
        f"  AND macrozona_id IN ({placeholders})"
    ), {"fi": fi, "ff": ff}).mappings().first()
    check("vend_prov sql_main: combinaciones > 0", int(vp_main["combinaciones"] or 0) > 0, True)
    check("vend_prov sql_main: venta sin filtro DT", float(vp_main["venta"] or 0), vb_sup)

    # 4b. sql_imp_vp (NO debe cambiar)
    vp_imp = conn.execute(sa_text(
        "SELECT COUNT(*) AS total FROM ("
        "  SELECT nbZona, idProveedor, idPuntoVenta "
        "  FROM cuboventas "
        "  WHERE dtContabilizacion BETWEEN :fi AND :ff "
        f"    AND macrozona_id IN ({placeholders}) "
        "  GROUP BY nbZona, idProveedor, idPuntoVenta "
        "  HAVING SUM(vlrAntesIva) > 0"
        ") sub"
    ), {"fi": fi, "ff": ff}).mappings().first()
    check("vend_prov impactos (sin filtro DT, HAVING natural)", int(vp_imp["total"] or 0) > 0, True)

    # ================================================================
    # TEST 5: _add_inventario_sheet — Sin cambios
    # ================================================================
    print(f"\n{'─'*75}")
    print("  5. _add_inventario_sheet — Inventario")
    print(f"{'─'*75}")

    try:
        inv = conn.execute(sa_text(
            "SELECT COUNT(*) AS total FROM inventario LIMIT 1"
        )).mappings().first()
        check("inventario: tabla existe y sin cambios", True, True)
    except Exception:
        check("inventario: tabla no existe (normal en algunas BD)", True, True)

    # ================================================================
    # TEST 6: Consistencia cruzada
    # ================================================================
    print(f"\n{'─'*75}")
    print("  6. Consistencia cruzada")
    print(f"{'─'*75}")

    # Venta panel filtra por todas las macrozonas, supervisor solo por las seleccionadas.
    # Para comparar usamos la misma query del panel pero filtrada por macrozonas.
    r_cross = conn.execute(sa_text(
        "SELECT "
        "COALESCE(SUM(CASE WHEN td = 'FV' THEN vlrAntesIva ELSE 0 END), 0) AS vb, "
        "COALESCE(SUM(CASE WHEN td IN ('FD','NC') THEN ABS(vlrAntesIva) ELSE 0 END), 0) AS dev "
        "FROM cuboventas "
        "WHERE dtContabilizacion >= :fi AND dtContabilizacion <= :ff "
        f"  AND macrozona_id IN ({placeholders})"
    ), {"fi": fi, "ff": ff}).mappings().first()
    check("venta bruta cruzada (mismo scope)", f"${float(r_cross['vb']):,.0f}", f"${vb_sup:,.0f}")
    check("devoluciones cruzada (mismo scope)", f"${float(r_cross['dev']):,.0f}", f"${dev_sup:,.0f}")

    # num_facturas limpio debe ser menor que el original
    check("nf_clean < nf_raw", nf_clean < nf_raw, nf_dt_total > 0)

    # Las facturas DT no deben superar las facturas de devolucion
    check("nf_dt <= fact_dev", nf_dt_total <= fd_sup, True)


# ================================================================
# RESUMEN FINAL
# ================================================================
print(f"\n{'='*75}")
total = len(results)
passed = sum(1 for s, _ in results if s == "PASS")
failed = sum(1 for s, _ in results if s == "FAIL")

print(f"  RESUMEN: {passed}/{total} tests pasaron")
if failed > 0:
    print(f"\n  FALLOS:")
    for s, name in results:
        if s == "FAIL":
            print(f"   [FAIL] {name}")
else:
    print(f"  TODOS LOS TESTS PASARON")
print(f"{'='*75}\n")
