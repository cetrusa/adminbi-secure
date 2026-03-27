"""
Script standalone para reemplazar las queries TSOL 1-11 en conf_sql_tsol.
Las queries originales (basadas en dim_productos, dim_clientes, dim_estructura)
no aplicaban. Se reemplazan con queries adaptadas al schema real de las BDs BI
(tablas: productos, clientes, zona, supervisores, supervisores_macrozona,
inventario, cuboventas).

Luego elimina las queries 12-22 (ya no necesarias) y actualiza planos_tsol
de latorre (id=631) para usar [1,...,11].
"""

import json
import pymysql

# Leer credenciales desde secret.json
with open("secret.json") as f:
    secret = json.loads(f.read())

DB_HOST = secret["DB_HOST"]
DB_PORT = int(secret["DB_PORT"])
DB_USER = secret["DB_USERNAME"]
DB_PASS = secret["DB_PASS"]
DB_NAME = secret["DB_NAME"]  # powerbi_adm

# ── Queries TSOL (nbSql 1-11) ─────────────────────────────────────
# Estas queries se almacenan como texto y se ejecutan via SQLAlchemy text()
# con :fi y :ff como parámetros. Usar % simple en DATE_FORMAT.

SQL_TSOL = [
    # ─── 1. VENTAS ──────────────────────────────────────────────
    {
        "nbSql": 1,
        "nmReporte": "ventas_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "ventas.txt - Detalle de ventas del periodo",
        "txSqlExtrae": """SELECT
    cv.idPuntoVenta       AS `Codigo Cliente`,
    cv.documento_id       AS `Codigo Vendedor`,
    cv.nbProducto         AS `Codigo Producto (Sku)`,
    DATE_FORMAT(cv.dtContabilizacion, '%Y/%m/%d') AS `Fecha`,
    cv.nbFactura          AS `Numero Documento`,
    CAST(cv.cantAsignada AS SIGNED) AS `Cantidad`,
    ROUND(cv.vlrTotalconIva, 2) AS `Valor Total Item Vendido`,
    CASE WHEN cv.td IN ('FD','NC') THEN '1' ELSE '0' END AS `Tipo`,
    ROUND(cv.costodeMercancia, 2) AS `Costo`,
    'UND'                 AS `Unidad de Medida`,
    COALESCE(cv.nbAlmacen, '91') AS `Codigo bodega`,
    cv.nmProveedor        AS `Proveedor`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff""",
    },
    # ─── 2. PRODUCTOS (SKU) ─────────────────────────────────────
    {
        "nbSql": 2,
        "nmReporte": "productos_tsol",
        "txTabla": "productos",
        "txDescripcion": "SKU (Productos).txt - Catalogo de productos",
        "txSqlExtrae": """SELECT
    p.nbProducto          AS `Codigo`,
    p.nmProducto          AS `Nombre`,
    'RG'                  AS `Tipo Referencia`,
    COALESCE(p.nmudm, 'UND') AS `Tipo De Unidad`,
    COALESCE(p.nbBarra, '') AS `Codigo De Barras`,
    COALESCE(p.nmTpCategoria, '001') AS `Codigo Categoria`,
    COALESCE(p.nmTpCategoria, 'GENERAL') AS `Nombre Categoria`,
    COALESCE(p.tpProducto, '001') AS `Codigo SubCategoria`,
    COALESCE(p.tpProducto, 'GENERAL') AS `Nombre SubCategoria`,
    COALESCE(p.nbEmbalaje, 1) AS `Factor Conversion Unidad`,
    COALESCE(p.nbPeso, 1)  AS `Factor Peso`,
    COALESCE(i.nbAlmacen, '') AS `Codigo Sede`,
    COALESCE(i.nbAlmacen, '') AS `Nombre Sede`,
    COALESCE(p.nmProveedor, 'SIN PROVEEDOR') AS `Proveedor`
FROM productos p
LEFT JOIN (
    SELECT DISTINCT nbProducto, nbAlmacen
    FROM inventario
    WHERE InvDisponible > 0
) i ON p.nbProducto = i.nbProducto
WHERE p.idEstado = 1""",
    },
    # ─── 3. CLIENTES ────────────────────────────────────────────
    {
        "nbSql": 3,
        "nmReporte": "clientes_tsol",
        "txTabla": "clientes",
        "txDescripcion": "Clientes.txt - Maestro de clientes con sede via cuboventas",
        "txSqlExtrae": """SELECT
    c.cliente_id          AS `Codigo`,
    c.nombre              AS `Nombre`,
    COALESCE(DATE_FORMAT(c.fingreso_dt, '%Y/%m/%d'), '') AS `Fecha Ingreso`,
    COALESCE(c.nit, '0')     AS `Nit`,
    COALESCE(c.direccion, '') AS `Direccion`,
    COALESCE(c.telefono, '0') AS `Telefono`,
    COALESCE(c.contacto_nm, 'NA') AS `Representante Legal`,
    COALESCE(c.dane, '76001000') AS `Codigo Municipio`,
    COALESCE(c.tipologia, '0') AS `Codigo Tipo Negocio`,
    COALESCE(c.nivelsocioeconomico, '4') AS `Estrato`,
    COALESCE(cv_sede.nbAlmacen, '') AS `Codigo Sede`,
    COALESCE(cv_sede.nbAlmacen, '') AS `Nombre Sede`,
    COALESCE(c.longitud_cl, '') AS `Ubicacion longitud`,
    COALESCE(c.latitud_cl, '') AS `Ubicacion latitud`,
    c.cliente_id          AS `Identificador de sucursal`
FROM clientes c
INNER JOIN (
    SELECT cv.idPuntoVenta, MAX(cv.nbAlmacen) AS nbAlmacen
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
    GROUP BY cv.idPuntoVenta
) cv_sede ON c.cliente_id = cv_sede.idPuntoVenta""",
    },
    # ─── 4. VENDEDORES ──────────────────────────────────────────
    {
        "nbSql": 4,
        "nmReporte": "vendedores_tsol",
        "txTabla": "zona",
        "txDescripcion": "Vendedores.txt - Vendedores activos (zona + supervisores_macrozona)",
        "txSqlExtrae": """SELECT
    z.documento_id        AS `Codigo`,
    z.zona_nm             AS `Nombre`,
    COALESCE(z.macro, 'SIN UBICACION') AS `Ubicacion`,
    COALESCE(z.documento_id, '0') AS `Cedula`,
    COALESCE(CAST(sm.supervisor_id AS CHAR), '0000') AS `Codigo Supervisor`,
    COALESCE(z.nbAlmacen, '') AS `Codigo Sede`,
    COALESCE(z.nbAlmacen, '') AS `Nombre Sede`
FROM zona z
LEFT JOIN supervisores_macrozona sm ON z.macrozona_id = sm.macrozona_id
WHERE z.documento_id IS NOT NULL
  AND z.documento_id != ''
  AND z.idEstado = 1""",
    },
    # ─── 5. SUPERVISORES ────────────────────────────────────────
    {
        "nbSql": 5,
        "nmReporte": "supervisores_tsol",
        "txTabla": "supervisores",
        "txDescripcion": "Supervisores.txt - Maestro de supervisores (supervisores + macrozona + zona)",
        "txSqlExtrae": """SELECT DISTINCT
    CAST(s.id AS CHAR)    AS `Codigo`,
    s.nombre              AS `Nombre`,
    COALESCE(z.nbAlmacen, '') AS `Codigo Sede`,
    COALESCE(z.nbAlmacen, '') AS `Nombre Sede`
FROM supervisores s
JOIN supervisores_macrozona sm ON s.id = sm.supervisor_id
LEFT JOIN zona z ON sm.macrozona_id = z.macrozona_id AND z.idEstado = 1
WHERE s.activo = 1""",
    },
    # ─── 6. INVENTARIO ──────────────────────────────────────────
    {
        "nbSql": 6,
        "nmReporte": "inventario_tsol",
        "txTabla": "inventario",
        "txDescripcion": "Inventario.txt - Stock disponible por producto y bodega",
        "txSqlExtrae": """SELECT
    DATE_FORMAT(NOW(), '%Y/%m/%d') AS `Fecha`,
    i.nbProducto          AS `Codigo Producto`,
    CAST(COALESCE(i.InvDisponible, 0) AS SIGNED) AS `Cantidad`,
    'UND'                 AS `Unidad de Medida`,
    COALESCE(i.nbAlmacen, '') AS `Codigo de bodega`,
    COALESCE(i.nbAlmacen, '') AS `Codigo Sede`,
    COALESCE(i.nbAlmacen, '') AS `Nombre Sede`
FROM inventario i
WHERE i.InvDisponible > 0""",
    },
    # ─── 7. TIPOS DE NEGOCIO ────────────────────────────────────
    {
        "nbSql": 7,
        "nmReporte": "tipos_negocio_tsol",
        "txTabla": "clientes",
        "txDescripcion": "Tipos De Negocio.txt - Tipologias de clientes",
        "txSqlExtrae": """SELECT DISTINCT
    c.tipologia           AS `Codigo`,
    c.tipologia           AS `Nombre`
FROM clientes c
WHERE c.tipologia IS NOT NULL
  AND c.tipologia != ''
ORDER BY c.tipologia""",
    },
    # ─── 8. MUNICIPIOS ──────────────────────────────────────────
    {
        "nbSql": 8,
        "nmReporte": "municipios_tsol",
        "txTabla": "clientes",
        "txDescripcion": "Municipios.txt - Municipios de clientes activos",
        "txSqlExtrae": """SELECT DISTINCT
    COALESCE(c.dane, '76001000') AS `Codigo`,
    COALESCE(c.ciudad, '')       AS `Nombre`
FROM clientes c
WHERE c.dane IS NOT NULL
  AND c.dane != ''
  AND c.cliente_id IN (
    SELECT DISTINCT cv.idPuntoVenta
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
)
ORDER BY c.dane""",
    },
    # ─── 9. RUTAS ───────────────────────────────────────────────
    {
        "nbSql": 9,
        "nmReporte": "rutas_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "Rutas.txt - Relacion vendedor-cliente del periodo",
        "txSqlExtrae": """SELECT DISTINCT
    cv.documento_id       AS `Codigo Vendedor`,
    cv.idPuntoVenta       AS `Codigo Cliente`,
    MONTH(:ff)            AS `Mes`,
    1                     AS `Dia Semana`,
    4                     AS `Frecuencia`,
    COALESCE(cv.nbAlmacen, '') AS `Codigo Sede`,
    COALESCE(cv.nbAlmacen, '') AS `Nombre Sede`,
    cv.idPuntoVenta       AS `Identificador de sucursal`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff""",
    },
    # ─── 10. LISTADO DE FACTURAS ─────────────────────────────────
    {
        "nbSql": 10,
        "nmReporte": "facturas_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "Listado de Facturas.txt - Facturas agrupadas",
        "txSqlExtrae": """SELECT
    cv.idPuntoVenta       AS `Codigo Cliente`,
    cv.documento_id       AS `Codigo Vendedor`,
    DATE_FORMAT(cv.dtContabilizacion, '%Y/%m/%d') AS `Fecha`,
    cv.nbFactura          AS `Numero Documento`,
    ROUND(SUM(cv.vlrTotalconIva), 2) AS `Valor_Total_Factura`,
    ROUND(SUM(cv.vlrTotalconIva), 2) AS `Valor_Facturado_Casa_Comercial`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
GROUP BY cv.idPuntoVenta, cv.documento_id, cv.dtContabilizacion, cv.nbFactura""",
    },
    # ─── 11. TOTALES DE CONTROL ──────────────────────────────────
    {
        "nbSql": 11,
        "nmReporte": "totales_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "Totales de Control.txt - Total neto ventas (FD/NC como devolucion)",
        "txSqlExtrae": """SELECT
    'TotalValorVenta' AS `Descriptor Total`,
    ROUND(
        SUM(CASE WHEN cv.td NOT IN ('FD','NC') THEN cv.vlrTotalconIva ELSE 0 END)
      - SUM(CASE WHEN cv.td IN ('FD','NC') THEN cv.vlrTotalconIva ELSE 0 END)
    , 2) AS `Valor`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff""",
    },
]


def main():
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=False,
    )

    try:
        with conn.cursor() as cur:
            # 1. Reemplazar queries 1-11 con las queries correctas
            for q in SQL_TSOL:
                cur.execute(
                    """INSERT INTO conf_sql_tsol
                       (nbSql, nmReporte, txTabla, txDescripcion, txSqlExtrae,
                        fecha_creacion, fecha_actualizacion)
                       VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                       ON DUPLICATE KEY UPDATE
                         nmReporte = VALUES(nmReporte),
                         txTabla = VALUES(txTabla),
                         txDescripcion = VALUES(txDescripcion),
                         txSqlExtrae = VALUES(txSqlExtrae),
                         fecha_actualizacion = NOW()""",
                    (q["nbSql"], q["nmReporte"], q["txTabla"],
                     q["txDescripcion"], q["txSqlExtrae"]),
                )
                print(f"  [OK] nbSql={q['nbSql']} ({q['nmReporte']})")

            # 2. Eliminar queries 12-22 (ya no necesarias)
            cur.execute("DELETE FROM conf_sql_tsol WHERE nbSql BETWEEN 12 AND 22")
            deleted = cur.rowcount
            print(f"\n  [OK] Eliminadas {deleted} queries antiguas (nbSql 12-22)")

            # 3. Actualizar planos_tsol de latorre para usar 1-11
            new_planos = json.dumps(list(range(1, 12)))  # [1,2,...,11]
            cur.execute(
                """UPDATE conf_empresas
                   SET planos_tsol = %s
                   WHERE id = 631""",
                (new_planos,),
            )
            print(f"\n  [OK] latorre (id=631) planos_tsol -> {new_planos}")

            # 4. Verificar
            cur.execute("SELECT nbSql, nmReporte FROM conf_sql_tsol ORDER BY nbSql")
            rows = cur.fetchall()
            print(f"\n  Verificacion: {len(rows)} registros en conf_sql_tsol:")
            for r in rows:
                print(f"    nbSql={r[0]}, nmReporte={r[1]}")

            cur.execute("SELECT planos_tsol FROM conf_empresas WHERE id = 631")
            row = cur.fetchone()
            print(f"\n  latorre planos_tsol = {row[0]}")

        conn.commit()
        print("\n  COMMIT exitoso.")

    except Exception as e:
        conn.rollback()
        print(f"\n  ERROR: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
