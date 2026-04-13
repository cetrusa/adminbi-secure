# Migración: Correcciones a queries SQL TSOL para Latorre
#
# Las queries 2, 3, 7, 9 y 12 tenían referencias a tablas/columnas incorrectas
# (dim_clientes, dim_productos, codigo_sap, etc. que no existen en Latorre).
# Se corrigen apuntando a las tablas reales del esquema de Latorre.
#
# Cambios:
#   ID 2  productos_tsol  : dim_productos.codigo_sap → productos.nbProducto
#   ID 3  clientes_tsol   : dim_clientes.cod_cliente  → clientes.cliente_id (cols reales)
#   ID 7  tipos_negocio   : c.tipologia→Codigo Y Nombre (igual) → cuboventas tipologia_id+tipologia
#   ID 9  rutas_tsol      : cuboventas → rutas JOIN zona (dia_visita real LU/MA/MI/JU/VI/SA/DO)
#   ID 12 barrios_tsol    : dim_clientes.cod_barrio    → clientes.barrio

from django.db import migrations

FIXES = [
    # ─── ID 2: PRODUCTOS ─────────────────────────────────────────────
    # Antes: FROM dim_productos (no existe), p.codigo_sap (no existe)
    # Ahora: FROM productos, p.nbProducto (varchar con ceros a la izquierda)
    # UPPER() garantiza consistencia con el UPPER() aplicado en ventas
    {
        "nbSql": 2,
        "txSqlExtrae": """SELECT
    UPPER(p.nbProducto)   AS `Codigo`,
    p.nmProducto          AS `Nombre`,
    COALESCE(p.tpProducto, 'RG') AS `Tipo Referencia`,
    COALESCE(p.nmudm, 'UND') AS `Tipo De Unidad`,
    COALESCE(p.nbBarra, '') AS `Codigo De Barras`,
    COALESCE(p.nmTpCategoria, '001') AS `Codigo Categoria`,
    COALESCE(p.nmTpCategoria, 'GENERAL') AS `Nombre Categoria`,
    COALESCE(p.nmTpFamilia, '001') AS `Codigo SubCategoria`,
    COALESCE(p.nmTpFamilia, 'GENERAL') AS `Nombre SubCategoria`,
    COALESCE(p.nbEmbalaje, 1) AS `Factor Conversion Unidad`,
    1                     AS `Factor Peso`,
    '91'                  AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`,
    COALESCE(p.nmProveedor, 'SIN PROVEEDOR') AS `Proveedor`,
    CAST(COALESCE(p.idProveedor, 0) AS CHAR) AS `Codigo Proveedor`
FROM productos p""",
    },
    # ─── ID 3: CLIENTES ──────────────────────────────────────────────
    # Antes: FROM dim_clientes, cod_cliente, nom_cliente, codigo_negocio, etc.
    # Ahora: FROM clientes con columnas reales + JOIN en lugar de IN (subquery)
    # tipologia default='1' cuando NULL (según indicación del cliente)
    {
        "nbSql": 3,
        "txSqlExtrae": """SELECT DISTINCT
    c.cliente_id          AS `Codigo`,
    c.nombre              AS `Nombre`,
    COALESCE(DATE_FORMAT(c.fingreso_dt, '%Y/%m/%d'), '') AS `Fecha Ingreso`,
    COALESCE(c.nit, '0')     AS `Nit`,
    COALESCE(c.direccion, '') AS `Direccion`,
    COALESCE(c.telefono, '0') AS `Telefono`,
    COALESCE(c.contacto, 'NA') AS `Representante Legal`,
    COALESCE(c.dane, '76001') AS `Codigo Municipio`,
    COALESCE(c.tipologia, '1') AS `Codigo Tipo Negocio`,
    COALESCE(c.nivelsocioeconomico, '4') AS `Estrato`,
    '91'                  AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`,
    COALESCE(c.longitud_cl, -76.3) AS `Ubicacion longitud`,
    COALESCE(c.latitud_cl, 3.45)   AS `Ubicacion latitud`,
    '001'                 AS `Identificador de sucursal`,
    COALESCE(c.barrio, '0') AS `Codigo Barrio`
FROM clientes c
JOIN (
    SELECT DISTINCT idPuntoVenta
    FROM cuboventas
    WHERE dtContabilizacion BETWEEN :fi AND :ff
) cv ON cv.idPuntoVenta = c.cliente_id""",
    },
    # ─── ID 7: TIPOS DE NEGOCIO ──────────────────────────────────────
    # Antes: c.tipologia AS Codigo, c.tipologia AS Nombre (ambos iguales = bug)
    # Ahora: cuboventas.tipologia_id como Codigo y cuboventas.tipologia como Nombre
    # (la tabla clientes solo tiene el código; el nombre está en cuboventas)
    {
        "nbSql": 7,
        "txSqlExtrae": """SELECT DISTINCT
    cv.tipologia_id       AS `Codigo`,
    cv.tipologia          AS `Nombre`
FROM cuboventas cv
WHERE cv.tipologia_id IS NOT NULL
  AND cv.tipologia_id != ''
  AND cv.tipologia IS NOT NULL
  AND cv.tipologia != ''
ORDER BY cv.tipologia_id""",
    },
    # ─── ID 9: RUTAS ─────────────────────────────────────────────────
    # Antes: derivado de cuboventas (solo días con ventas, no todo el plan)
    # Ahora: FROM rutas JOIN zona → dia_visita real (LU/MA/MI/JU/VI/SA/DO)
    # JOIN en lugar de IN (subquery) para mejor rendimiento
    {
        "nbSql": 9,
        "txSqlExtrae": """SELECT DISTINCT
    z.documento_id        AS `Codigo Vendedor`,
    r.cliente_id          AS `Codigo Cliente`,
    MONTH(:ff)            AS `Mes`,
    CASE r.dia_visita
        WHEN 'LU' THEN 1
        WHEN 'MA' THEN 2
        WHEN 'MI' THEN 3
        WHEN 'JU' THEN 4
        WHEN 'VI' THEN 5
        WHEN 'SA' THEN 6
        WHEN 'DO' THEN 7
        ELSE 1
    END                   AS `Dia Semana`,
    4                     AS `Frecuencia`,
    z.nbAlmacen           AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`,
    '001'                 AS `Identificador de sucursal`
FROM rutas r
JOIN zona z ON z.zona_id = r.zona_id
JOIN (
    SELECT DISTINCT idPuntoVenta
    FROM cuboventas
    WHERE dtContabilizacion BETWEEN :fi AND :ff
) cv ON cv.idPuntoVenta = r.cliente_id
WHERE r.dia_visita IS NOT NULL
  AND r.dia_visita NOT IN ('', '00')
  AND z.documento_id IS NOT NULL
  AND z.documento_id NOT IN ('', '0000')""",
    },
    # ─── ID 12: BARRIOS ──────────────────────────────────────────────
    # Antes: FROM dim_clientes con c.cod_barrio (no existe, barrio_id=0 para todos)
    # Ahora: FROM clientes con c.barrio (nombre del barrio como código único)
    # JOIN en lugar de IN (subquery) para mejor rendimiento
    {
        "nbSql": 12,
        "txSqlExtrae": """SELECT DISTINCT
    COALESCE(c.barrio, 'SIN BARRIO') AS `Codigo`,
    COALESCE(c.barrio, 'SIN BARRIO') AS `Nombre`,
    COALESCE(c.dane, '76001')        AS `Codigo Municipio`
FROM clientes c
JOIN (
    SELECT DISTINCT idPuntoVenta
    FROM cuboventas
    WHERE dtContabilizacion BETWEEN :fi AND :ff
) cv ON cv.idPuntoVenta = c.cliente_id
WHERE c.barrio IS NOT NULL
  AND c.barrio != ''
ORDER BY `Codigo`""",
    },
]


def apply_fixes(apps, schema_editor):
    ConfSqlTsol = apps.get_model("permisos", "ConfSqlTsol")
    for fix in FIXES:
        ConfSqlTsol.objects.filter(nbSql=fix["nbSql"]).update(
            txSqlExtrae=fix["txSqlExtrae"]
        )


def reverse_fixes(apps, schema_editor):
    # No hay rollback seguro sin guardar el estado previo
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0030_fix_conf_sql_tsol"),
    ]

    operations = [
        migrations.RunPython(apply_fixes, reverse_fixes),
    ]
