from django.db import models


class Clientes(models.Model):
    """Tabla de dimensión - Clientes (desde PROVEE-TSOL.xlsx hoja CLIENTES)"""
    cod_cliente = models.CharField(max_length=30, primary_key=True, db_column="cod_cliente")
    nom_cliente = models.TextField(db_column="nom_cliente", blank=True, null=True)
    empresas = models.CharField(max_length=100, db_column="empresas", blank=True, null=True)
    fecha_ingreso = models.DateField(db_column="fecha_ingreso", blank=True, null=True)
    nit = models.CharField(max_length=50, db_column="nit", blank=True, null=True)
    direccion = models.TextField(db_column="direccion", blank=True, null=True)
    telefono = models.CharField(max_length=50, db_column="telefono", blank=True, null=True)
    representante_legal = models.CharField(max_length=200, db_column="representante_legal", blank=True, null=True)
    codigo_municipio = models.CharField(max_length=20, db_column="codigo_municipio", blank=True, null=True)
    codigo_negocio = models.CharField(max_length=20, db_column="codigo_negocio", blank=True, null=True)
    tipo_negocio = models.CharField(max_length=50, db_column="tipo_negocio", blank=True, null=True)
    estracto = models.CharField(max_length=10, db_column="estracto", blank=True, null=True)
    barrio = models.CharField(max_length=100, db_column="barrio", blank=True, null=True)
    clasificacion = models.CharField(max_length=100, db_column="clasificacion", blank=True, null=True)
    comuna = models.CharField(max_length=50, db_column="comuna", blank=True, null=True)
    estrato = models.CharField(max_length=10, db_column="estrato", blank=True, null=True)
    coord = models.CharField(max_length=100, db_column="coord", blank=True, null=True)
    longitud = models.FloatField(db_column="longitud", blank=True, null=True)
    latitud = models.FloatField(db_column="latitud", blank=True, null=True)

    class Meta:
        db_table = "dim_clientes"
        managed = False
        
    def __str__(self):
        return f"{self.cod_cliente} - {self.nom_cliente}"


class Productos(models.Model):
    """Tabla de dimensión - Productos (desde PROVEE-TSOL.xlsx hoja PRODUCTO)"""
    codigo_sap = models.CharField(max_length=50, primary_key=True, db_column="codigo_sap")
    nombre = models.TextField(db_column="nombre", blank=True, null=True)
    tipo_referencia = models.CharField(max_length=10, db_column="tipo_referencia", blank=True, null=True)
    unidad = models.CharField(max_length=20, db_column="unidad", blank=True, null=True)
    codigo_barras = models.CharField(max_length=50, db_column="codigo_barras", blank=True, null=True)
    proveedor = models.CharField(max_length=100, db_column="proveedor", blank=True, null=True)
    proveedor_2 = models.CharField(max_length=100, db_column="proveedor_2", blank=True, null=True)  # Tía Mati - marca propia
    categoria = models.CharField(max_length=100, db_column="categoria", blank=True, null=True)
    tipo_prod = models.CharField(max_length=100, db_column="tipo_prod", blank=True, null=True)
    contenido = models.CharField(max_length=100, db_column="contenido", blank=True, null=True)

    class Meta:
        db_table = "dim_productos"
        managed = False
        
    def __str__(self):
        return f"{self.codigo_sap} - {self.nombre}"


class Proveedores(models.Model):
    """Tabla de dimensión - Proveedores (desde PROVEE-TSOL.xlsx hoja Proveedores)"""
    codigo_proveedor = models.CharField(max_length=50, primary_key=True, db_column="codigo_proveedor")
    emails = models.TextField(db_column="emails", blank=True, null=True)
    copia = models.TextField(db_column="copia", blank=True, null=True)
    columnas = models.IntegerField(db_column="columnas", blank=True, null=True)
    periodo = models.FloatField(db_column="periodo", blank=True, null=True)
    ppks = models.CharField(max_length=10, db_column="ppks", blank=True, null=True)

    class Meta:
        db_table = "dim_proveedores"
        managed = False
        
    def __str__(self):
        return self.codigo_proveedor


class Estructura(models.Model):
    """Tabla de dimensión - Estructura de vendedores y fuerza de ventas (desde PROVEE-TSOL.xlsx hoja ESTRUCTURA)"""
    cod_ejecutivo = models.CharField(max_length=30, primary_key=True, db_column="cod_ejecutivo")
    nom_ejecutivo = models.CharField(max_length=200, db_column="nom_ejecutivo", blank=True, null=True)
    lider_tsol = models.CharField(max_length=200, db_column="lider_tsol", blank=True, null=True)
    empresa = models.CharField(max_length=100, db_column="empresa", blank=True, null=True)
    lider_comercial = models.CharField(max_length=200, db_column="lider_comercial", blank=True, null=True)
    cod_bod = models.CharField(max_length=20, db_column="cod_bod", blank=True, null=True)
    bodega = models.CharField(max_length=100, db_column="bodega", blank=True, null=True)
    area_nombre = models.CharField(max_length=100, db_column="area_nombre", blank=True, null=True)

    class Meta:
        db_table = "dim_estructura"
        managed = False
        
    def __str__(self):
        return f"{self.cod_ejecutivo} - {self.nom_ejecutivo}"


class CuotasVendedores(models.Model):
    """Tabla de hechos - Historial de cuotas mensuales por vendedor"""
    id = models.AutoField(primary_key=True, db_column="id")
    cod_ejecutivo = models.CharField(max_length=30, db_column="cod_ejecutivo")
    periodo = models.CharField(max_length=20, db_column="periodo")  # Formato: "SEPTIEMBRE 2025", "AGOSTO 2025", etc.
    anio = models.IntegerField(db_column="anio")
    mes = models.IntegerField(db_column="mes")  # 1-12
    mes_nombre = models.CharField(max_length=15, db_column="mes_nombre")  # "SEPTIEMBRE"
    cuota = models.FloatField(db_column="cuota", blank=True, null=True)
    fecha_actualizacion = models.DateTimeField(db_column="fecha_actualizacion", auto_now=True)

    class Meta:
        db_table = "fact_cuotas_vendedores"
        managed = False
        unique_together = ['cod_ejecutivo', 'anio', 'mes']  # No duplicar cuota del mismo mes
        
    def __str__(self):
        return f"{self.cod_ejecutivo} - {self.periodo}: {self.cuota}"


class ProductosColgate(models.Model):
    """Tabla de dimensión - Productos Colgate (desde 023-COLGATE PALMOLIVE.xlsx hoja Productos EQ)"""
    cod_texto = models.CharField(max_length=50, primary_key=True, db_column="cod_texto")
    pro_cod = models.CharField(max_length=50, db_column="pro_cod", blank=True, null=True)
    producto = models.TextField(db_column="producto", blank=True, null=True)
    alterno = models.CharField(max_length=50, db_column="alterno", blank=True, null=True)
    cod = models.CharField(max_length=50, db_column="cod", blank=True, null=True)
    portafolio = models.IntegerField(db_column="portafolio", blank=True, null=True)
    inici = models.IntegerField(db_column="inici", blank=True, null=True)
    subc = models.CharField(max_length=100, db_column="subc", blank=True, null=True)
    producto_alt = models.TextField(db_column="producto_alt", blank=True, null=True)

    class Meta:
        db_table = "dim_productos_colgate"
        managed = False
        
    def __str__(self):
        return f"{self.cod_texto} - {self.producto}"


class Rutero(models.Model):
    """Tabla de dimensión - Rutero completo (desde rutero_distrijass_total.xlsx)"""
    codigo = models.CharField(max_length=50, primary_key=True, db_column="codigo")
    cod_asesor = models.CharField(max_length=30, db_column="cod_asesor", blank=True, null=True)
    asesores = models.CharField(max_length=200, db_column="asesores", blank=True, null=True)
    documento = models.CharField(max_length=50, db_column="documento", blank=True, null=True)
    cliente = models.CharField(max_length=200, db_column="cliente", blank=True, null=True)
    razon_social = models.CharField(max_length=200, db_column="razon_social", blank=True, null=True)
    direccion = models.TextField(db_column="direccion", blank=True, null=True)
    barrio = models.CharField(max_length=100, db_column="barrio", blank=True, null=True)
    ciudad = models.CharField(max_length=100, db_column="ciudad", blank=True, null=True)
    telefono = models.CharField(max_length=50, db_column="telefono", blank=True, null=True)
    sucursal = models.CharField(max_length=20, db_column="sucursal", blank=True, null=True)
    dias = models.CharField(max_length=20, db_column="dias", blank=True, null=True)
    orden = models.IntegerField(db_column="orden", blank=True, null=True)
    orden_dia = models.IntegerField(db_column="orden_dia", blank=True, null=True)
    email = models.CharField(max_length=200, db_column="email", blank=True, null=True)
    segmento = models.CharField(max_length=100, db_column="segmento", blank=True, null=True)
    telefono2 = models.CharField(max_length=50, db_column="telefono2", blank=True, null=True)

    class Meta:
        db_table = "dim_rutero"
        managed = False
        
    def __str__(self):
        return f"{self.codigo} - {self.cliente}"


class AsiVamos(models.Model):
    """Tabla de dimensión - Información de correos Así Vamos (desde PROVEE-TSOL.xlsx hoja Envío Así Vamos)"""
    asesor = models.CharField(max_length=200, primary_key=True, db_column="asesor")
    para = models.TextField(db_column="para", blank=True, null=True)
    copia = models.TextField(db_column="copia", blank=True, null=True)
    campo_30 = models.CharField(max_length=50, db_column="campo_30", blank=True, null=True)  # La columna "30"

    class Meta:
        db_table = "dim_asi_vamos"
        managed = False
        
    def __str__(self):
        return self.asesor


class Infoventas(models.Model):
    id = models.CharField(
        max_length=255, primary_key=True, db_column="id"
    )  # Artificial PK
    cod_cliente = models.CharField(max_length=30, db_column="Cod. cliente")
    nom_cliente = models.TextField(db_column="Nom. Cliente")
    cod_vendedor = models.CharField(max_length=30, db_column="Cod. vendedor")
    nombre = models.TextField(db_column="Nombre")
    cod_productto = models.CharField(max_length=30, db_column="Cod. productto")
    descripcion = models.TextField(db_column="Descripción")
    fecha = models.DateField(db_column="Fecha")
    fac_numero = models.CharField(max_length=30, db_column="Fac. numero")
    cantidad = models.FloatField(db_column="Cantidad")
    vta_neta = models.FloatField(db_column="Vta neta")
    tipo = models.CharField(max_length=30, db_column="Tipo")
    costo = models.FloatField(db_column="Costo")
    unidad = models.CharField(max_length=30, db_column="Unidad", blank=True, null=True)
    pedido = models.CharField(max_length=30, db_column="Pedido", blank=True, null=True)
    proveedor = models.TextField(db_column="Proveedor", blank=True, null=True)
    empresa = models.CharField(
        max_length=100, db_column="Empresa", blank=True, null=True
    )
    lider = models.TextField(db_column="Líder", blank=True, null=True)
    area = models.TextField(db_column="Área", blank=True, null=True)
    codigo_bodega = models.CharField(
        max_length=30, db_column="Codigo bodega", blank=True, null=True
    )
    bodega = models.CharField(max_length=100, db_column="Bodega", blank=True, null=True)
    categoria = models.TextField(db_column="Categoría", blank=True, null=True)
    tipo_prod = models.TextField(db_column="Tipo Prod", blank=True, null=True)
    cod_barra = models.CharField(
        max_length=50, db_column="Cod. Barra", blank=True, null=True
    )
    nbLinea = models.FloatField(db_column="nbLinea", blank=True, null=True)

    class Meta:
        db_table = "infoventas"
        managed = False  # No genera migraciones ni crea/modifica la tabla

    def save(self, *args, **kwargs):
        # Poblar el campo id concatenando los valores de las claves compuestas
        self.id = f"{self.cod_cliente}_{self.cod_vendedor}_{self.cod_productto}_{self.fac_numero}_{self.tipo}_{self.nbLinea}"
        super().save(*args, **kwargs)
