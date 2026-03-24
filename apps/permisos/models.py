from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import RegexValidator, MinValueValidator
from django.utils import timezone
from django.core.exceptions import ValidationError
import re
from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType


class PermisosBarra(models.Model):
    """
    Modelo para definir los permisos de la aplicación.
    Este modelo no crea una tabla en la base de datos, solo define permisos.
    """

    class Meta:
        managed = False
        permissions = (
            ("nav_bar", _("Ver la barra de menú")),
            ("panel_cubo", _("Panel de cubo")),
            ("panel_bi", _("Panel de BI")),
            ("panel_actualizacion", _("Panel de Actualización de datos")),
            ("panel_interface", _("Panel de Interfaces Contables")),
            ("cubo", _("Generar cubo de ventas")),
            ("proveedor", _("Generar cubo de ventas para proveedor")),
            ("matrix", _("Generar Matrix de Ventas")),
            ("interface", _("Generar interface contable")),
            ("interface_siigo", _("Generar interface Siigo")),
            ("plano", _("Generar archivo plano")),
            ("cargue_plano", _("Cargar archivo plano")),
            ("cargue_tsol", _("Cargue archivo plano TSOL")),
            ("informe_bi", _("Informe BI")),
            ("informe_bi_embed", _("Informe BI Embed")),
            ("actualizar_base", _("Actualización de datos")),
            ("actualizacion_bi", _("Actualizar BI")),
            ("admin", _("Ir al Administrador")),
            ("amovildesk", _("Puede ver Informe Amovildesk")),
            ("reportes", _("Puede ver Reportes")),
            ("reportes_bimbo", _("Puede ver Reportes Bimbo (Venta Cero, Ruteros)")),
            ("reporte_preventa_bimbo", _("Puede ver Reporte Preventa Bimbo")),
            ("cargue_infoventas", _("Cargar Archivo Infoventas")),
            ("cargue_maestras", _("Cargar Tablas Maestras")),
            ("cargue_infoproducto", _("Cargar Información de Producto")),
            ("cargue_infoproveedor", _("Cargar Información de Proveedor")),
            ("faltantes", _("Generar informe de Faltantes")),
            ("preventa", _("Generar informe de Preventa")),
            ("trazabilidad", _("Generar informe de Trazabilidad Preventa")),
            ("config_email_reportes", _("Configurar correos para reportes programados")),
            ("ejecutar_tsol", _("Ejecutar planos TSOL")),
            ("ejecutar_cosmos", _("Ejecutar planos Cosmos")),
        )
        verbose_name = _("Permiso")
        verbose_name_plural = _("Permisos")


class ConfDt(models.Model):
    """
    Configuración de rangos de fechas para los datos.
    """

    nbDt = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    nmDt = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Nombre Rango de Fecha"),
        help_text=_("Nombre descriptivo para identificar el rango de fechas"),
    )
    txDtIni = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Fecha Inicial"),
        help_text=_("Fecha de inicio del periodo"),
    )
    txDtFin = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Fecha Final"),
        help_text=_("Fecha de fin del periodo"),
    )

    def __str__(self):
        if self.nmDt:
            return f"{self.nmDt} ({self.txDtIni} - {self.txDtFin})"
        return f"Rango {self.nbDt}: {self.txDtIni} - {self.txDtFin}"

    def clean(self):
        """Validaciones personalizadas para el modelo."""
        # Aquí puedes añadir validaciones para las fechas si necesitas
        # convertir el texto a objeto date y validar que fecha_fin > fecha_inicio
        pass

    class Meta:
        db_table = "conf_dt"
        # managed = False
        verbose_name = _("Configuración Rango de Fecha")
        verbose_name_plural = _("Configuración Rangos de Fechas")
        ordering = ["nbDt"]  # Ordenar por ID por defecto


class ConfEmpresas(models.Model):
    """
    Configuración de empresas y sus conexiones a bases de datos.
    """

    id = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    nmEmpresa = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Nombre Empresa"),
        help_text=_("Nombre completo de la empresa"),
    )
    name = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Nombre de la Base"),
        help_text=_("Nombre de la base de datos"),
    )
    nbServerSidis = models.BigIntegerField(
        null=True,
        blank=True,
        verbose_name=_("ID Servidor Sidis"),
        help_text=_("ID del servidor donde se aloja Sidis"),
    )
    dbSidis = models.CharField(
        max_length=150,
        null=True,
        blank=True,
        verbose_name=_("Base de datos Sidis"),
        help_text=_("Nombre de la base de datos Sidis"),
    )
    nbServerBi = models.BigIntegerField(
        null=True,
        blank=True,
        verbose_name=_("ID Servidor PowerBI"),
        help_text=_("ID del servidor donde se aloja PowerBI"),
    )
    dbBi = models.CharField(
        max_length=150,
        null=True,
        blank=True,
        verbose_name=_("Base de datos BI"),
        help_text=_("Nombre de la base de datos de Business Intelligence"),
    )
    txProcedureExtrae = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL Extractor"),
        help_text=_("Nombre del procedimiento para extracción de datos"),
    )
    txProcedureCargue = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL del Cargue"),
        help_text=_("Procedimiento SQL para carga de datos"),
    )
    # ... Resto de campos del modelo original ...
    nmProcedureInterface = models.CharField(
        max_length=30,
        null=True,
        blank=True,
        verbose_name=_("Procedimiento Interface"),
        help_text=_("Nombre del procedimiento para la interfaz"),
    )
    txProcedureInterface = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL de Interface"),
        help_text=_("SQL para el procesamiento de interfaz"),
    )
    nmProcedureExcel = models.CharField(
        max_length=30,
        null=True,
        blank=True,
        verbose_name=_("Procedimiento a Excel"),
        help_text=_("Nombre del procedimiento para exportar a Excel"),
    )
    txProcedureExcel = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL a Excel"),
        help_text=_("SQL para exportación a Excel"),
    )
    nmProcedureExcel2 = models.CharField(
        max_length=30,
        null=True,
        blank=True,
        verbose_name=_("Procedimiento a Excel2"),
        help_text=_("Nombre del procedimiento alternativo para Excel"),
    )
    txProcedureExcel2 = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL a Excel2"),
        help_text=_("SQL alternativo para exportación a Excel"),
    )
    nmProcedureCsv = models.CharField(
        max_length=30,
        null=True,
        blank=True,
        verbose_name=_("Procedimiento a CSV"),
        help_text=_("Nombre del procedimiento para exportar a CSV"),
    )
    txProcedureCsv = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL a CSV"),
        help_text=_("SQL para exportación a CSV"),
    )
    nmProcedureCsv2 = models.CharField(
        max_length=30,
        null=True,
        blank=True,
        verbose_name=_("Procedimiento a CSV2"),
        help_text=_("Nombre del procedimiento alternativo para CSV"),
    )
    txProcedureCsv2 = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL a CSV2"),
        help_text=_("SQL alternativo para exportación a CSV"),
    )
    nmProcedureSql = models.CharField(
        max_length=30,
        null=True,
        blank=True,
        verbose_name=_("Procedimiento a SQL"),
        help_text=_("Nombre del procedimiento para exportar SQL"),
    )
    txProcedureSql = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Procesos SQL a SQL"),
        help_text=_("SQL para exportación SQL"),
    )
    report_id_powerbi = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name=_("ID Reporte PowerBI"),
        help_text=_("Identificador del reporte en PowerBI"),
    )
    dataset_id_powerbi = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name=_("Dataset PowerBI"),
        help_text=_("Identificador del conjunto de datos en PowerBI"),
    )
    url_powerbi = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("URL Pública PowerBI"),
        help_text=_("URL para acceder al reporte público en PowerBI"),
    )
    es_bimbo = models.BooleanField(
        default=False,
        verbose_name=_("Es Bimbo"),
        help_text=_("Indica si la empresa opera con BIMBO"),
    )
    ceve = models.IntegerField(
        null=True,
        blank=True,
        verbose_name=_("Código CEVE"),
        help_text=_("Código CEVE asignado si es agencia BIMBO"),
    )
    idProveedorBimbo = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("IDs Proveedor Bimbo"),
        help_text=_("IDs proveedor BIMBO separados por coma (ej: 6,40). Fuente real: proveedores_agencia_bimbo"),
    )
    estado = models.IntegerField(
        null=True,
        blank=True,
        verbose_name=_("Activo"),
        help_text=_("Estado de la empresa (1: activo, 0: inactivo)"),
    )
    envio_email_activo = models.BooleanField(
        default=False,
        verbose_name=_("Envío email activo"),
        help_text=_("Habilita el envío nocturno automático de reportes por correo para esta empresa"),
    )
    # ── Configuración CDT ──────────────────────────────────────────
    envio_cdt_activo = models.BooleanField(
        default=False,
        verbose_name=_("Envío CDT activo"),
        help_text=_("Habilita el envío nocturno automático de planos CDT para esta empresa"),
    )
    planos_cdt = models.CharField(
        max_length=200,
        null=True,
        blank=True,
        verbose_name=_("Planos CDT (IDs)"),
        help_text=_("Lista de IDs de conf_sql_cdt (ej: [1,2,3])"),
    )
    cdt_nombre_proveedor = models.CharField(
        max_length=100, null=True, blank=True,
        verbose_name=_("Nombre Proveedor CDT"),
        help_text=_("Nombre del proveedor CDT (ej: MasterFoods Colombia)"),
    )
    cdt_codigo_proveedor = models.CharField(
        max_length=100, null=True, blank=True,
        verbose_name=_("Código Proveedor CDT"),
        help_text=_("Código para filtrar en cuboventas (ej: 006-MASTERFOODS COLOMBIA LTDA)"),
    )
    cdt_codigos_distribuidor = models.TextField(
        null=True, blank=True,
        verbose_name=_("Códigos Distribuidor CDT"),
        help_text=_(
            'JSON con códigos de distribuidor. Ejemplo: '
            '[{"codigo": "17645695", "empresa": "Distrijass", "tipo": "con_vendedores"}]'
        ),
    )
    cdt_vendedores_especiales = models.CharField(
        max_length=200, null=True, blank=True,
        verbose_name=_("Vendedores Especiales CDT"),
        help_text=_("Vendedores especiales separados por coma (ej: MA01,MA02)"),
    )
    cdt_bodega_especial = models.CharField(
        max_length=100, null=True, blank=True,
        verbose_name=_("Patrón Bodega Especial CDT"),
        help_text=_("Patrón de bodega especial para filtro de inventario (ej: SPT)"),
    )
    cdt_conexion = models.JSONField(
        null=True, blank=True, default=dict,
        verbose_name=_("Conexión SFTP CDT"),
        help_text=_('JSON: {"host": "", "port": 22, "user": "", "pass": "", "ruta_remota": "/"}'),
    )
    # ── Configuración TSOL ─────────────────────────────────────────
    envio_tsol_activo = models.BooleanField(
        default=False,
        verbose_name=_("Envío TSOL activo"),
        help_text=_("Habilita el envío nocturno automático de planos TSOL para esta empresa"),
    )
    planos_tsol = models.CharField(
        max_length=200,
        null=True,
        blank=True,
        verbose_name=_("Planos TSOL (IDs)"),
        help_text=_("Lista de IDs de conf_sql_tsol (ej: [1,2,3,4,5,6,7,8,9,10,11])"),
    )
    tsol_nombre = models.CharField(
        max_length=100, null=True, blank=True,
        verbose_name=_("Nombre TSOL"),
        help_text=_("Nombre legible (ej: Distrijass Cali)"),
    )
    tsol_codigo = models.CharField(
        max_length=100, null=True, blank=True,
        verbose_name=_("Código TSOL"),
        help_text=_("Código para nombre del ZIP (ej: DISTRIJASS_211688)"),
    )
    tsol_filtro_proveedores = models.TextField(
        null=True, blank=True,
        verbose_name=_("Filtro Proveedores TSOL"),
        help_text=_('JSON array de proveedores para filtrar. Ej: ["023-COLGATE","024-PAPELES"]'),
    )
    tsol_bodega_to_code = models.TextField(
        null=True, blank=True,
        verbose_name=_("Mapeo Bodega→Código TSOL"),
        help_text=_('JSON dict bodega→código. Ej: {"CALI":"01","PALMIRA":"01"}'),
    )
    tsol_code_to_sede = models.TextField(
        null=True, blank=True,
        verbose_name=_("Mapeo Código→Sede TSOL"),
        help_text=_('JSON dict código→nombre. Ej: {"01":"PALMIRA/CALI","02":"TULUÁ"}'),
    )
    tsol_sedes_permitidas = models.CharField(
        max_length=200, null=True, blank=True,
        verbose_name=_("Sedes Permitidas TSOL"),
        help_text=_("Códigos separados por coma (ej: 01,04,06). Vacío = todas."),
    )
    tsol_sede_default_code = models.CharField(
        max_length=10, default="01", null=True, blank=True,
        verbose_name=_("Código Sede Default TSOL"),
    )
    tsol_sede_default_name = models.CharField(
        max_length=100, default="PALMIRA/CALI", null=True, blank=True,
        verbose_name=_("Nombre Sede Default TSOL"),
    )
    tsol_conexion = models.JSONField(
        null=True, blank=True, default=dict,
        verbose_name=_("Conexión FTP TSOL"),
        help_text=_('JSON: {"host": "", "port": 21, "user": "", "pass": "", "ruta_remota": "/"}'),
    )
    # ── Configuración Cosmos ──────────────────────────────────────
    envio_cosmos_activo = models.BooleanField(
        default=False,
        verbose_name=_("Envío Cosmos activo"),
        help_text=_("Habilita el envío nocturno automático de planos Cosmos para esta empresa"),
    )
    cosmos_empresa_id = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("ID Empresa Cosmos"),
        help_text=_("Identificador Cosmos de la empresa (ej: CO-CBIA-DTR-0093)"),
    )
    planos_cosmos = models.CharField(
        max_length=200,
        null=True,
        blank=True,
        verbose_name=_("Planos Cosmos (IDs)"),
        help_text=_("Lista de IDs de conf_sql_cosmos (ej: [1,2,3])"),
    )
    cosmos_conexion = models.JSONField(
        null=True, blank=True, default=dict,
        verbose_name=_("Conexión FTPS Cosmos"),
        help_text=_('JSON: {"host": "", "port": 990, "user": "", "pass": "", "ruta_remota": "/", "certificate": ""}'),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Última actualización"),
        help_text=_("Fecha y hora de la última modificación"),
    )

    @property
    def esta_activo(self):
        """Retorna un booleano indicando si la empresa está activa."""
        return self.estado == 1

    def get_servidores(self):
        """Obtiene los servidores asociados a la empresa."""
        servidores = []
        if self.nbServerSidis:
            try:
                servidores.append(ConfServer.objects.get(nbServer=self.nbServerSidis))
            except ConfServer.DoesNotExist:
                pass
        if self.nbServerBi:
            try:
                servidores.append(ConfServer.objects.get(nbServer=self.nbServerBi))
            except ConfServer.DoesNotExist:
                pass
        return servidores

    # ── Helpers TSOL ──────────────────────────────────────────────
    def get_tsol_proveedores_list(self):
        """Retorna la lista de proveedores TSOL a filtrar como lista Python."""
        if not self.tsol_filtro_proveedores:
            return []
        import json
        try:
            return json.loads(self.tsol_filtro_proveedores)
        except (json.JSONDecodeError, TypeError):
            return []

    def get_tsol_bodega_mapping(self):
        """Retorna el mapeo bodega→código TSOL como dict."""
        if not self.tsol_bodega_to_code:
            return {}
        import json
        try:
            return json.loads(self.tsol_bodega_to_code)
        except (json.JSONDecodeError, TypeError):
            return {}

    def get_tsol_code_to_sede(self):
        """Retorna el mapeo código→nombre_sede TSOL como dict."""
        if not self.tsol_code_to_sede:
            return {}
        import json
        try:
            return json.loads(self.tsol_code_to_sede)
        except (json.JSONDecodeError, TypeError):
            return {}

    def get_tsol_sedes_set(self):
        """Retorna los códigos de sede TSOL permitidos como set."""
        if not self.tsol_sedes_permitidas:
            return set()
        return {s.strip() for s in self.tsol_sedes_permitidas.split(",") if s.strip()}

    def __str__(self):
        if self.nmEmpresa:
            return f"{self.nmEmpresa} ({self.name})"
        return f"Empresa {self.id}: {self.name}"

    class Meta:
        db_table = "conf_empresas"
        # managed = False
        verbose_name = _("Configuración Empresa")
        verbose_name_plural = _("Configuración Empresas")
        ordering = ["id", "nmEmpresa"]
        indexes = [
            models.Index(fields=["name"], name="empresa_name_idx"),
            models.Index(fields=["nmEmpresa"], name="empresa_nmempresa_idx"),
        ]


class ConfServer(models.Model):
    """
    Configuración de servidores para conexiones a bases de datos.
    """

    nbServer = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID del Servidor"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    nmServer = models.CharField(
        max_length=30,
        null=True,
        blank=True,
        verbose_name=_("Descripción del Servidor"),
        help_text=_("Nombre descriptivo del servidor"),
    )
    hostServer = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Host"),
        help_text=_("Dirección IP o hostname del servidor"),
    )
    portServer = models.CharField(
        max_length=10,
        null=True,
        blank=True,
        verbose_name=_("Puerto"),
        help_text=_("Puerto para la conexión al servidor"),
    )
    nbTipo = models.BigIntegerField(
        null=True,
        blank=True,
        verbose_name=_("Tipo"),
        help_text=_("Tipo de servidor según la tabla de configuración de tipos"),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Última actualización"),
        help_text=_("Fecha y hora de la última modificación"),
    )

    def get_tipo_servidor(self):
        """Obtiene el objeto tipo de servidor asociado."""
        if self.nbTipo:
            try:
                return ConfTipo.objects.get(nbTipo=self.nbTipo)
            except ConfTipo.DoesNotExist:
                return None
        return None

    def get_connection_string(self):
        """Genera una cadena de conexión para este servidor."""
        if not all([self.hostServer, self.portServer]):
            return None

        tipo = self.get_tipo_servidor()
        if tipo and hasattr(tipo, "nmUsr") and hasattr(tipo, "txPass"):
            return (
                f"servidor: {self.hostServer}:{self.portServer}, usuario: {tipo.nmUsr}"
            )
        return f"servidor: {self.hostServer}:{self.portServer}"

    def __str__(self):
        if self.nmServer:
            return f"{self.nmServer} ({self.hostServer}:{self.portServer})"
        return f"Servidor {self.nbServer}: {self.hostServer}:{self.portServer}"

    class Meta:
        db_table = "conf_server"
        # managed = False
        verbose_name = _("Configuración Servidor")
        verbose_name_plural = _("Configuración Servidores")
        ordering = ["nbServer"]
        indexes = [
            models.Index(fields=["hostServer"], name="server_host_idx"),
        ]


class ConfSql(models.Model):
    """
    Configuración de procesos SQL para la aplicación.
    """

    nbSql = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID del Proceso"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    txSql = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("SQL Script"),
        help_text=_("Consulta SQL a ejecutar"),
    )
    nmReporte = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Nombre del Proceso"),
        help_text=_("Nombre descriptivo del proceso SQL"),
    )
    txTabla = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Tabla de Inserción"),
        help_text=_("Tabla donde se insertarán los resultados"),
    )
    txDescripcion = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name=_("Descripción del Proceso"),
        help_text=_("Descripción detallada del propósito del proceso SQL"),
    )
    nmProcedure_out = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Nombre del Procedimiento Extractor"),
        help_text=_("Nombre del procedimiento almacenado para extracción"),
    )
    nmProcedure_in = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Nombre del Procedimiento Carga"),
        help_text=_("Nombre del procedimiento almacenado para carga"),
    )
    fecha_creacion = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_("Fecha de creación"),
        null=True,
        blank=True,
        help_text=_("Fecha y hora de creación del registro"),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Última actualización"),
        null=True,
        blank=True,
        help_text=_("Fecha y hora de la última modificación"),
    )

    def clean(self):
        """Validaciones personalizadas para el modelo."""
        if self.txSql:
            # Validar que no haya operaciones peligrosas
            dangerous_patterns = [
                r"\bDROP\s+(?:TABLE|DATABASE|SCHEMA)\b",
                r"\bDELETE\s+FROM\b(?!\s+WHERE)",
                r"\bTRUNCATE\s+TABLE\b",
            ]

            for pattern in dangerous_patterns:
                if re.search(pattern, self.txSql, re.IGNORECASE):
                    raise ValidationError(
                        {
                            "txSql": _(
                                "La consulta SQL contiene operaciones potencialmente peligrosas."
                            )
                        }
                    )

    def __str__(self):
        if self.txDescripcion and self.nmReporte:
            return f"{self.txDescripcion} ({self.nmReporte})"
        elif self.txDescripcion:
            return self.txDescripcion
        elif self.nmReporte:
            return self.nmReporte
        return f"SQL {self.nbSql}"

    class Meta:
        db_table = "conf_sql"
        # managed = False
        verbose_name = _("Configuración Proceso SQL")
        verbose_name_plural = _("Configuración Procesos SQL")
        ordering = ["nbSql"]
        indexes = [
            models.Index(fields=["nmReporte"], name="sql_nmreporte_idx"),
            models.Index(fields=["txDescripcion"], name="sql_descripcion_idx"),
        ]


class ConfTipo(models.Model):
    """
    Configuración de tipos de servidores con sus credenciales de acceso.
    """

    nbTipo = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    nmUsr = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        verbose_name=_("Usuario"),
        help_text=_("Nombre de usuario para la conexión"),
    )
    txPass = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Password"),
        help_text=_("Contraseña para la conexión"),
    )
    txDescripcion = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Descripción"),
        help_text=_("Descripción del tipo de servidor"),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Última actualización"),
        null=True,
        blank=True,
        help_text=_("Fecha y hora de la última modificación"),
    )

    def __str__(self):
        if hasattr(self, "txDescripcion") and self.txDescripcion:
            return f"{self.txDescripcion} (ID: {self.nbTipo})"
        return f"Tipo {self.nbTipo}"

    class Meta:
        db_table = "conf_tipo"
        # managed = False
        verbose_name = _("Configuración Tipo Servidor")
        verbose_name_plural = _("Configuración Tipos de Servidores")
        ordering = ["nbTipo"]


# ══════════════════════════════════════════════════════════════════
# Modelos CDT (Planos para proveedores como MasterFoods)
# ══════════════════════════════════════════════════════════════════


class ConfSqlCdt(models.Model):
    """
    Configuración de consultas SQL para la generación de planos CDT.
    Similar a ConfSql pero específico para el módulo CDT.
    """

    nbSql = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    nmReporte = models.CharField(
        max_length=100,
        verbose_name=_("Nombre del Reporte"),
        help_text=_("Nombre identificador del reporte CDT (ej: ventas_cdt, clientes_cdt)"),
    )
    txTabla = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Tabla Fuente"),
        help_text=_("Tabla de donde se extraen los datos (ej: cuboventas, clientes, inventario)"),
    )
    txDescripcion = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name=_("Descripción"),
        help_text=_("Descripción del propósito de esta consulta CDT"),
    )
    txSqlExtrae = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("SQL de Extracción"),
        help_text=_("Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin), :IdDs (empresa)"),
    )
    fecha_creacion = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    fecha_actualizacion = models.DateTimeField(auto_now=True, null=True, blank=True)

    def __str__(self):
        return f"{self.nbSql} - {self.nmReporte}"

    class Meta:
        db_table = "conf_sql_cdt"
        verbose_name = _("Config SQL CDT")
        verbose_name_plural = _("Config SQL CDT")
        ordering = ["nbSql"]
        indexes = [
            models.Index(fields=["nmReporte"], name="sqlcdt_nmreporte_idx"),
        ]


class CdtEnvio(models.Model):
    """
    Historial de envíos de planos CDT.
    Registra cada ejecución con sus resultados, archivos y estado.
    """

    class Estado(models.TextChoices):
        PENDIENTE = "pendiente", _("Pendiente")
        PROCESANDO = "procesando", _("Procesando")
        ENVIADO = "enviado", _("Enviado")
        ERROR = "error", _("Error")

    empresa = models.ForeignKey(
        ConfEmpresas,
        on_delete=models.CASCADE,
        verbose_name=_("Empresa"),
        related_name="envios_cdt",
    )
    fecha_inicio = models.DateField(verbose_name=_("Fecha Inicio Periodo"))
    fecha_fin = models.DateField(verbose_name=_("Fecha Fin Periodo"))
    estado = models.CharField(
        max_length=20,
        choices=Estado.choices,
        default=Estado.PENDIENTE,
        verbose_name=_("Estado"),
    )
    total_ventas = models.IntegerField(default=0, verbose_name=_("Total Ventas"))
    total_clientes = models.IntegerField(default=0, verbose_name=_("Total Clientes"))
    total_inventario = models.IntegerField(default=0, verbose_name=_("Total Inventario"))
    archivos_generados = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Archivos Generados"),
        help_text=_("JSON con lista de archivos generados y sus tamaños"),
    )
    archivo_descarga = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name=_("Archivo Descarga"),
        help_text=_("Ruta al archivo ZIP para descarga"),
    )
    enviado_sftp = models.BooleanField(
        default=False,
        verbose_name=_("Enviado por SFTP"),
    )
    log_ejecucion = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Log de Ejecución"),
    )
    usuario = models.ForeignKey(
        "users.User",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        verbose_name=_("Usuario"),
    )
    fecha_ejecucion = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_("Fecha de Ejecución"),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Última Actualización"),
    )

    def __str__(self):
        return f"CDT {self.empresa.name} ({self.fecha_inicio} a {self.fecha_fin}) [{self.estado}]"

    class Meta:
        db_table = "cdt_envio"
        verbose_name = _("Envío CDT")
        verbose_name_plural = _("Envíos CDT")
        ordering = ["-fecha_ejecucion"]
        indexes = [
            models.Index(fields=["estado"], name="cdtenvio_estado_idx"),
            models.Index(fields=["fecha_inicio", "fecha_fin"], name="cdtenvio_periodo_idx"),
        ]
        permissions = [
            ("ejecutar_cdt", _("Puede ejecutar generación de planos CDT")),
            ("reenviar_cdt", _("Puede re-enviar planos CDT por SFTP")),
        ]


# ══════════════════════════════════════════════════════════════════
# Modelos TSOL (Planos TrackSales para distribuidores)
# ══════════════════════════════════════════════════════════════════


class ConfSqlTsol(models.Model):
    """
    Configuración de consultas SQL para la generación de planos TSOL.
    Similar a ConfSqlCdt pero específico para el módulo TSOL (TrackSales).
    """

    nbSql = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    nmReporte = models.CharField(
        max_length=100,
        verbose_name=_("Nombre del Reporte"),
        help_text=_(
            "Identificador del reporte TSOL "
            "(ej: ventas_tsol, clientes_tsol, productos_tsol)"
        ),
    )
    txTabla = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Tabla Fuente"),
        help_text=_(
            "Tabla BI fuente (ej: cuboventas, clientes, productos, inventario)"
        ),
    )
    txDescripcion = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name=_("Descripción"),
        help_text=_("Descripción del propósito de esta consulta TSOL"),
    )
    txSqlExtrae = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("SQL de Extracción"),
        help_text=_(
            "Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin)"
        ),
    )
    fecha_creacion = models.DateTimeField(
        auto_now_add=True, null=True, blank=True
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True, null=True, blank=True
    )

    def __str__(self):
        return f"{self.nbSql} - {self.nmReporte}"

    class Meta:
        db_table = "conf_sql_tsol"
        verbose_name = _("Config SQL TSOL")
        verbose_name_plural = _("Config SQL TSOL")
        ordering = ["nbSql"]
        indexes = [
            models.Index(fields=["nmReporte"], name="sqltsol_nmreporte_idx"),
        ]


class TsolEnvio(models.Model):
    """
    Historial de envíos de planos TSOL.
    Registra cada ejecución con sus resultados, archivos y estado.
    """

    class Estado(models.TextChoices):
        PENDIENTE = "pendiente", _("Pendiente")
        PROCESANDO = "procesando", _("Procesando")
        ENVIADO = "enviado", _("Enviado")
        ERROR = "error", _("Error")

    empresa = models.ForeignKey(
        ConfEmpresas,
        on_delete=models.CASCADE,
        verbose_name=_("Empresa"),
        related_name="envios_tsol",
    )
    fecha_inicio = models.DateField(verbose_name=_("Fecha Inicio Periodo"))
    fecha_fin = models.DateField(verbose_name=_("Fecha Fin Periodo"))
    estado = models.CharField(
        max_length=20,
        choices=Estado.choices,
        default=Estado.PENDIENTE,
        verbose_name=_("Estado"),
    )
    total_ventas = models.IntegerField(
        default=0, verbose_name=_("Total Ventas")
    )
    total_clientes = models.IntegerField(
        default=0, verbose_name=_("Total Clientes")
    )
    total_productos = models.IntegerField(
        default=0, verbose_name=_("Total Productos")
    )
    total_vendedores = models.IntegerField(
        default=0, verbose_name=_("Total Vendedores")
    )
    total_inventario = models.IntegerField(
        default=0, verbose_name=_("Total Inventario")
    )
    archivos_generados = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Archivos Generados"),
        help_text=_("JSON con lista de archivos generados y sus tamaños"),
    )
    archivo_descarga = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name=_("Archivo Descarga"),
        help_text=_("Ruta al archivo ZIP para descarga"),
    )
    enviado_ftp = models.BooleanField(
        default=False,
        verbose_name=_("Enviado por FTP"),
    )
    log_ejecucion = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Log de Ejecución"),
    )
    usuario = models.ForeignKey(
        "users.User",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        verbose_name=_("Usuario"),
    )
    fecha_ejecucion = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_("Fecha de Ejecución"),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Última Actualización"),
    )

    def __str__(self):
        return (
            f"TSOL {self.empresa.name} "
            f"({self.fecha_inicio} a {self.fecha_fin}) [{self.estado}]"
        )

    class Meta:
        db_table = "tsol_envio"
        verbose_name = _("Envío TSOL")
        verbose_name_plural = _("Envíos TSOL")
        ordering = ["-fecha_ejecucion"]
        indexes = [
            models.Index(fields=["estado"], name="tsolenvio_estado_idx"),
            models.Index(
                fields=["fecha_inicio", "fecha_fin"],
                name="tsolenvio_periodo_idx",
            ),
        ]
        permissions = [
            ("ejecutar_tsol", _("Puede ejecutar generación de planos TSOL")),
            ("reenviar_tsol", _("Puede re-enviar planos TSOL por FTP")),
        ]


# ══════════════════════════════════════════════════════════════════
# Modelos Cosmos (Planos para envío FTPS a Cosmos)
# ══════════════════════════════════════════════════════════════════


class ConfSqlCosmos(models.Model):
    """
    Configuración de consultas SQL para la generación de planos Cosmos.
    Tabla existente en BD: conf_sql_cosmos.
    """

    nbSql = models.BigIntegerField(
        primary_key=True,
        verbose_name=_("ID"),
        validators=[MinValueValidator(1, _("El ID debe ser un número positivo"))],
    )
    nmReporte = models.CharField(
        max_length=100,
        verbose_name=_("Nombre del Reporte"),
        help_text=_("Nombre identificador del reporte Cosmos"),
    )
    txTabla = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name=_("Tabla Fuente"),
        help_text=_("Tabla de donde se extraen los datos"),
    )
    txDescripcion = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name=_("Descripción"),
        help_text=_("Descripción del propósito de esta consulta Cosmos"),
    )
    txSqlExtrae = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("SQL de Extracción"),
        help_text=_("Query SQL con parámetros :fi (fecha inicio), :ff (fecha fin), :IdDs (empresa)"),
    )
    fecha_creacion = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    fecha_actualizacion = models.DateTimeField(auto_now=True, null=True, blank=True)

    def __str__(self):
        return f"{self.nbSql} - {self.nmReporte}"

    class Meta:
        db_table = "conf_sql_cosmos"
        verbose_name = _("Config SQL Cosmos")
        verbose_name_plural = _("Config SQL Cosmos")
        ordering = ["nbSql"]
        indexes = [
            models.Index(fields=["nmReporte"], name="sqlcosmos_nmreporte_idx"),
        ]


class CosmosEnvio(models.Model):
    """
    Historial de envíos de planos Cosmos.
    Registra cada ejecución con sus resultados, archivos y estado.
    """

    class Estado(models.TextChoices):
        PENDIENTE = "pendiente", _("Pendiente")
        PROCESANDO = "procesando", _("Procesando")
        ENVIADO = "enviado", _("Enviado")
        ERROR = "error", _("Error")

    empresa = models.ForeignKey(
        ConfEmpresas,
        on_delete=models.CASCADE,
        verbose_name=_("Empresa"),
        related_name="envios_cosmos",
    )
    fecha_inicio = models.DateField(verbose_name=_("Fecha Inicio Periodo"))
    fecha_fin = models.DateField(verbose_name=_("Fecha Fin Periodo"))
    estado = models.CharField(
        max_length=20,
        choices=Estado.choices,
        default=Estado.PENDIENTE,
        verbose_name=_("Estado"),
    )
    total_registros = models.IntegerField(
        default=0, verbose_name=_("Total Registros")
    )
    archivos_generados = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Archivos Generados"),
        help_text=_("JSON con lista de archivos generados"),
    )
    archivo_descarga = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name=_("Archivo Descarga"),
        help_text=_("Ruta al archivo ZIP para descarga"),
    )
    enviado_ftps = models.BooleanField(
        default=False,
        verbose_name=_("Enviado por FTPS"),
    )
    log_ejecucion = models.TextField(
        null=True,
        blank=True,
        verbose_name=_("Log de Ejecución"),
    )
    usuario = models.ForeignKey(
        "users.User",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        verbose_name=_("Usuario"),
    )
    fecha_ejecucion = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_("Fecha de Ejecución"),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Última Actualización"),
    )

    def __str__(self):
        return (
            f"Cosmos {self.empresa.name} "
            f"({self.fecha_inicio} a {self.fecha_fin}) [{self.estado}]"
        )

    class Meta:
        db_table = "cosmos_envio"
        verbose_name = _("Envío Cosmos")
        verbose_name_plural = _("Envíos Cosmos")
        ordering = ["-fecha_ejecucion"]
        indexes = [
            models.Index(fields=["estado"], name="cosmosenvio_estado_idx"),
            models.Index(
                fields=["fecha_inicio", "fecha_fin"],
                name="cosmosenvio_periodo_idx",
            ),
        ]
        permissions = [
            ("ejecutar_cosmos", _("Puede ejecutar generación de planos Cosmos")),
            ("reenviar_cosmos", _("Puede re-enviar planos Cosmos por FTPS")),
        ]


# ══════════════════════════════════════════════════════════════════
# Programación de Tareas (RQ Scheduler)
# ══════════════════════════════════════════════════════════════════


class ProgramacionTarea(models.Model):
    """Configuración de horarios para tareas programadas (RQ Scheduler).
    Con empresa=NULL es global (ej: limpieza media).
    Con empresa=FK es per-empresa (CDT, TSOL, Cosmos, Email).
    """

    empresa = models.ForeignKey(
        ConfEmpresas, null=True, blank=True,
        on_delete=models.CASCADE, related_name="tareas_programadas",
        verbose_name=_("Empresa"),
        help_text=_("NULL = tarea global (ej: limpieza media)"),
    )
    nombre = models.CharField(
        max_length=100, verbose_name=_("Nombre"),
    )
    descripcion = models.CharField(
        max_length=255, blank=True, default="", verbose_name=_("Descripción"),
    )
    hora_local = models.TimeField(
        verbose_name=_("Hora (Colombia)"),
        help_text=_("Hora local Colombia (UTC-5) para ejecutar la tarea"),
    )
    activo = models.BooleanField(default=True, verbose_name=_("Activo"))
    func_path = models.CharField(
        max_length=255,
        verbose_name=_("Ruta de la función"),
        help_text=_("Ruta Python completa de la función a ejecutar"),
    )
    intervalo_segundos = models.IntegerField(
        default=86400,
        verbose_name=_("Intervalo (segundos)"),
        help_text=_("Intervalo entre ejecuciones. 86400=diario, 3600=cada hora"),
    )
    icono = models.CharField(max_length=50, default="fas fa-clock", blank=True)
    ultima_modificacion = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "programacion_tarea"
        verbose_name = _("Programación de Tarea")
        verbose_name_plural = _("Programación de Tareas")
        unique_together = [("empresa", "nombre")]
        ordering = ["empresa", "nombre"]

    def __str__(self):
        prefix = self.empresa.name if self.empresa else "Global"
        return f"[{prefix}] {self.nombre} ({self.hora_local})"

    @property
    def hora_utc(self):
        """Convierte hora local Colombia (UTC-5) a UTC."""
        from datetime import timedelta, datetime as dt_cls
        combined = dt_cls.combine(dt_cls.today(), self.hora_local)
        combined_utc = combined + timedelta(hours=5)
        return combined_utc.time()

    @property
    def frecuencia_label(self):
        """Etiqueta legible de la frecuencia."""
        if self.intervalo_segundos == 86400:
            return "Diaria"
        elif self.intervalo_segundos == 3600:
            return "Cada hora"
        elif self.intervalo_segundos == 43200:
            return "Cada 12 horas"
        else:
            horas = self.intervalo_segundos / 3600
            if horas >= 1:
                return f"Cada {int(horas)} horas"
            return f"Cada {self.intervalo_segundos // 60} min"
