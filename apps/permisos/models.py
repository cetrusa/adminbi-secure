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
            ("config_email_reportes", _("Configurar correos para reportes programados")),
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
