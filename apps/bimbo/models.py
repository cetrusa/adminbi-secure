from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _


class AgenciaBimbo(models.Model):
    """
    Modelo Django (unmanaged) que refleja powerbi_bimbo.agencias_bimbo.
    La tabla real se gestiona por SQL directo; este modelo permite usarla
    en Django Admin y en queries ORM cuando hay un database router configurado.
    """

    ESTADO_CHOICES = [
        ("PENDIENTE", _("Pendiente")),
        ("ACTIVO", _("Activo")),
        ("INACTIVO", _("Inactivo")),
    ]

    id = models.AutoField(primary_key=True)
    id_agente = models.IntegerField(
        unique=True,
        verbose_name=_("ID Agente SIDIS"),
        help_text=_("FK lógica a conf_empresas.id"),
    )
    Nombre = models.CharField(
        max_length=255,
        verbose_name=_("Nombre Agencia"),
    )
    db_powerbi = models.CharField(
        max_length=150,
        blank=True,
        null=True,
        verbose_name=_("BD PowerBI"),
        help_text=_("Nombre de la base de datos PowerBI del agente (de conf_empresas.name)"),
    )
    nbOficinaV = models.CharField(
        max_length=10,
        default="1001",
        verbose_name=_("Oficina Ventas"),
    )
    SIDIS = models.CharField(
        max_length=10,
        default="SI",
        verbose_name=_("SIDIS"),
    )
    nmOficinaV = models.CharField(
        max_length=50,
        default="BIMBO",
        verbose_name=_("Nombre Oficina"),
    )
    CEVE = models.IntegerField(
        default=0,
        verbose_name=_("Código CEVE"),
        help_text=_("Código CEVE asignado por BIMBO"),
    )
    estado = models.CharField(
        max_length=10,
        choices=ESTADO_CHOICES,
        default="PENDIENTE",
        verbose_name=_("Estado"),
    )
    es_bimbo = models.BooleanField(
        default=True,
        verbose_name=_("Es BIMBO"),
        help_text=_("Marca si la agencia opera con BIMBO"),
    )
    id_proveedor_bimbo = models.CharField(
        max_length=10,
        blank=True,
        null=True,
        verbose_name=_("ID Proveedor BIMBO"),
        help_text=_("idProveedor BIMBO en el SIDIS de esta agencia (resultado de Discovery)"),
    )
    id_proveedor_fvp = models.CharField(
        max_length=10,
        blank=True,
        null=True,
        verbose_name=_("ID Proveedor FVP"),
        help_text=_("idProveedor FVP en el SIDIS de esta agencia (resultado de Discovery)"),
    )
    fecha_alta = models.DateTimeField(
        blank=True,
        null=True,
        verbose_name=_("Fecha Alta"),
    )
    fecha_ultimo_snapshot = models.DateTimeField(
        blank=True,
        null=True,
        verbose_name=_("Último Snapshot"),
    )

    class Meta:
        managed = False
        db_table = "agencias_bimbo"
        verbose_name = _("Agencia BIMBO")
        verbose_name_plural = _("Agencias BIMBO")
        ordering = ["CEVE"]

    def __str__(self):
        return f"{self.CEVE} — {self.Nombre}"


class PermisoBimboAgente(models.Model):
    """
    Asigna agencias BIMBO visibles a cada usuario.
    Tabla managed por Django (vive en la BD default: powerbi_adm).

    NOTA: AgenciaBimbo vive en otra BD (powerbi_bimbo), por lo que no se puede
    usar un ForeignKey real. Se guarda agencia_id (IntegerField) y se resuelve
    el objeto via la propiedad `agencia`.
    """

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="permisos_bimbo",
        verbose_name=_("Usuario"),
    )
    # FK lógica — sin constraint en DB porque AgenciaBimbo está en otra BD
    agencia_id = models.IntegerField(
        verbose_name=_("ID Agencia"),
        help_text=_("FK lógica a AgenciaBimbo.id (powerbi_bimbo.agencias_bimbo)"),
    )
    puede_ejecutar = models.BooleanField(
        default=False,
        verbose_name=_("Puede Ejecutar"),
        help_text=_("Permite ejecutar Discovery, Snapshot y Homologación"),
    )
    puede_editar = models.BooleanField(
        default=False,
        verbose_name=_("Puede Editar"),
        help_text=_("Permite hacer match manual en equivalencias"),
    )

    class Meta:
        managed = True
        db_table = "bimbo_permiso_agente"
        unique_together = ("user", "agencia_id")
        verbose_name = _("Permiso Agente BIMBO")
        verbose_name_plural = _("Permisos Agentes BIMBO")

    @property
    def agencia(self):
        """Resuelve el objeto AgenciaBimbo desde la BD 'bimbo'."""
        return AgenciaBimbo.objects.using("bimbo").filter(pk=self.agencia_id).first()

    def __str__(self):
        return f"{self.user.username} → agencia_id={self.agencia_id}"
