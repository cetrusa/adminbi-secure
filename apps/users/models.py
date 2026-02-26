from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from apps.permisos.models import ConfEmpresas
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin
from .managers import UserManager


class UserPermission(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, verbose_name="Usuario"
    )
    empresa = models.ForeignKey(
        ConfEmpresas, on_delete=models.CASCADE, verbose_name="Empresa"
    )
    proveedores = models.TextField(
        blank=True, verbose_name="Proveedores"
    )  # Almacenar como texto
    macrozonas = models.TextField(blank=True, verbose_name="Macrozonas")

    class Meta:
        verbose_name = "Permiso de Usuario"
        verbose_name_plural = "Permisos de Usuario"

    def __str__(self):
        return f"{self.user.username} - {self.empresa.nmEmpresa}"


class User(AbstractBaseUser, PermissionsMixin):
    GENDER_CHOISES = (
        ("M", "Masculino"),
        ("F", "Femenino"),
        ("O", "Otros"),
    )
    username = models.CharField(max_length=10, unique=True, verbose_name="Usuario")
    email = models.EmailField(verbose_name="e-mail")
    nombres = models.CharField(max_length=50, verbose_name="Nombres")
    apellidos = models.CharField(max_length=50, verbose_name="Apellidos")
    genero = models.CharField(
        max_length=1, choices=GENDER_CHOISES, blank=True, verbose_name="Género"
    )
    codregistro = models.CharField(
        max_length=6, blank=True, verbose_name="Código de Registro"
    )
    conf_empresas = models.ManyToManyField(
        ConfEmpresas, blank=True, verbose_name="Bases de datos"
    )
    is_staff = models.BooleanField(default=False, verbose_name="Administrador")
    is_active = models.BooleanField(default=False, verbose_name="Activo")
    is_superuser = models.BooleanField(default=False, verbose_name="Superusuario")
    date_joined = models.DateTimeField(
        auto_now_add=True, verbose_name="Fecha de creación"
    )

    # Nuevos campos para 2FA
    totp_secret = models.CharField(
        _("TOTP Secret"),
        max_length=255,
        blank=True,
        null=True,
        help_text=_("Clave secreta para la autenticación de dos factores"),
    )
    two_factor_enabled = models.BooleanField(
        _("2FA Habilitado"),
        default=False,
        help_text=_("Indica si la autenticación de dos factores está habilitada"),
    )
    last_login_ip = models.GenericIPAddressField(
        _("Última IP de inicio de sesión"), blank=True, null=True
    )
    session_security = models.JSONField(
        _("Seguridad de sesión"),
        default=dict,
        blank=True,
        help_text=_("Datos de seguridad de sesión como dispositivos conocidos"),
    )

    USERNAME_FIELD = "username"
    REQUIRED_FIELDS = ["email"]

    objects = UserManager()

    def get_short_name(self):
        return self.username

    def get_full_name(self):
        return self.nombres + " " + self.apellidos

    def __str__(self):
        return (
            self.username
            + " - "
            + self.nombres
            + " - "
            + self.apellidos
            + " - "
            + self.email
        )

    @classmethod
    def cod_validation(cls, id_user, codigo):
        try:
            user = cls.objects.get(id=id_user, codregistro=codigo)
            return True
        except cls.DoesNotExist:
            return False

    def get_empresas_nombres(self):
        """Devuelve una lista de nombres de empresas asociadas al usuario."""
        return ", ".join([e.nmEmpresa for e in self.conf_empresas.all()])

    get_empresas_nombres.short_description = "Empresas asociadas"


class UserProfile(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    conf_empresas = models.ManyToManyField(ConfEmpresas)


class RegistroAuditoria(models.Model):
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    fecha_hora = models.DateTimeField(auto_now_add=True)
    ip = models.GenericIPAddressField()
    transaccion = models.CharField(max_length=255)
    detalle = models.JSONField()
    database_name = models.CharField(max_length=100, null=True, blank=True)
    city = models.CharField(max_length=100, null=True, blank=True)

    def __str__(self):
        return (
            self.fecha_hora.strftime("%Y-%m-%d %H:%M:%S")
            + " - "
            + self.database_name
            + " - "
            + self.usuario.get_full_name()
            + " - "
            + (str(self.city) if self.city else "")
        )

    class Meta:
        db_table = "registro_auditoria"
        verbose_name = "Registro de Auditoria"
        verbose_name_plural = "Registro de Auditorias"
