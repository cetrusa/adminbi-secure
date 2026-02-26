from django.db import models
from django.contrib.auth.models import BaseUserManager
from django.core.exceptions import ValidationError
import re


class UserManager(BaseUserManager, models.Manager):
    """
    Manager personalizado para el modelo User que proporciona métodos
    para crear usuarios y superusuarios, así como validar códigos de registro.
    """

    def _create_user(
        self,
        username,
        email,
        password,
        is_staff,
        is_superuser,
        is_active,
        **extra_fields
    ):
        """
        Método base para crear usuarios.
        Normaliza el email y valida que no esté vacío.
        """
        if not username:
            raise ValueError("El nombre de usuario es obligatorio")
        if not email:
            raise ValueError("El email es obligatorio")

        # Validación básica de email
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(email_pattern, email):
            raise ValidationError("El formato del email no es válido")

        email = self.normalize_email(email)
        user = self.model(
            username=username,
            email=email,
            is_staff=is_staff,
            is_superuser=is_superuser,
            is_active=is_active,
            **extra_fields
        )
        user.set_password(password)
        user.save(using=self.db)
        return user

    def create_user(self, username, email, password=None, **extra_fields):
        """
        Crea y guarda un usuario con los campos proporcionados.
        Por defecto: is_staff=False, is_superuser=False, is_active=False
        """
        return self._create_user(
            username, email, password, False, False, False, **extra_fields
        )

    def create_superuser(self, username, email, password=None, **extra_fields):
        """
        Crea y guarda un superusuario con los campos proporcionados.
        Por defecto: is_staff=True, is_superuser=True, is_active=True
        """
        return self._create_user(
            username, email, password, True, True, True, **extra_fields
        )

    def cod_validation(self, id_user, cod_registro):
        """
        Valida si existe un usuario con el ID y código de registro dados.
        Se optimiza usando get() dentro de un try/except como en el modelo.
        """
        try:
            self.get(id=id_user, codregistro=cod_registro)
            return True
        except self.model.DoesNotExist:
            return False

    def activate_user(self, id_user):
        """
        Activa la cuenta de un usuario por su ID.
        Retorna True si se activó correctamente, False si no existe.
        """
        try:
            user = self.get(id=id_user)
            user.is_active = True
            user.codregistro = ""  # Opcional: limpiar el código una vez activado
            user.save(update_fields=["is_active", "codregistro"])
            return True
        except self.model.DoesNotExist:
            return False
