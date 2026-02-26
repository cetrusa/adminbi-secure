from django import forms
from django.contrib.auth import authenticate
from django.shortcuts import get_object_or_404
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

from captcha.fields import ReCaptchaField
from captcha.widgets import ReCaptchaV2Checkbox

from .models import User


class UserRegisterForm(forms.ModelForm):
    """
    Formulario para el registro de nuevos usuarios.
    Incluye campos para contraseña y su confirmación.
    """

    password1 = forms.CharField(
        label=_("Contraseña"),
        required=True,
        widget=forms.PasswordInput(attrs={"placeholder": _("Contraseña")}),
        help_text=_(
            "La contraseña debe tener al menos 8 caracteres y contener letras y números."
        ),
    )
    password2 = forms.CharField(
        label=_("Confirmar Contraseña"),
        required=True,
        widget=forms.PasswordInput(attrs={"placeholder": _("Repetir Contraseña")}),
    )

    class Meta:
        """Meta definition for Userform."""

        model = User
        fields = (
            "username",
            "email",
            "nombres",
            "apellidos",
            "genero",
        )

        widgets = {
            "username": forms.TextInput(attrs={"placeholder": _("Nombre de usuario")}),
            "email": forms.EmailInput(attrs={"placeholder": _("Correo electrónico")}),
            "nombres": forms.TextInput(attrs={"placeholder": _("Nombres")}),
            "apellidos": forms.TextInput(attrs={"placeholder": _("Apellidos")}),
        }

        help_texts = {
            "username": _("Nombre de usuario único, máximo 10 caracteres."),
            "email": _("Introduce un correo electrónico válido."),
        }

    def clean_password1(self):
        """Valida que la contraseña cumpla con requisitos mínimos de seguridad."""
        password = self.cleaned_data.get("password1")
        if password:
            # Verifica longitud mínima
            if len(password) < 8:
                raise ValidationError(
                    _("La contraseña debe tener al menos 8 caracteres.")
                )

            # Verifica que tenga letras
            if not any(char.isalpha() for char in password):
                raise ValidationError(
                    _("La contraseña debe contener al menos una letra.")
                )

            # Verifica que tenga números
            if not any(char.isdigit() for char in password):
                raise ValidationError(
                    _("La contraseña debe contener al menos un número.")
                )

        return password

    def clean_password2(self):
        """Verifica que password1 y password2 coincidan."""
        password1 = self.cleaned_data.get("password1")
        password2 = self.cleaned_data.get("password2")

        if password1 and password2 and password1 != password2:
            raise ValidationError(_("Las contraseñas no coinciden."))
        return password2

    def clean_email(self):
        """Verifica que el email no esté ya registrado."""
        email = self.cleaned_data.get("email")
        if User.objects.filter(email=email).exists():
            raise ValidationError(_("Este correo electrónico ya está registrado."))
        return email

    def save(self, commit=True):
        """Guarda el usuario con la contraseña encriptada."""
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password1"])
        if commit:
            user.save()
        return user


class LoginForm(forms.Form):
    """Formulario para inicio de sesión de usuarios."""

    username = forms.CharField(
        label=_("Usuario"),
        required=True,
        widget=forms.TextInput(
            attrs={
                "placeholder": _("Usuario"),
                "autocomplete": "username",
            }
        ),
    )
    password = forms.CharField(
        label=_("Contraseña"),
        required=True,
        widget=forms.PasswordInput(
            attrs={
                "placeholder": _("Contraseña"),
                "autocomplete": "current-password",
            }
        ),
    )

    captcha = ReCaptchaField(widget=ReCaptchaV2Checkbox)

    def clean(self):
        """Valida las credenciales del usuario."""
        cleaned_data = super().clean()
        username = cleaned_data.get("username")
        password = cleaned_data.get("password")

        if username and password:
            if not authenticate(username=username, password=password):
                raise ValidationError(_("Los datos de usuario no son correctos."))

        return cleaned_data


class UpdatePasswordForm(forms.Form):
    """Formulario para actualizar la contraseña del usuario."""

    password_current = forms.CharField(
        label=_("Contraseña Actual"),
        required=True,
        widget=forms.PasswordInput(
            attrs={
                "placeholder": _("Contraseña Actual"),
                "autocomplete": "current-password",
            }
        ),
    )
    password_new = forms.CharField(
        label=_("Contraseña Nueva"),
        required=True,
        widget=forms.PasswordInput(
            attrs={
                "placeholder": _("Contraseña Nueva"),
                "autocomplete": "new-password",
            }
        ),
        help_text=_(
            "La contraseña debe tener al menos 8 caracteres y contener letras y números."
        ),
    )
    password_confirm = forms.CharField(
        label=_("Confirmar Contraseña"),
        required=True,
        widget=forms.PasswordInput(
            attrs={
                "placeholder": _("Confirmar Contraseña Nueva"),
                "autocomplete": "new-password",
            }
        ),
    )

    def __init__(self, user, *args, **kwargs):
        self.user = user
        super().__init__(*args, **kwargs)

    def clean_password_current(self):
        """Verifica que la contraseña actual sea correcta."""
        password = self.cleaned_data.get("password_current")
        if not self.user.check_password(password):
            raise ValidationError(_("La contraseña actual es incorrecta."))
        return password

    def clean_password_new(self):
        """Valida requisitos de la nueva contraseña."""
        password = self.cleaned_data.get("password_new")
        if password:
            if len(password) < 8:
                raise ValidationError(
                    _("La contraseña debe tener al menos 8 caracteres.")
                )
            if not any(char.isalpha() for char in password):
                raise ValidationError(
                    _("La contraseña debe contener al menos una letra.")
                )
            if not any(char.isdigit() for char in password):
                raise ValidationError(
                    _("La contraseña debe contener al menos un número.")
                )
        return password

    def clean_password_confirm(self):
        """Verifica que las nuevas contraseñas coincidan."""
        password_new = self.cleaned_data.get("password_new")
        password_confirm = self.cleaned_data.get("password_confirm")

        if password_new and password_confirm and password_new != password_confirm:
            raise ValidationError(_("Las contraseñas nuevas no coinciden."))
        return password_confirm


class VerificationForm(forms.Form):
    """Formulario para verificación de código de registro."""

    codregistro = forms.CharField(
        label=_("Código de Verificación"),
        required=True,
        widget=forms.TextInput(
            attrs={
                "placeholder": _("Ingrese el código de 6 dígitos"),
                "maxlength": "6",
                "minlength": "6",
            }
        ),
        help_text=_("Ingrese el código de 6 dígitos enviado a su correo."),
    )

    def __init__(self, pk, *args, **kwargs):
        self.id_user = pk
        super().__init__(*args, **kwargs)

    def clean_codregistro(self):
        """Valida que el código de registro sea correcto."""
        codigo = self.cleaned_data.get("codregistro")

        if not codigo:
            raise ValidationError(_("Debe ingresar un código de verificación."))

        if len(codigo) != 6:
            raise ValidationError(_("El código debe tener exactamente 6 caracteres."))

        try:
            user = User.objects.get(id=self.id_user)
            if user.codregistro != codigo:
                raise ValidationError(_("El código de verificación es incorrecto."))
        except User.DoesNotExist:
            raise ValidationError(_("Usuario no encontrado."))

        return codigo


class UserVerificationForm(forms.Form):
    """Formulario para verificación de código de autenticación de dos factores."""

    verification_code = forms.CharField(
        label=_("Código de Verificación"),
        required=True,
        widget=forms.TextInput(
            attrs={
                "placeholder": _("Ingrese el código de 6 dígitos"),
                "maxlength": "6",
                "minlength": "6",
                "autocomplete": "one-time-code",
            }
        ),
        help_text=_(
            "Ingrese el código de 6 dígitos de su aplicación de autenticación."
        ),
    )

    def clean_verification_code(self):
        """Valida el formato del código de verificación."""
        code = self.cleaned_data.get("verification_code")

        if not code:
            raise ValidationError(_("Debe ingresar un código de verificación."))

        if not code.isdigit():
            raise ValidationError(_("El código debe contener solo dígitos."))

        if len(code) != 6:
            raise ValidationError(_("El código debe tener exactamente 6 dígitos."))

        return code
