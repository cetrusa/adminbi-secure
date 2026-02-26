from django.shortcuts import render, get_object_or_404, redirect
from django.core.mail import send_mail
from django.urls import reverse_lazy, reverse
from django.contrib.auth import authenticate, login, logout, update_session_auth_hash
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required, permission_required
from django.utils.decorators import method_decorator
from django.contrib.auth import get_user_model
from django.contrib import messages
from django.http import HttpResponseRedirect, JsonResponse
from django.utils.translation import gettext_lazy as _
from django.views.generic import (
    View,
    CreateView,
    ListView,
    TemplateView,
    DetailView,
    DeleteView,
)
from django.views.generic.edit import FormView, UpdateView
from django.db import transaction
from django.conf import settings
from django.core.cache import cache
import logging

from .utils import code_generator
from .forms import (
    UserRegisterForm,
    LoginForm,
    UpdatePasswordForm,
    VerificationForm,
    UserVerificationForm,
)
from .models import User
from apps.permisos.models import ConfEmpresas

import json
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.cache import never_cache
from django.contrib.auth.forms import AuthenticationForm

from .utils import (
    generate_totp_secret,
    generate_totp_uri,
    generate_qr_code,
    verify_totp,
)

# Configuración global
User = get_user_model()
logger = logging.getLogger(__name__)


class UserVerificationView(View):
    """
    Vista para verificar el código de activación de un usuario.
    """

    template_name = "users/verification.html"

    def get(self, request, *args, **kwargs):
        return render(request, self.template_name, {"user_id": kwargs["pk"]})

    def post(self, request, *args, **kwargs):
        user = get_object_or_404(User, pk=kwargs["pk"])
        codigo = request.POST.get("codigo")

        if user.codregistro == codigo:
            user.is_active = True
            user.codregistro = ""  # Limpiamos el código de verificación
            user.save()
            messages.success(
                request, _("Cuenta verificada correctamente. Ya puede iniciar sesión.")
            )
            return HttpResponseRedirect(reverse("users_app:user-login"))
        else:
            messages.error(
                request, _("Código de verificación incorrecto. Inténtelo nuevamente.")
            )
            return render(
                request,
                self.template_name,
                {"error": "Código incorrecto", "user_id": kwargs["pk"]},
            )


class UserRegisterView(FormView):
    """
    Vista para el registro de nuevos usuarios con envío de código de verificación.
    """

    template_name = "users/register.html"
    form_class = UserRegisterForm
    success_url = reverse_lazy("users_app:user-login")

    def form_valid(self, form):
        # Ejecutamos la creación del usuario en una transacción para garantizar la integridad
        with transaction.atomic():
            # Generar el código de verificación
            codigo = code_generator()

            # Crear el usuario en la base de datos
            user = User.objects.create_user(
                form.cleaned_data["username"],
                form.cleaned_data["email"],
                form.cleaned_data["password1"],
                nombres=form.cleaned_data["nombres"],
                apellidos=form.cleaned_data["apellidos"],
                genero=form.cleaned_data["genero"],
                codregistro=codigo,
            )

            # Preparar el envío del correo electrónico
            asunto = _("Confirmación de email - DataZenith BI")
            mensaje = _(
                f"Hola {user.nombres},\n\nGracias por registrarte en DataZenith BI. "
                f"Para activar tu cuenta, utiliza el siguiente código de verificación:\n\n"
                f"{codigo}\n\n"
                f"Si no has solicitado este registro, puedes ignorar este mensaje.\n\n"
                f"Saludos,\nEquipo DataZenith BI"
            )

            email_remitente = getattr(
                settings, "DEFAULT_FROM_EMAIL", "torredecontrolamovil@gmail.com"
            )

            try:
                # Enviar el correo electrónico
                send_mail(
                    asunto, mensaje, email_remitente, [form.cleaned_data["email"]]
                )
                messages.success(
                    self.request,
                    _(
                        "Se ha enviado un código de verificación a su correo electrónico."
                    ),
                )
            except Exception as e:
                # Registrar el error y mostrar un mensaje al usuario
                messages.warning(
                    self.request,
                    _(
                        "No se pudo enviar el correo de verificación. El administrador ha sido notificado."
                    ),
                )
                print(f"Error al enviar correo: {e}")

        # Redirigir a la pantalla de validación
        return HttpResponseRedirect(
            reverse("users_app:user-verification", kwargs={"pk": user.id})
        )


class LoginUser(FormView):
    """
    Vista para el inicio de sesión de usuarios.
    """

    template_name = "users/login.html"
    form_class = LoginForm
    success_url = reverse_lazy("home_app:panel_cubo")

    @method_decorator(csrf_protect)
    @method_decorator(never_cache)
    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            return HttpResponseRedirect(self.get_success_url())
        return super(LoginUser, self).dispatch(request, *args, **kwargs)

    def form_valid(self, form):
        user = authenticate(
            username=form.cleaned_data["username"],
            password=form.cleaned_data["password"],
        )

        if user is not None:
            # Verifica si el usuario tiene 2FA habilitado
            if user.two_factor_enabled:
                # Almacenar temporalmente los datos de autenticación en la sesión
                self.request.session["pre_auth_user_id"] = user.id
                self.request.session["pre_auth_remember"] = form.cleaned_data.get(
                    "remember", False
                )
                return redirect("users_app:two_factor_verify")

            # Registro de la dirección IP
            user.last_login_ip = self.request.META.get("REMOTE_ADDR", "")
            user.save()

            login(self.request, user)

            # Configuración del tiempo de expiración de la sesión según checkbox "Recordarme"
            if form.cleaned_data.get("remember", False):
                # Configurar sesión para que expire en 30 días
                self.request.session.set_expiry(60 * 60 * 24 * 30)
            else:
                # Usar el tiempo por defecto (cierra al cerrar navegador)
                self.request.session.set_expiry(0)

            return super(LoginUser, self).form_valid(form)
        else:
            messages.error(
                self.request,
                _("No se pudo iniciar sesión con las credenciales proporcionadas."),
            )
            return self.form_invalid(form)


class LogoutView(View):
    """
    Vista para cerrar la sesión de un usuario.
    """

    def get(self, request, *args, **kwargs):
        logout(request)
        messages.info(request, _("Ha cerrado sesión correctamente."))
        return HttpResponseRedirect(reverse("users_app:user-login"))


class CodeVerificationView(FormView):
    """
    Vista para verificar el código de activación de un usuario.
    """

    template_name = "users/verification.html"
    form_class = VerificationForm
    success_url = reverse_lazy("users_app:user-login")

    def get_form_kwargs(self):
        kwargs = super(CodeVerificationView, self).get_form_kwargs()
        kwargs.update({"pk": self.kwargs["pk"]})
        return kwargs

    def form_valid(self, form):
        with transaction.atomic():
            user = get_object_or_404(User, id=self.kwargs["pk"])
            user.is_active = True
            user.codregistro = ""  # Limpiar el código de verificación
            user.save()

            # Verificar si el usuario está activo
            if user.is_active:
                # Agregar un mensaje flash
                messages.success(
                    self.request,
                    _(
                        "Señor usuario ha confirmado correctamente, debe esperar que sea asignada la empresa"
                    ),
                )
                # Redirigir al inicio de sesión con un mensaje
                return HttpResponseRedirect(reverse("users_app:user-login"))
            else:
                messages.error(
                    self.request,
                    _(
                        "Hubo un error al activar su cuenta. Por favor, intente nuevamente."
                    ),
                )
                return super().form_invalid(form)


class BaseView(LoginRequiredMixin, TemplateView):
    """
    Vista base que incluye la lista de bases de datos disponibles para el usuario.
    Proporciona la funcionalidad para listar, seleccionar y cambiar entre bases de datos.
    """

    template_name = "base.html"
    login_url = reverse_lazy("users_app:user-login")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Obtener empresas asignadas al usuario
        try:
            # Obtenemos las bases de datos con caché para mejorar rendimiento
            cache_key = f"user_databases_{self.request.user.id}"
            databases = cache.get(cache_key)

            if not databases:
                databases = (
                    self.request.user.conf_empresas.all()
                    .select_related()
                    .order_by("nmEmpresa")
                )
                # Guardamos en caché por 5 minutos (ajustar según necesidad)
                cache.set(cache_key, databases, 300)

            # Crear lista para la plantilla (como tuplas para el selector)
            databases_list = [
                (database.name, database.nmEmpresa) for database in databases
            ]

            # Crear lista para operaciones internas (como diccionarios)
            database_dict_list = [
                {
                    "name": database.name,
                    "nmEmpresa": database.nmEmpresa,
                    "id": database.id,
                }
                for database in databases
            ]

            # Ordenar las bases de datos por nombre de empresa
            sorted_database_list = sorted(
                database_dict_list, key=lambda x: x["nmEmpresa"]
            )

            # Obtener la base de datos seleccionada de la sesión o el POST
            database_name = self.request.session.get(
                "database_name"
            ) or self.request.POST.get("database_select")

            # Guardar la base de datos original para verificar si cambió
            original_database_name = database_name

            # Verificar si la base de datos seleccionada está en la lista
            if database_name and sorted_database_list:
                db_exists = any(
                    db["name"] == database_name for db in sorted_database_list
                )

                if not db_exists:
                    # Si la BD seleccionada no está en la lista, seleccionar la primera disponible
                    database_name = sorted_database_list[0]["name"]
                    self.request.session["database_name"] = database_name
                    self.request.session.modified = True

                    # Log del cambio para seguimiento
                    logger.warning(
                        f"Se cambió automáticamente la base de datos de {original_database_name} a {database_name} "
                        f"porque la original no estaba disponible para el usuario {self.request.user.username}"
                    )

            # Si aún no hay una base de datos seleccionada y hay opciones disponibles, seleccionar la primera
            elif not database_name and sorted_database_list:
                database_name = sorted_database_list[0]["name"]
                self.request.session["database_name"] = database_name
                self.request.session.modified = True

                # Log para seguimiento
                logger.info(
                    f"Primera selección automática de base de datos {database_name} para el usuario {self.request.user.username}"
                )

            # Actualizar StaticPage.name para mantener consistencia global
            if database_name:
                try:
                    from scripts.StaticPage import StaticPage

                    StaticPage.name = database_name
                except ImportError:
                    logger.warning(
                        "No se pudo importar StaticPage para actualizar el nombre de la base de datos"
                    )

            # Pasar las variables de contexto a la plantilla
            context["databases_list"] = (
                databases_list  # Lista de tuplas para la plantilla
            )
            context["database_list"] = (
                sorted_database_list  # Lista de diccionarios para compatibilidad
            )
            context["database_name"] = database_name
            context["database_name_session"] = (
                database_name  # Para el selector en la plantilla
            )
            context["form_url"] = (
                self.get_form_url()
            )  # URL para el formulario de selección

            # Agregar información adicional de la base de datos seleccionada
            if database_name:
                try:
                    # Intentar obtener de la lista filtrada primero (más eficiente)
                    selected_db_info = next(
                        (
                            db
                            for db in sorted_database_list
                            if db["name"] == database_name
                        ),
                        None,
                    )

                    if selected_db_info:
                        context["selected_database"] = selected_db_info
                    else:
                        # Si no está en la lista, intentar obtenerlo de la base de datos
                        selected_db = ConfEmpresas.objects.get(name=database_name)
                        context["selected_database"] = {
                            "name": selected_db.name,
                            "nmEmpresa": selected_db.nmEmpresa,
                            "id": selected_db.id,
                        }
                except (ConfEmpresas.DoesNotExist, StopIteration) as e:
                    logger.error(
                        f"Error al obtener información de la base de datos seleccionada: {e}"
                    )
                    # No hacemos nada, simplemente no tendrá la información adicional

        except Exception as e:
            # Manejo de errores robusto
            logger.exception(
                f"Error al obtener bases de datos para el usuario {self.request.user.username}: {e}"
            )
            context["databases_list"] = []
            context["database_list"] = []
            context["database_name"] = None
            context["database_name_session"] = None
            context["form_url"] = self.get_form_url()

            messages.error(
                self.request,
                _(
                    "Error al cargar las bases de datos disponibles. Por favor, intente nuevamente."
                ),
            )

        return context

    def get_form_url(self):
        """
        Determina la URL a la que se enviará el formulario de selección de base de datos.
        Por defecto usa la URL actual.
        """
        return self.request.path

    def post(self, request, *args, **kwargs):
        """
        Maneja la selección de una base de datos.
        Actualiza la sesión y responde con un JSON o redirecciona según la petición.
        """
        database_name = request.POST.get("database_select")

        if not database_name:
            return JsonResponse(
                {
                    "status": "error",
                    "message": _("Nombre de base de datos no proporcionado."),
                },
                status=400,
            )

        try:
            # Verificar que la base de datos exista y esté asignada al usuario
            db_exists = request.user.conf_empresas.filter(name=database_name).exists()

            if not db_exists:
                return JsonResponse(
                    {
                        "status": "error",
                        "message": _("No tiene acceso a esta base de datos."),
                    },
                    status=403,
                )

            # Guardar en la sesión el nombre de la base de datos seleccionada
            request.session["database_name"] = database_name
            request.session.modified = True
            request.session.save()  # Forzar guardado de la sesión

            # Actualizar StaticPage.name para mantener consistencia
            try:
                from scripts.StaticPage import StaticPage

                StaticPage.name = database_name
            except ImportError:
                logger.warning(
                    "No se pudo importar StaticPage para actualizar el nombre de la base de datos"
                )

            # Invalidar los cachés relacionados
            self.invalidate_related_caches(request)

            # Determinar si la petición espera JSON o redirección
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "status": "success",
                        "message": _("Base de datos actualizada correctamente."),
                        "database_name": database_name,
                    }
                )
            else:
                # Redirección para peticiones normales
                messages.success(
                    request,
                    _("Base de datos cambiada a: %(name)s") % {"name": database_name},
                )
                return HttpResponseRedirect(request.path)

        except Exception as e:
            logger.exception(f"Error al actualizar base de datos: {e}")

            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "status": "error",
                        "message": _("Error al procesar la solicitud."),
                    },
                    status=500,
                )
            else:
                messages.error(
                    request, _("Ocurrió un error al cambiar la base de datos.")
                )
                return HttpResponseRedirect(request.path)

    def invalidate_related_caches(self, request):
        """
        Invalida las cachés relacionadas con la base de datos seleccionada.
        """
        try:
            from django.core.cache import cache

            # Invalidar caché de bases de datos del usuario
            cache_key = f"user_databases_{request.user.id}"
            cache.delete(cache_key)

            # Invalidar otras cachés relacionadas
            cache_keys = [
                f"panel_cubo_{request.user.id}",
                f"panel_bi_{request.user.id}",
                f"panel_actualizacion_{request.user.id}",
                f"panel_interface_{request.user.id}",
            ]
            for key in cache_keys:
                cache.delete(key)

        except Exception as e:
            logger.error(f"Error al invalidar cachés: {e}")


class DatabaseListView(ListView):
    model = ConfEmpresas
    context_object_name = "databases"
    template_name = "includes/database_list.html"

    def get_queryset(self):
        return self.request.user.conf_empresas.all().order_by("nmEmpresa")


@login_required(login_url=reverse_lazy("users_app:user-login"))
def database_list(request):
    """
    Vista para obtener la lista de bases de datos en formato JSON.
    """
    try:
        databases = request.user.conf_empresas.all().order_by("nmEmpresa")
        databases_list = [
            {
                "database_name": database.name,
                "database_nmEmpresa": database.nmEmpresa,
            }
            for database in databases
        ]
        return JsonResponse({"status": "success", "databases_list": databases_list})
    except Exception as e:
        print(f"Error en database_list: {e}")
        return JsonResponse(
            {
                "status": "error",
                "message": _("Error al obtener la lista de bases de datos."),
            },
            status=500,
        )


@login_required(login_url=reverse_lazy("users_app:user-login"))
def home(request):
    """
    Vista de la página de inicio.
    """
    return render(request, "index.html")


class TwoFactorSetupView(LoginRequiredMixin, TemplateView):
    """
    Vista para configurar la autenticación de dos factores.
    """

    template_name = "users/two_factor_setup.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Generar una nueva clave secreta TOTP para el usuario
        secret = generate_totp_secret()
        self.request.session["temp_totp_secret"] = secret

        # Generar URI para el código QR
        uri = generate_totp_uri(secret, self.request.user.username)

        # Generar código QR
        qr_code = generate_qr_code(uri)

        context.update(
            {
                "secret": secret,
                "qr_code": qr_code,
            }
        )

        return context

    def post(self, request, *args, **kwargs):
        code = request.POST.get("code")
        secret = request.session.get("temp_totp_secret")

        if secret and verify_totp(secret, code):
            # Guardar el secreto en el perfil del usuario
            user = request.user
            user.totp_secret = secret
            user.two_factor_enabled = True
            user.save()

            # Limpiar la sesión
            if "temp_totp_secret" in request.session:
                del request.session["temp_totp_secret"]

            messages.success(
                request,
                "La autenticación de dos factores ha sido configurada correctamente.",
            )
            return redirect("users_app:user-profile")
        else:
            messages.error(
                request, "El código introducido no es válido. Inténtalo de nuevo."
            )
            return self.get(request, *args, **kwargs)


class TwoFactorDisableView(LoginRequiredMixin, FormView):
    """
    Vista para desactivar la autenticación de dos factores.
    """

    template_name = "users/two_factor_disable.html"
    form_class = AuthenticationForm
    success_url = reverse_lazy("users_app:user-profile")

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs["data"] = kwargs.get("data", {})
        kwargs["data"] = {
            **kwargs["data"],
            "username": self.request.user.username,
        }
        return kwargs

    def form_valid(self, form):
        user = self.request.user
        user.totp_secret = None
        user.two_factor_enabled = False
        user.save()

        messages.success(
            self.request,
            "La autenticación de dos factores ha sido desactivada correctamente.",
        )
        return super().form_valid(form)


class TwoFactorVerifyView(FormView):
    """
    Vista para verificar el código de autenticación de dos factores.
    """

    template_name = "users/two_factor_verify.html"
    form_class = UserVerificationForm
    success_url = reverse_lazy("home_app:home")

    def dispatch(self, request, *args, **kwargs):
        # Verificar que haya un usuario pre-autenticado en la sesión
        if "pre_auth_user_id" not in request.session:
            return redirect("users_app:user-login")
        return super().dispatch(request, *args, **kwargs)

    def form_valid(self, form):
        user_id = self.request.session.get("pre_auth_user_id")
        try:
            user = User.objects.get(id=user_id)
            code = form.cleaned_data["verification_code"]

            if verify_totp(user.totp_secret, code):
                # Registro de la dirección IP
                user.last_login_ip = self.request.META.get("REMOTE_ADDR", "")
                user.save()

                # Iniciar sesión
                login(self.request, user)

                # Configurar duración de la sesión
                if self.request.session.get("pre_auth_remember", False):
                    self.request.session.set_expiry(60 * 60 * 24 * 30)  # 30 días
                else:
                    self.request.session.set_expiry(0)  # Cerrar al cerrar navegador

                # Limpiar datos de pre-autenticación
                del self.request.session["pre_auth_user_id"]
                if "pre_auth_remember" in self.request.session:
                    del self.request.session["pre_auth_remember"]

                messages.success(
                    self.request,
                    "Autenticación de dos factores completada correctamente.",
                )
                return super().form_valid(form)
            else:
                form.add_error(
                    "verification_code", "Código incorrecto. Inténtalo de nuevo."
                )
                return self.form_invalid(form)

        except User.DoesNotExist:
            form.add_error(
                None, "Ha ocurrido un error. Por favor, inicia sesión de nuevo."
            )
            return self.form_invalid(form)


class UserProfileView(LoginRequiredMixin, TemplateView):
    """
    Vista para mostrar y gestionar el perfil del usuario.
    """

    template_name = "users/profile.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["user"] = self.request.user
        return context


class ChangePasswordView(LoginRequiredMixin, FormView):
    """
    Vista para cambiar la contraseña del usuario.
    """

    template_name = "users/change_password.html"
    form_class = UpdatePasswordForm
    success_url = reverse_lazy("users_app:user-profile")

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs["user"] = self.request.user
        return kwargs

    def form_valid(self, form):
        # Cambiar la contraseña del usuario
        user = self.request.user
        password = form.cleaned_data["password_new"]
        user.set_password(password)
        user.save()

        # Actualizar la sesión para que el usuario no sea desconectado
        update_session_auth_hash(self.request, user)

        messages.success(self.request, _("Contraseña actualizada correctamente."))
        return super().form_valid(form)


class UserListView(LoginRequiredMixin, ListView):
    """
    Vista para listar todos los usuarios del sistema.
    """

    model = User
    template_name = "users/user_list.html"
    context_object_name = "users"
    login_url = reverse_lazy("users_app:user-login")

    def get_queryset(self):
        return User.objects.all().order_by("username")


class UserDetailView(LoginRequiredMixin, DetailView):
    """
    Vista para ver los detalles de un usuario.
    """

    model = User
    template_name = "users/user_detail.html"
    context_object_name = "user_detail"
    login_url = reverse_lazy("users_app:user-login")


class UserUpdateView(LoginRequiredMixin, UpdateView):
    """
    Vista para actualizar la información de un usuario.
    """

    model = User
    template_name = "users/user_edit.html"
    fields = [
        "username",
        "email",
        "nombres",
        "apellidos",
        "genero",
        "is_active",
        "is_staff",
    ]
    success_url = reverse_lazy("users_app:user-list")
    login_url = reverse_lazy("users_app:user-login")

    def form_valid(self, form):
        messages.success(self.request, _("Usuario actualizado correctamente."))
        return super().form_valid(form)


class UserDeleteView(LoginRequiredMixin, DeleteView):
    """
    Vista para eliminar un usuario.
    """

    model = User
    template_name = "users/user_confirm_delete.html"
    success_url = reverse_lazy("users_app:user-list")
    login_url = reverse_lazy("users_app:user-login")

    def delete(self, request, *args, **kwargs):
        messages.success(self.request, _("Usuario eliminado correctamente."))
        return super().delete(request, *args, **kwargs)


class EmailVerificationView(View):
    """
    Vista para verificar el correo electrónico mediante token.
    """

    def get(self, request, token):
        try:
            user = User.objects.get(codregistro=token)
            user.is_active = True
            user.codregistro = ""
            user.save()
            messages.success(
                request, _("Cuenta verificada correctamente. Ya puede iniciar sesión.")
            )
            return redirect("users_app:user-login")
        except User.DoesNotExist:
            messages.error(request, _("Token de verificación inválido."))
            return redirect("users_app:user-login")


class Setup2FAView(LoginRequiredMixin, View):
    """
    Vista para configurar la autenticación de dos factores (2FA).
    """

    template_name = "users/setup_2fa.html"
    login_url = reverse_lazy("users_app:user-login")

    def get(self, request):
        secret = generate_totp_secret()
        request.session["temp_totp_secret"] = secret
        uri = generate_totp_uri(secret, request.user.username)
        qr_code = generate_qr_code(uri)

        return render(
            request, self.template_name, {"secret": secret, "qr_code": qr_code}
        )

    def post(self, request):
        code = request.POST.get("code")
        secret = request.session.get("temp_totp_secret")

        if secret and verify_totp(secret, code):
            user = request.user
            user.totp_secret = secret
            user.two_factor_enabled = True
            user.save()

            if "temp_totp_secret" in request.session:
                del request.session["temp_totp_secret"]

            messages.success(
                request,
                _(
                    "La autenticación de dos factores ha sido configurada correctamente."
                ),
            )
            return redirect("users_app:user-profile")
        else:
            messages.error(
                request, _("El código introducido no es válido. Inténtalo de nuevo.")
            )
            return redirect("users_app:setup-2fa")


class Verify2FAView(View):
    """
    Vista para verificar el código de autenticación de dos factores.
    """

    template_name = "users/verify_2fa.html"

    def get(self, request):
        if "pre_auth_user_id" not in request.session:
            return redirect("users_app:user-login")
        return render(request, self.template_name)

    def post(self, request):
        if "pre_auth_user_id" not in request.session:
            return redirect("users_app:user-login")

        user_id = request.session.get("pre_auth_user_id")
        code = request.POST.get("code")

        try:
            user = User.objects.get(id=user_id)

            if verify_totp(user.totp_secret, code):
                # Registrar IP
                user.last_login_ip = request.META.get("REMOTE_ADDR", "")
                user.save()

                # Iniciar sesión
                login(request, user)

                # Configurar duración de sesión
                if request.session.get("pre_auth_remember", False):
                    request.session.set_expiry(60 * 60 * 24 * 30)  # 30 días
                else:
                    request.session.set_expiry(0)

                # Limpiar datos de pre-autenticación
                del request.session["pre_auth_user_id"]
                if "pre_auth_remember" in request.session:
                    del request.session["pre_auth_remember"]

                messages.success(
                    request,
                    _("Autenticación de dos factores completada correctamente."),
                )
                return redirect(reverse_lazy("home_app:panel_cubo"))
            else:
                messages.error(request, _("Código incorrecto. Inténtalo de nuevo."))
                return render(request, self.template_name)

        except User.DoesNotExist:
            messages.error(
                request, _("Ha ocurrido un error. Por favor, inicia sesión de nuevo.")
            )
            return redirect("users_app:user-login")


# API Views para usuarios
@login_required(login_url=reverse_lazy("users_app:user-login"))
def api_user_list(request):
    """
    API endpoint para listar usuarios.
    Devuelve un JSON con la lista de usuarios.
    """
    users = User.objects.all().values(
        "id", "username", "email", "nombres", "apellidos", "is_active"
    )
    return JsonResponse({"users": list(users)})


@login_required(login_url=reverse_lazy("users_app:user-login"))
def api_user_detail(request, pk):
    """
    API endpoint para obtener detalles de un usuario específico.
    Devuelve un JSON con los detalles del usuario.
    """
    try:
        user = User.objects.get(pk=pk)
        data = {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "nombres": user.nombres,
            "apellidos": user.apellidos,
            "genero": user.genero,
            "is_active": user.is_active,
            "is_staff": user.is_staff,
            "date_joined": user.date_joined.strftime("%Y-%m-%d %H:%M:%S"),
            "last_login": (
                user.last_login.strftime("%Y-%m-%d %H:%M:%S")
                if user.last_login
                else None
            ),
        }
        return JsonResponse({"status": "success", "user": data})
    except User.DoesNotExist:
        return JsonResponse(
            {"status": "error", "message": "Usuario no encontrado"}, status=404
        )
