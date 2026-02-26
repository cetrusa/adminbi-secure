from django import forms
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import Permission
from django.utils.translation import gettext_lazy as _
from django.core.exceptions import ValidationError
import ast
import json
from import_export.admin import ImportExportModelAdmin
from import_export import resources

from .models import User, RegistroAuditoria, UserProfile, UserPermission
from apps.permisos.models import ConfEmpresas


class UserProfileInline(admin.StackedInline):
    """Inline admin para UserProfile que permite editar perfiles desde el admin de User."""

    model = UserProfile
    can_delete = False
    verbose_name_plural = _("Perfil de usuario")


class UserAdminForm(forms.ModelForm):
    """Formulario personalizado para la administración de usuarios."""

    conf_empresas = forms.ModelMultipleChoiceField(
        queryset=ConfEmpresas.objects.all().order_by("nmEmpresa"),  # Ordenar por nombre
        widget=forms.CheckboxSelectMultiple,
        required=False,
        label=_("Empresas"),
        help_text=_("Seleccione las empresas a las que el usuario tendrá acceso."),
    )
    select_all_empresas = forms.BooleanField(
        required=False,
        label=_("Seleccionar todas las empresas"),
        help_text=_("Marque esta casilla para dar acceso a todas las empresas."),
    )

    class Meta:
        model = User
        fields = "__all__"
        help_texts = {
            "is_active": _("Indica si el usuario puede iniciar sesión."),
            "is_staff": _(
                "Indica si el usuario puede acceder al panel de administración."
            ),
            "is_superuser": _(
                "Indica si el usuario tiene todos los permisos sin asignación explícita."
            ),
        }

    def __init__(self, *args, **kwargs):
        super(UserAdminForm, self).__init__(*args, **kwargs)

        # Mejorar la usabilidad con fieldsets claros
        for field_name in ["username", "email", "password"]:
            if field_name in self.fields:
                self.fields[field_name].help_text = _(
                    f"Requerido. {field_name.capitalize()} único para identificación del usuario."
                )

        # Verificar si el usuario tiene todas las empresas seleccionadas
        if self.instance.pk and ConfEmpresas.objects.exists():
            has_all_empresas = (
                self.instance.conf_empresas.count() == ConfEmpresas.objects.count()
            )
            self.fields["select_all_empresas"].initial = has_all_empresas

    def clean(self):
        """Validación adicional del formulario."""
        cleaned_data = super().clean()
        select_all_empresas = cleaned_data.get("select_all_empresas")

        # Si se seleccionan todas las empresas, asegurar que conf_empresas esté vacío
        # para evitar redundancia en la validación
        if select_all_empresas and "conf_empresas" in cleaned_data:
            cleaned_data["conf_empresas"] = ConfEmpresas.objects.all()

        return cleaned_data

    def clean_conf_empresas(self):
        """Procesa la selección de empresas."""
        select_all_empresas = self.cleaned_data.get("select_all_empresas")
        if select_all_empresas:
            return ConfEmpresas.objects.all()
        return self.cleaned_data["conf_empresas"]

    def save(self, commit=True):
        """Guarda el usuario con sus empresas asociadas."""
        instance = super(UserAdminForm, self).save(commit=False)
        select_all_empresas = self.cleaned_data.get("select_all_empresas")

        if commit:
            instance.save()
            # Establecer las empresas según la selección
            if select_all_empresas:
                instance.conf_empresas.set(ConfEmpresas.objects.all())
            else:
                self.save_m2m()

        return instance


class UserResource(resources.ModelResource):
    get_empresas_nombres = resources.Field()

    class Meta:
        model = User
        fields = (
            "id",
            "username",
            "email",
            "nombres",
            "apellidos",
            "genero",
            "is_active",
            "is_staff",
            "date_joined",
            "get_empresas_nombres",
        )
        export_order = (
            "id",
            "username",
            "email",
            "nombres",
            "apellidos",
            "genero",
            "is_active",
            "is_staff",
            "date_joined",
            "get_empresas_nombres",
        )

    def dehydrate_get_empresas_nombres(self, user):
        return user.get_empresas_nombres()


def print_users(modeladmin, request, queryset):
    """Acción para imprimir usuarios seleccionados (abre vista de impresión simple)."""
    from django.http import HttpResponse

    rows = [
        f"{user.username}, {user.email}, {user.nombres}, {user.apellidos}, {user.date_joined.strftime('%Y-%m-%d %H:%M:%S')}"
        for user in queryset
    ]
    html = "<html><head><title>Imprimir usuarios</title></head><body>"
    html += "<h2>Listado de usuarios</h2><pre>" + "\n".join(rows) + "</pre>"
    html += "<script>window.print();</script></body></html>"
    return HttpResponse(html)


print_users.short_description = _("Imprimir usuarios seleccionados")


class UserAdmin(ImportExportModelAdmin, BaseUserAdmin):
    """Admin personalizado para el modelo User que extiende el UserAdmin de Django."""

    form = UserAdminForm
    resource_class = UserResource
    inlines = [UserProfileInline]

    # Campos a mostrar en el listado
    list_display = (
        "username",
        "email",
        "nombres",
        "apellidos",
        "genero",
        "is_active",
        "is_staff",
        "get_empresas_count",
        "get_empresas_nombres",
        "date_joined",
    )

    # Filtros en la barra lateral
    list_filter = ("is_active", "is_staff", "is_superuser", "genero")

    # Campos de búsqueda
    search_fields = ("username", "email", "nombres", "apellidos")

    # Ordenación por defecto
    ordering = ("-date_joined",)  # Ordenar por fecha de creación descendente

    # Organización en fieldsets para mejor visualización
    fieldsets = (
        (None, {"fields": ("username", "password")}),
        (
            _("Información Personal"),
            {"fields": ("email", "nombres", "apellidos", "genero")},
        ),
        (
            _("Permisos"),
            {
                "fields": (
                    "is_active",
                    "is_staff",
                    "is_superuser",
                    "groups",
                    "user_permissions",
                ),
            },
        ),
        (
            _("Empresas"),
            {
                "fields": ("select_all_empresas", "conf_empresas"),
                "description": _(
                    "Seleccione las empresas a las que este usuario tendrá acceso"
                ),
            },
        ),
        (_("Seguridad"), {"fields": ("codregistro", "two_factor_enabled")}),
        (_("Fechas importantes"), {"fields": ("last_login",)}),
    )

    # Campos para la creación de usuarios
    add_fieldsets = (
        (
            None,
            {
                "classes": ("wide",),
                "fields": ("username", "email", "password1", "password2"),
            },
        ),
        (_("Información Personal"), {"fields": ("nombres", "apellidos", "genero")}),
        (
            _("Permisos"),
            {
                "fields": ("is_active", "is_staff", "is_superuser", "groups"),
            },
        ),
        (
            _("Empresas"),
            {
                "fields": ("select_all_empresas", "conf_empresas"),
            },
        ),
    )

    # Campos con selección múltiple
    filter_horizontal = ("groups", "user_permissions", "conf_empresas")

    def get_empresas_count(self, obj):
        """Método para mostrar el número de empresas asignadas al usuario."""
        count = obj.conf_empresas.count()
        total = ConfEmpresas.objects.count()
        return f"{count}/{total}" if count < total else _("Todas")

    get_empresas_count.short_description = _("Empresas")

    def get_empresas_nombres(self, obj):
        """Método para mostrar los nombres de las empresas asociadas."""
        return ", ".join([empresa.nmEmpresa for empresa in obj.conf_empresas.all()])

    get_empresas_nombres.short_description = _("Nombres de Empresas")

    actions = [print_users]

    class Media:
        js = ("admin/js/select_all.js",)
        css = {"all": ("admin/css/custom_admin.css",)}


class JSONWidget(forms.Textarea):
    """Widget para visualizar y editar campos JSON."""

    def __init__(self, attrs=None):
        default_attrs = {"rows": 4, "cols": 40, "class": "vLargeTextField json-editor"}
        if attrs:
            default_attrs.update(attrs)
        super().__init__(default_attrs)


class UserPermissionForm(forms.ModelForm):
    """Formulario para el modelo UserPermission con validación de JSON."""

    class Meta:
        model = UserPermission
        fields = "__all__"
        widgets = {
            "proveedores": JSONWidget(),
            "macrozonas": JSONWidget(),
        }

    def clean_proveedores(self):
        """Valida que el campo proveedores sea un JSON válido."""
        data = self.cleaned_data.get("proveedores", "")
        if not data:
            return ""

        try:
            # Intentar convertir el texto a estructura Python
            json_data = json.loads(data)
            # Volver a convertir a JSON formateado (para visualización más limpia)
            return json.dumps(json_data, indent=2)
        except json.JSONDecodeError:
            # Intentar con ast.literal_eval si el JSON no es válido
            try:
                python_data = ast.literal_eval(data)
                return json.dumps(python_data, indent=2)
            except (ValueError, SyntaxError):
                raise ValidationError(_("Ingrese un JSON válido para proveedores."))

    def clean_macrozonas(self):
        """Valida que el campo macrozonas sea un JSON válido."""
        data = self.cleaned_data.get("macrozonas", "")
        if not data:
            return ""

        try:
            # Intentar convertir el texto a estructura Python
            json_data = json.loads(data)
            # Volver a convertir a JSON formateado (para visualización más limpia)
            return json.dumps(json_data, indent=2)
        except json.JSONDecodeError:
            # Intentar con ast.literal_eval si el JSON no es válido
            try:
                python_data = ast.literal_eval(data)
                return json.dumps(python_data, indent=2)
            except (ValueError, SyntaxError):
                raise ValidationError(_("Ingrese un JSON válido para macrozonas."))


class UserPermissionAdmin(admin.ModelAdmin):
    """Admin para gestionar los permisos de usuario por empresa."""

    form = UserPermissionForm
    list_display = ("user", "empresa", "get_proveedores_count", "get_macrozonas_count")
    search_fields = ("user__username", "empresa__nmEmpresa")
    list_filter = ("empresa", "user")
    autocomplete_fields = ["user", "empresa"]

    def get_proveedores_count(self, obj):
        """Muestra la cantidad de proveedores asignados."""
        try:
            if not obj.proveedores:
                return "0"
            data = json.loads(obj.proveedores)
            if isinstance(data, list):
                return str(len(data))
            elif isinstance(data, dict):
                return str(len(data.keys()))
            return "?"
        except (json.JSONDecodeError, ValueError):
            return "Error"

    get_proveedores_count.short_description = _("Proveedores")

    def get_macrozonas_count(self, obj):
        """Muestra la cantidad de macrozonas asignadas."""
        try:
            if not obj.macrozonas:
                return "0"
            data = json.loads(obj.macrozonas)
            if isinstance(data, list):
                return str(len(data))
            elif isinstance(data, dict):
                return str(len(data.keys()))
            return "?"
        except (json.JSONDecodeError, ValueError):
            return "Error"

    get_macrozonas_count.short_description = _("Macrozonas")


class RegistroAuditoriaAdmin(admin.ModelAdmin):
    """Admin para visualizar los registros de auditoría."""

    list_display = (
        "fecha_hora",
        "usuario",
        "ip",
        "database_name",
        "city",
        "transaccion",
    )
    list_filter = ("fecha_hora", "usuario", "database_name", "city")
    search_fields = ("usuario__username", "ip", "transaccion", "city")
    readonly_fields = (
        "fecha_hora",
        "usuario",
        "ip",
        "transaccion",
        "detalle",
        "database_name",
        "city",
    )
    date_hierarchy = "fecha_hora"

    def has_add_permission(self, request):
        """Deshabilita la creación manual de registros de auditoría."""
        return False

    def has_change_permission(self, request, obj=None):
        """Deshabilita la edición de registros de auditoría."""
        return False


# Registrar los modelos en el admin
admin.site.register(User, UserAdmin)
admin.site.register(UserPermission, UserPermissionAdmin)
admin.site.register(RegistroAuditoria, RegistroAuditoriaAdmin)
admin.site.register(Permission)
