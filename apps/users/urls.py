from django.urls import path, reverse_lazy
from django.shortcuts import redirect


from django.contrib.auth.views import (
    PasswordResetView,
    PasswordResetDoneView,
    PasswordResetConfirmView,
    PasswordResetCompleteView,
)

from apps.users import views

app_name = "users_app"

urlpatterns = [
    path(
        "login/",
        views.LoginUser.as_view(),
        name="user-login",
    ),
    # Redirección automática a login (reemplaza la vista home que estaba comentada)
    path(
        "", lambda request: redirect("users_app:user-login"), name="redirect-to-login"
    ),
    # Ruta home (se mantiene como estaba)
    path("home/", views.home, name="home"),
    path(
        "logout/",
        views.LogoutView.as_view(),
        name="user-logout",
    ),
    # Rutas para perfil de usuario
    path(
        "profile/",
        views.UserProfileView.as_view(),
        name="user-profile",
    ),
    path(
        "profile/edit/",
        views.UserProfileView.as_view(),
        name="user-profile-edit",
    ),
    path(
        "profile/change-password/",
        views.ChangePasswordView.as_view(),
        name="change-password",
    ),
    # Rutas para administración de usuarios
    path(
        "users/list/",
        views.UserListView.as_view(),
        name="user-list",
    ),
    path(
        "users/detail/<int:pk>/",
        views.UserDetailView.as_view(),
        name="user-detail",
    ),
    path(
        "users/edit/<int:pk>/",
        views.UserUpdateView.as_view(),
        name="user-edit",
    ),
    path(
        "users/delete/<int:pk>/",
        views.UserDeleteView.as_view(),
        name="user-delete",
    ),
    path(
        "database/",
        views.DatabaseListView.as_view(),
        name="user-database",
    ),
    path(
        "base/",
        views.BaseView.as_view(),
        name="base",
    ),
    path("database/list/", views.database_list, name="database_list"),
    path(
        "register/",
        views.UserRegisterView.as_view(),
        name="user-register",
    ),
    # Rutas para la verificación con tokens
    path(
        "verify-email/<str:token>/",
        views.EmailVerificationView.as_view(),
        name="verify-email",
    ),
    path(
        "password_reset/",
        PasswordResetView.as_view(
            template_name="users/password_reset_form.html",
            email_template_name="users/password_reset_email.html",
            success_url=reverse_lazy("users_app:password_reset_done"),
        ),
        name="password_reset",
    ),
    path(
        "password_reset/done/",
        PasswordResetDoneView.as_view(template_name="users/password_reset_done.html"),
        name="password_reset_done",
    ),
    path(
        "reset/<uidb64>/<token>/",
        PasswordResetConfirmView.as_view(
            template_name="users/password_reset_confirm.html",
            success_url=reverse_lazy("users_app:password_reset_complete"),
        ),
        name="password_reset_confirm",
    ),
    path(
        "reset/done/",
        PasswordResetCompleteView.as_view(
            template_name="users/password_reset_complete.html"
        ),
        name="password_reset_complete",
    ),
    path(
        "user/verification/<int:pk>/",
        views.CodeVerificationView.as_view(),
        name="user-verification",
    ),
    # Nuevas rutas para autenticación de dos factores (2FA)
    path(
        "setup-2fa/",
        views.Setup2FAView.as_view(),
        name="setup-2fa",
    ),
    path(
        "verify-2fa/",
        views.Verify2FAView.as_view(),
        name="verify-2fa",
    ),
    # API endpoints JSON
    path("api/users/", views.api_user_list, name="api-user-list"),
    path("api/users/<int:pk>/", views.api_user_detail, name="api-user-detail"),
]
