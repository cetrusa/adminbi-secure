#
from django.urls import path

from . import views
from .views import CargueInfoVentasPage, UploadMaestrasView, UploadInfoProductoView, CargueArchivosMaestrosView
from .views_checktaskstatus import CheckCargueTaskStatusView

app_name = "cargues_app"

urlpatterns = [
    path(
        "cargue/",
        views.UploadZipView.as_view(),
        name="cargue",
    ),
    path(
        "cargue_planos_tsol/",
        views.UploadPlanoFilesView.as_view(),
        name="cargue_planos_tsol",
    ),
    path(
        "cargue-infoventas/", CargueInfoVentasPage.as_view(), name="cargue_infoventas"
    ),
    path(
        "infoproducto/", UploadInfoProductoView.as_view(), name="infoproducto"
    ),
    path(
        "maestras/", UploadMaestrasView.as_view(), name="maestras"
    ),
    path(
        "archivos-maestros/", CargueArchivosMaestrosView.as_view(), name="cargue_archivos_maestros"
    ),
    path(
        "check_task_status/",
        CheckCargueTaskStatusView.as_view(),
        name="check_task_status",
    ),
]
