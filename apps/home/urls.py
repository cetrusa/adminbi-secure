#
from django.urls import path

from . import views
from .views import CleanMediaView

app_name = "home_app"

urlpatterns = [
    path(
        "panel_cubo/",
        views.HomePanelCuboPage.as_view(),
        name="panel_cubo",
    ),
    path(
        "panel_bi/",
        views.HomePanelBiPage.as_view(),
        name="panel_bi",
    ),
    path(
        "panel_actualizacion",
        views.HomePanelActualizacionPage.as_view(),
        name="panel_actualizacion",
    ),
    path(
        "panel_interface/",
        views.HomePanelInterfacePage.as_view(),
        name="panel_interface",
    ),
    path(
        "panel_left_planos/",
        views.HomePanelCuboPage.as_view(),
        name="panel_left_planos",
    ),
    path(
        "cubo/",
        views.CuboPage.as_view(),
        name="cubo",
    ),
    path(
        "proveedor/",
        views.ProveedorPage.as_view(),
        name="proveedor",
    ),
    path(
        "matrix/",
        views.MatrixPage.as_view(),
        name="matrix",
    ),
    path(
        "interface/",
        views.InterfacePage.as_view(),
        name="interface",
    ),
    path(
        "interface-siigo/",
        views.InterfaceSiigoPage.as_view(),
        name="interface_siigo",
    ),
    path(
        "actualizacion/",
        views.ActualizacionBdPage.as_view(),
        name="actualizacion",
    ),
    path(
        "plano/",
        views.PlanoPage.as_view(),
        name="plano",
    ),
    path("download_file/", views.DownloadFileView.as_view(), name="download_file"),
    path("delete_file/", views.DeleteFileView.as_view(), name="delete_file"),
    path(
        "check-task-status/",
        views.CheckTaskStatusView.as_view(),
        name="check_task_status",
    ),
    path("amovildesk/", views.AmovildeskPage.as_view(), name="amovildesk"),
    path("reporte-list/", views.ReporteListView.as_view(), name="reporte_list"),
    path(
        "reporteador/data/",
        views.ReporteadorDataAjaxView.as_view(),
        name="reporteador_data_ajax",
    ),
    path(
        "reporteador/",
        views.ReporteadorPage.as_view(),
        name="reporteador",
    ),
    path(
        "clean_media/",
        CleanMediaView.as_view(),
        name="clean_media",
    ),
]
