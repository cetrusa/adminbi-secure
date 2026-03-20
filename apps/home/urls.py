#
from django.urls import path

from . import views
from . import views_email_config as email_views
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
        "panel_planos/",
        views.HomePanelPlanosPage.as_view(),
        name="panel_planos",
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
        "faltantes/",
        views.FaltantesPage.as_view(),
        name="faltantes",
    ),
    path(
        "preventa/",
        views.PreventaPage.as_view(),
        name="preventa",
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
    path("cubo-kpis/", views.CuboKpisAjaxView.as_view(), name="cubo_kpis_ajax"),
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
    # --- Email config (proveedores / supervisores) ---
    path("email-config/init-tables/", email_views.InitEmailTablesView.as_view(), name="init_email_tables"),
    path("email-config/proveedores/", email_views.ProveedoresBiListView.as_view(), name="proveedores_bi_list"),
    path("email-config/proveedores/crear/", email_views.ProveedoresBiCreateView.as_view(), name="proveedores_bi_create"),
    path("email-config/proveedores/<int:pk>/editar/", email_views.ProveedoresBiEditView.as_view(), name="proveedores_bi_edit"),
    path("email-config/proveedores/<int:pk>/eliminar/", email_views.ProveedoresBiDeleteView.as_view(), name="proveedores_bi_delete"),
    path("email-config/proveedores/carga-masiva/", email_views.CargaMasivaProveedoresView.as_view(), name="carga_masiva_proveedores"),
    path("email-config/supervisores/", email_views.SupervisoresListView.as_view(), name="supervisores_list"),
    path("email-config/supervisores/crear/", email_views.SupervisoresCreateView.as_view(), name="supervisores_create"),
    path("email-config/supervisores/<int:pk>/editar/", email_views.SupervisoresEditView.as_view(), name="supervisores_edit"),
    path("email-config/supervisores/<int:pk>/eliminar/", email_views.SupervisoresDeleteView.as_view(), name="supervisores_delete"),
    path("email-config/supervisores/carga-masiva/", email_views.CargaMasivaSupervisoresView.as_view(), name="carga_masiva_supervisores"),
    path("email-config/macrozonas/", email_views.MacrozonasJsonView.as_view(), name="macrozonas_json"),
    # --- Trazabilidad Preventa vs Facturación ---
    path("trazabilidad/", views.TrazabilidadPage.as_view(), name="trazabilidad"),
    path("trazabilidad/data/", views.TrazabilidadDataAjaxView.as_view(), name="trazabilidad_data_ajax"),
    path("trazabilidad/kpis/", views.TrazabilidadKpisAjaxView.as_view(), name="trazabilidad_kpis_ajax"),
    # --- CDT (Planos para proveedores) ---
    path("cdt/", views.CdtPage.as_view(), name="cdt_planos"),
    path("cdt/historial/", views.CdtHistorialPage.as_view(), name="cdt_historial"),
    path("cdt/download/<int:envio_id>/", views.cdt_download, name="cdt_download"),
    path("cdt/reenviar/<int:envio_id>/", views.cdt_reenviar_sftp, name="cdt_reenviar_sftp"),
    # --- TSOL (Planos TrackSales para proveedores) ---
    path("tsol/", views.TsolPage.as_view(), name="tsol_planos"),
    path("tsol/historial/", views.TsolHistorialPage.as_view(), name="tsol_historial"),
    path("tsol/download/<int:envio_id>/", views.tsol_download, name="tsol_download"),
    path("tsol/reenviar/<int:envio_id>/", views.tsol_reenviar_ftp, name="tsol_reenviar_ftp"),
    # --- Cosmos (Planos para envío FTPS) ---
    path("cosmos/", views.CosmosPage.as_view(), name="cosmos_planos"),
    path("cosmos/historial/", views.CosmosHistorialPage.as_view(), name="cosmos_historial"),
    path("cosmos/download/<int:envio_id>/", views.cosmos_download, name="cosmos_download"),
    path("cosmos/reenviar/<int:envio_id>/", views.cosmos_reenviar_ftps, name="cosmos_reenviar_ftps"),
]
