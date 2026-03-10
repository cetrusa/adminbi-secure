from django.urls import path

from . import views

app_name = "bimbo_app"

urlpatterns = [
    path("panel_bimbo/", views.HomePanelBimboPage.as_view(), name="panel_bimbo"),
    path("venta-cero/", views.VentaCeroPage.as_view(), name="venta_cero"),
    path(
        "venta-cero/lookups/proveedores/",
        views.VentaCeroProveedorLookup.as_view(),
        name="venta_cero_lookup_proveedores",
    ),
    path(
        "venta-cero/lookups/categorias/",
        views.VentaCeroCategoriaLookup.as_view(),
        name="venta_cero_lookup_categorias",
    ),
    path(
        "venta-cero/lookups/subcategorias/",
        views.VentaCeroSubcategoriaLookup.as_view(),
        name="venta_cero_lookup_subcategorias",
    ),
    path(
        "venta-cero/lookups/productos/",
        views.VentaCeroProductoLookup.as_view(),
        name="venta_cero_lookup_productos",
    ),
    path("rutero/", views.RuteroPage.as_view(), name="rutero"),
    path("inventarios/", views.InventariosPage.as_view(), name="inventarios"),
    path(
        "inventarios/data/",
        views.InventariosDataAjaxView.as_view(),
        name="inventarios_data",
    ),
    path("preventa/", views.PreventaPage.as_view(), name="preventa"),
    path("faltantes/", views.FaltantesPage.as_view(), name="faltantes"),
    path("planos-bimbo/", views.PlanosBimboPage.as_view(), name="planos_bimbo"),
    path(
        "equivalencias-bimbo/",
        views.BimboEquivalenciasPage.as_view(),
        name="equivalencias_bimbo",
    ),
    path(
        "equivalencias-bimbo/data/",
        views.BimboEquivalenciasDataView.as_view(),
        name="equivalencias_bimbo_data",
    ),
    path(
        "equivalencias-bimbo/match/",
        views.BimboMatchManualView.as_view(),
        name="equivalencias_bimbo_match",
    ),
    path(
        "homologacion-bimbo/",
        views.HomologacionBimboPage.as_view(),
        name="homologacion_bimbo",
    ),
    path(
        "permisos-bimbo/",
        views.BimboPermisosPage.as_view(),
        name="permisos_bimbo",
    ),
    path(
        "permisos-bimbo/data/",
        views.BimboPermisosDataView.as_view(),
        name="permisos_bimbo_data",
    ),
    path(
        "permisos-bimbo/save/",
        views.BimboPermisoSaveView.as_view(),
        name="permisos_bimbo_save",
    ),
    path(
        "permisos-bimbo/delete/",
        views.BimboPermisoDeleteView.as_view(),
        name="permisos_bimbo_delete",
    ),
    path("agregar-ceve/", views.AgregarCevePage.as_view(), name="agregar_ceve"),
    path(
        "agregar-ceve/empresas/",
        views.AgregarCeveEmpresasLookup.as_view(),
        name="agregar_ceve_empresas",
    ),
    path(
        "agregar-ceve/diagnostico-productos/",
        views.AgregarCeveDiagnosticoProductosView.as_view(),
        name="agregar_ceve_diagnostico",
    ),
]

