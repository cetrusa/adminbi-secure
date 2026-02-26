#
from django.urls import path

from . import views

app_name = "bi_app"

urlpatterns = [
    path(
        'actualizacion-bi/', 
        views.ActualizacionBiPage.as_view(),
        name='actualizacion_bi',
    ),
    path(
        'reporte_embed/', 
        views.IncrustarBiPage.as_view(),
        name='reporte_embed',
    ),
    path(
        'reporte_bi_publico/', 
        views.ReporteBiPublicoPage.as_view(),
        name='reporte_bi_publico',
    ),
    path('eliminar_reporte_fetched/', views.EliminarReporteFetched.as_view(), name='eliminar_reporte_fetched'),
    
]