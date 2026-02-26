from django.urls import path
from .views import HomePanelMonitorPage

app_name = 'monitor'

urlpatterns = [
    path('', HomePanelMonitorPage.as_view(), name='dashboard'),
]
