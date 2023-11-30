from django.urls import path
from .api import views
from .testes_unitarios import TestUploadArquivos

urlpatterns = [
    path('upload-arquivos', views.UploadArquivosView.as_view()),
    path('teste/upload-arquivos', TestUploadArquivos.as_view()),
    path('available-tables/', views.AvailableTablesView.as_view())
]

