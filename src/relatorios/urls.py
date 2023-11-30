from django.urls import path
from .api import views

urlpatterns = [
    path('load-fato-meta/', views.LoadFatoMetaView.as_view()),
    path('load-fato-venda/', views.LoadFatoVendaView.as_view()),
    path('vendas-equipe-consolidado/', views.AnaliseMensalEquipeVendasView.as_view()),
    path('vendas-vendedor-consolidado/', views.AnaliseMensalVendedorView.as_view()),
    path('vendas-produto-consolidado/', views.AnaliseMensalProdutoCodigoView.as_view()),
]