from django.urls import path
from .api import views

urlpatterns = [
    path('home-teste/', views.HomeView.as_view()),
]