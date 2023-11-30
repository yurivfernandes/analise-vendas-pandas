# api/views.py

from django.apps import apps
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from ..serializers import TableSerializer

class AvailableTablesView(APIView):
    def get(self, request):
        # Obtenha os nomes de todas as tabelas disponíveis no app etl, excluindo "LoadLog"
        app_models = apps.get_app_config('etl').get_models()
        table_names = [model._meta.object_name for model in app_models if model._meta.object_name != 'LoadLog']
        
        serializer = TableSerializer({'names': table_names})
        return Response(serializer.data, status=status.HTTP_200_OK)
