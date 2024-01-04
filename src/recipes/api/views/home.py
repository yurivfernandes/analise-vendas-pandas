from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

class HomeView(APIView):

    def get(self, request, *args, **kwargs) -> HttpResponse:
        return HttpResponse('HOME')