import pandas as pd
from app.utils.DataFrameGenerics import DataFrameGenerics
from django.db import models
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from ...tasks.load_fato_meta import LoadFatoMeta

class LoadFatoMetaView(APIView, DataFrameGenerics):
    """View que aciona a task de LoadFatoMeta"""
    def post(self, request, *args, **kwargs) -> Response:
        params = {k: v for k, v in request.data.items()}
        # load_fato_meta_async.delay(**params)
        # return Response(
        #     {"message": "A requisição foi recebida e a carga foi iniciada!"}, status=status.HTTP_202_ACCEPTED
        # )
        with LoadFatoMeta(**params) as load:
            log = load.run()
        return Response(log)