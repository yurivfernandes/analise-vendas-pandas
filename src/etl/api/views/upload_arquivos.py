import pandas as pd
from app.utils.DataFrameGenerics import DataFrameGenerics
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from etl.tasks import UploadArquivos


class UploadArquivosView(DataFrameGenerics, APIView):
    @classmethod
    def post(cls, request, **kwargs):
        try:
            sheet = pd.read_excel(request.FILES.getlist('file')[0])
            tabela = request.data.get('tabela')
        except Exception:
            return Response(
                {
                    "error": "A Planilha enviada está vazia!"
                },
                status= status.HTTP_400_BAD_REQUEST
            )
        if sheet.empty:
            return Response(
                {
                    "error": "A Planilha enviada está vazia!"
                },
                status= status.HTTP_400_BAD_REQUEST
            )
        upload = UploadArquivos(sheet = sheet, tabela=tabela)
        if tabela==None:
            return Response(
                {
                    "error": "É necessário informar a tabela a ser preenchida."
                },
                status= status.HTTP_400_BAD_REQUEST
            )
        try:
            result = upload.run()
        except Exception as e:
            return Response({"error" : str(e)}, status= status.HTTP_400_BAD_REQUEST)
        return Response(result)