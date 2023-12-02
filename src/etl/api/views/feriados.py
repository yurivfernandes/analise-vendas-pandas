import holidays
import pandas as pd
from app.utils.DataFrameGenerics import DataFrameGenerics
from datetime import datetime
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

DATA_INICIO = pd.to_datetime(datetime.now().replace(day=1, month=1))
DATA_FIM = datetime.now().replace(day=31, month=12)

class FeriadosBrasilView(APIView, DataFrameGenerics):
    """Retorna Um Dicionário com todos os feriados nacionais, {data:nome_feriado}"""
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_inicio = kwargs.get("data_inicio")
        self.data_fim = kwargs.get("data_fim")
        self.kwargs = kwargs
    
    def get(self, request: Request, *args, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_inicio = request.data.get("data_inicio", DATA_INICIO)
        self.data_fim = request.data.get("data_fim", DATA_FIM)

        return Response(self.main())

    def main(self) -> list:
        return self._feriados_dict

    @property
    def _feriados_dict(self)-> pd.DataFrame:
        """Retorna um dicionario com os feriados por data e mês"""
        feriados_data = pd.to_datetime(pd.Series(holidays.Brazil()[self.data_inicio:self.data_fim]))
        return {data.strftime('%Y-%m-%d'):str(holidays.Brazil().get(data.strftime('%Y-%m-%d'))) for data in feriados_data}