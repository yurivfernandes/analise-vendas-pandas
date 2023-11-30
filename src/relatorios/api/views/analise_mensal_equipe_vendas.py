import numpy as np
import pandas as pd
from app.utils.DataFrameGenerics import DataFrameGenerics
from django.db.models import F, ExpressionWrapper, DecimalField, functions
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.views import APIView
from ...models import FatoVenda

class AnaliseMensalEquipeVendasView(APIView, DataFrameGenerics):
    """Relatório de análise mensal de vendas por equipe para consumo no front"""

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="data_inicio",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                format=openapi.FORMAT_DATE,
                example = "2023-01-01",
                description="Data de início para filtro dos dados.",
                default=None,
                required=False),
            openapi.Parameter(
                name="data_fim",
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                format=openapi.FORMAT_DATE,
                example = "2023-01-01",
                description="Data de fim para filtro dos dados.",
                default=None,
                required=False),
        ],
        responses={
            200: openapi.Schema(
                type=openapi.TYPE_ARRAY,
                items=openapi.Items(
                    type=openapi.TYPE_OBJECT,
                    properties=None
                )
            )
        }
    )
    
    def __init__(self, **kwargs) -> None:
        self.data_inicio = kwargs.get('data_inicio')
        self.data_fim = kwargs.get('data_fim')
    
    def get(self, request, *args, **kwargs) -> Response:
        self.data_inicio = request.GET.get("data_inicio")
        self.data_fim = request.GET.get("data_fim")
        return Response(self.as_dict())
    
    def main(self)->pd.DataFrame:
        self._set_date_map()
        self._set_dataset()
        
        return self.dataset
    
    def as_dict(self) -> list:
        return (
            self.main()
            .pipe(self.prepare_to_export)
            .to_dict("records"))
    
    def _set_date_map(self) -> None:
        """Define o mapeamento entre datas em formato datetime.date e a string para consumo do front"""
        dt_list = [pd.to_datetime(self.data_inicio), pd.to_datetime(self.data_fim)]
        
        self.date_map = {
            d.date().isoformat(): f"{d.strftime('%Y')}-{d.strftime('%m')}"
            for d in
            pd.date_range(
                start=min(dt_list).replace(day=1),
                end=max(dt_list).replace(day=1),
                freq="MS")}
        
    def _set_dataset(self)-> None:
        """Cria o dataset principal para consumo do relatório"""
        self.dataset = (
            self._vendas
            .assign(
                data= lambda df_: df_["data"].astype("str").replace(self.date_map, regex=True))
            .pipe(self._pivot_data)
            .reset_index()
            .assign(
                key = lambda d_: np.arange(d_.shape[0]),
                total = lambda d_: d_[[c for c in self.date_map.values() if c in d_.columns]].sum(axis=1).astype("float64"))
            .sort_values(by=["equipe_vendas"])
            .fillna(0.0)
        )
    
    @property
    def _vendas(self)-> pd.DataFrame:
        """Trás os dados de venda agrupando por mês"""
        filtro = {'data__range': [self.data_inicio, self.data_fim]}
        qs = (
            FatoVenda
            .objects
            .filter(**filtro)
            .annotate(
                mes=functions.TruncMonth('data'),
                valor=ExpressionWrapper(
                    F('quantidade') * F('preco_unitario'), 
                    output_field=DecimalField()))
            .values('mes', 'valor','equipe_vendas'))
        return (
            pd.DataFrame(qs)
            .rename(columns= {'mes':'data'})
            .pipe(self.convert_to_float, 'valor')
            .groupby(['data','equipe_vendas'])
            .agg({'valor': 'sum'})
            .reset_index()
            .pipe(self.convert_to_date, {"data": "%d-%b-%Y"})
            .pipe(self.convert_to_first_month_day, ['data']))
    
    def _pivot_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Altera a estrutura dos dados do dataset com as colunas de índice definidas e a coluna de data pivotada"""
        idx_columns = [col for col in df.columns if col not in ('data', 'valor')]
        return (
            df
            .pivot_table(
                values='valor',
                index=idx_columns,
                columns=['data'],
                aggfunc=np.sum,
                fill_value=0.0
            )
        )
         
    @property
    def dataset_to_export(self) -> pd.DataFrame:
        """Transforma os dados do dataframe em formato padrão do python para que os métodos
        de exportação possam usar um só dataset tratado"""
        target_cols = (
            [{"name": "valor", "type": "decimal", "decimals": 2}] +
            [{"name": f, "type": "integer"} for f in self.fields.keys() if f.endswith("_id")]
        )
        return self.main().pipe(self.prepare_to_export, target_columns=target_cols)
    
    def export_file(self, **kwargs):
        data = self.main()
        file_type = kwargs.get("file_type", "csv")
        EXPORTS = {
            "csv": self.as_csv,
            "xlsx": self.as_xlsx,
            "txt": self.as_csv
        }
        args = {}
        if file_type == 'txt':
            args = {"extension": ".txt"}
        return EXPORTS.get(file_type)(**args)

    def as_csv(self, extension: str = ".csv"):
        return self.export_csv(
            df = self._dataset_to_export,
            file_name="dre",
            extension=extension
        )
    
    def _as_xlsx(self):
        col_frmt = [
            {"column": "data", "format": "date"},
            {"column": "valor", "format": "money"},
        ]
        return self.export_xlsx(
            df = self.dataset_to_export,
            file_name="dre",
            columns_formats=col_frmt
        )