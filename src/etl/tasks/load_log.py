from datetime import datetime
import pandas as pd
from app.utils.DataFrameGenerics import DataFrameGenerics
from rest_framework.views import APIView
from ..models import LoadLog
from django.db import close_old_connections

class LoadLogView(DataFrameGenerics, APIView):
    """API PARA INSERIR O LOG DE REGISTROS NA MODEL LOAD LOG"""

    def __init__(self, log: dict, load: str):
        super().__init__()
        self.log = log
        self.load = load
        self.dataset = pd.DataFrame()
    
    def run(self) -> None:
        """Roda a task, definindo os campos que serão inseridos."""
        self.dataset = self.dataset.assign(
                started_at = [self.log['started_at']],
                finished_at = [datetime.now()],
                n_inserted = [self.log['n_inserted']],
                load = [self.load],
                params = None)
        self._create()

    def _create(self)-> None:
        """Salva na model os registros"""
        close_old_connections()
        objs = [LoadLog(**vals) for vals in self.dataset.pipe(self.prepare_to_json).to_dict('records')]
        LoadLog.objects.bulk_create(objs=objs,batch_size=500)
    
    def get_last_inserted_id(self):
        """Retorna o ID do último registro inserido na tabela LoadLog."""
        try:
            latest_log = LoadLog.objects.latest('id')
            return latest_log.id
        except LoadLog.DoesNotExist:
            raise ValueError("Não foi possível obter o ID do último registro inserido na tabela LoadLog.")