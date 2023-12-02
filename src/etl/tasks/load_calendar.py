import holidays
import locale
import pytz
import pandas as pd
from celery import shared_task
from datetime import datetime
from relatorios.utils import LoadPipeline
from .load_log import LoadLogView
from ..models import Calendar

BRZ_TZ = pytz.timezone('America/Sao_Paulo')

###CRIAR API QUE RETORNA A LISTA DE TODOS OS FERIADOS
### RECEBER FILTRO COM DATA INICIO E FIM PARA CALCULAR O CALENDARIO E SE NÃO TIVER CALCULAR COM O ANO ATUAL
### RECEBER COMO FILTRO QUAIS FERIADOS SERÃO CONSIDERADOS COMO PONTO FACULTATIVO.

class LoadCalendar(LoadPipeline):  
    
    def __init__(self, **kwargs):
        super().__init__()
        self.data_inicio = kwargs.get("data_inicio")
        self.data_fim = kwargs.get("data_fim")
        self.ponto_facultativo = kwargs.get("ponto_facultativo")
        self.filters = {k:v for k,v in kwargs.items() if k != 'task_id'}
        super().__init__()
        locale.setlocale(locale.LC_TIME, 'pt_BR.utf-8')
        self.params = dict(**self.filters)
    
    def run(self) -> dict:

        self._set_and_transform_dataset()

        if not self.calendar_dataset.empty:
            self._delete_old_records()
            self._create_new_records()
        else:
            self.log['message'] = "Não há registros a serem criados."

        self.log['finished_at'] = datetime.utcnow()
        return self.log

    
    def _set_and_transform_dataset(self) -> None:
        """Gera o dataset com o calendário e as colunas necessárias"""
        self.calendar_dataset = (
            pd.DataFrame()
            .assign(
                data=lambda d_: [
                BRZ_TZ.localize(datetime.combine(date, datetime.min.time()))
                for date in pd.date_range(start=self.data_inicio, end=self.data_fim)],
                feriado = lambda d_:
                    d_["data"].apply(
                        lambda x: holidays.Brazil()
                        .get(x.strftime('%Y-%m-%d'))),
                dia_semana = lambda d_: (
                    d_['data']
                    .dt.day_of_week.map({
                        i: nome for i, nome in enumerate([
                            'Segunda','Terça','Quarta',
                            'Quinta','Sexta','Sábado',
                            'Domingo'])})),
                dia_util = lambda d_: ~((d_['data'].dt.day_of_week >= 5) | d_['feriado'].notnull()),
                ponto_facultativo = lambda d_: (
                    d_['feriado'].isin(self.ponto_facultativo) & (d_['feriado'].notnull())),
                ano = lambda d_: d_['data'].dt.year,
                mes = lambda d_: d_['data'].dt.month,
                dia = lambda d_: d_['data'].dt.day,
                mes_ano = lambda d_: d_['data'].dt.strftime('%B/%Y'),  
                trimestre = lambda d_: 'T' + d_['data'].dt.quarter.astype(str)))
    
    def _delete_old_records(self):
        """Deleta os registros antigos do model, independente do que há dentro"""
        if Calendar.objects.exists():
            n_deleted, _ = Calendar.objects.all().delete()
            self.log['n_deleted'] = n_deleted
        else:
            self.log['message'] = "Não há registros a serem deletados."

    def _create_new_records(self):
        """Cria os registros no banco de dados e salva o log."""
        try:
            load_log_task = LoadLogView(log=self.log, load=Calendar)
            load_log_task.run()
            load_log_id = load_log_task.get_last_inserted_id()

            objs = [Calendar(**vals) for vals in self.calendar_dataset.pipe(self.prepare_to_json).to_dict('records')]
            for obj in objs:
                obj.load_log_id = load_log_id
            Calendar.objects.bulk_create(objs=objs, batch_size=500)
            self.log['n_inserted'] = len(objs)
            self.log['message'] = "Dados Carregados com sucesso!"
        except Exception as e:
            self.log['create_error'] = str(e)

@shared_task(
    name='etl.load_calendar',
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=5,
    retry_kwargs={'max_retries': 3})
def load_consolidacao_comissao_async(self, **kwargs) -> dict:
    with LoadCalendar(**kwargs) as task:
        log = task.run()
    return log