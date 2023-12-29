import locale
import pandas as pd
from celery import shared_task
from datetime import datetime
from django.utils import timezone
from etl.models import Meta, Vendedor
from etl.tasks import LoadLogView
from ..models import FatoMeta
from ..utils import LoadPipeline

class LoadFatoMeta(LoadPipeline):
    """Carrega os dados da model [FatoMeta]"""
    
    def __init__(self, **filters) -> None:
        self.log = {
            "started_at": timezone.now(),
            "finished_at": None,
            "n_inserted": 0,
            "n_deleted": 0,
            "details": None
        }
        self.filters = filters
        self.params = dict(**filters)
        locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
        pass
    
    def run(self) -> dict:
        self._fetch_meta_dataset()
        self._transform_dataset()
        if not self.dataset.empty:
            # Executar a exclusão de registros
            self._delete_old_records()
            
            # Executar a criação de registros
            self._create_new_records()
            #self.log['message'] = ""
        else:
            # Se não houver registros a serem criados, retornar o log
            self.log['message'] = "Não há registros a serem criados."

        #self.load_log()
        self.log['finished_at'] = datetime.utcnow()
        return self.log

    def _fetch_meta_dataset(self)-> None:
        """Carrega os dados do dataset principal"""
        self.dataset = self._meta_dataset
    
    @property
    def _meta_dataset(self)->pd.DataFrame:
        fields = [
            'mes','ano','vendedor',
            'faturamento','margem_bruta',
            'notas_emitidas'
        ]
        filtro = {
            'ano__gte': self.filters['data_inicio'].split('-')[0],
            'ano__lte': self.filters['data_fim'].split('-')[0],
            # 'mes__gte':self.filters['data_inicio'].split('-')[1],
            # 'mes__lte':self.filters['data_fim'].split('-')[1]
        }
        qs = (
            Meta
            .objects
            .filter(**filtro)
            .values(*fields)
        )
        return (
            pd.DataFrame(qs, columns=fields)
            .pipe(self.convert_to_float, ['faturamento', 'margem_bruta'])
            .pipe(self.convert_to_int, ['notas_emitidas']))
    
    def _transform_dataset(self) -> None:
        """Efetua as transformações necessárias no dataset"""
        self.dataset = (
            self.dataset
            .assign(
                data = lambda d_: self._convert_data('01-'+d_['mes'] + "-" +d_['ano'].astype(str)),
                vendedor_id = lambda d_: (self._assign_vendedor(d_['vendedor'])),
                )
            .drop(columns=['vendedor','mes','ano'])
            .pipe(self.convert_to_date, {"data": "%d-%b-%Y"}))
        
    def _convert_data(self, data:pd.Series)->pd.Series:
        data_corrigida = []
        for d in data:
            data_corrigida.append(datetime.strptime(d, '%d-%B-%Y'))
        return pd.Series(data_corrigida)
    
    def _assign_vendedor(self, vendedor:pd.Series)->pd.Series:
        """Assina o vendedor_id de acordo com o código do vendedor"""
        filtro = {
            'codigo__in': [i.split(' - ',1)[0] for i in vendedor.unique() if pd.notna(i)]}
        fields = ['codigo','id','vendedor']
        qs = (
            Vendedor.objects
            .filter(**filtro)
            .values(*fields)
            )
        frn_map = {
            f"{i['codigo']} - {i['vendedor']}": i['id']
            for i in qs
        }
        return (vendedor).map(frn_map)
    
    def _delete_old_records(self):
        """Deleta os registros antigos do model, de acordo com o filtro aplicado"""
        # Verificar se há registros no banco
        if FatoMeta.objects.exists():
            # Se houver registros, realizar a exclusão
            filtro = {
                'data__range' : [self.filters['data_inicio'], self.filters['data_fim']]
            }
            n_deleted, _ = FatoMeta.objects.filter(**filtro).delete()
            self.log['n_deleted'] = n_deleted
        else:
            # Se não houver registros, retornar o log
            self.log['message'] = "Não há registros a serem deletados."
    
    def _create_new_records(self):
        """Cria os novos registros no banco."""
        try:
            # Chamar a task LoadLog para salvar o registro de log e obter o ID gerado
            load_log_task = LoadLogView(log=self.log, load=f"{FatoMeta}")
            load_log_task.run()
            load_log_id = load_log_task.get_last_inserted_id()  # Aqui você obtém o ID gerado pela task LoadLog

            # Atualizar o campo que você deseja associar ao ID da LoadLog em cada objeto criado
            objs = [FatoMeta(**vals) for vals in self.dataset.pipe(self.prepare_to_json).to_dict('records')]
            for obj in objs:
                obj.load_log_id = load_log_id

            # Usar bulk_create para criar todos os objetos de uma vez
            FatoMeta.objects.bulk_create(objs=objs, batch_size=500)
            self.log['n_inserted'] = len(objs)
            self.log['message'] = "Dados Carregados com sucesso!"
        except Exception as e:
            # Tratar a exceção em caso de falha na criação de registros
            self.log['create_error'] = str(e)
    
@shared_task(
    name='relatorios.load_fato_meta',
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=5,
    retry_kwargs={'max_retries': 3})
def load_fato_meta_async(self, versao_id: int, **filters) -> LoadFatoMeta:
    with LoadFatoMeta(**filters) as load:
        log = load.run()
    return log