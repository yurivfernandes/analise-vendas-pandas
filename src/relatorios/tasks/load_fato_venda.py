import locale
import pandas as pd
from celery import shared_task
from datetime import datetime
from django.utils import timezone
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from etl.models import Venda, Vendedor, Produto
from etl.tasks import LoadLogView
from ..models import FatoVenda
from ..utils import LoadPipeline

class LoadFatoVenda(LoadPipeline):
    """Carrega os dados da model [FatoVenda]"""
    
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
        self.dataset = self._venda_dataset
    
    @property
    def _venda_dataset(self)->pd.DataFrame:
        fields = [
            'data','nfe',
            'codigo_produto','codigo_vendedor',
            'equipe_vendas','quantidade',
            'preco_unitario'
        ]
        
        filtro = {
            'data__range': [self.filters['data_inicio'],self.filters['data_fim']],

        }
        qs = (
            Venda
            .objects
            .filter(**filtro)
            .values(*fields)
        )
        return (
            pd.DataFrame(qs, columns=fields)
            .pipe(self.convert_to_float, ['preco_unitario'])
            .pipe(self.convert_to_int, ['quantidade'])
            .pipe(self.convert_to_date, {"data": "%d-%b-%Y"}))
    
    def _transform_dataset(self) -> None:
        """Efetua as transformações necessárias no dataset"""
        self.dataset = (
            self.dataset
            .assign(
                vendedor_id = lambda d_: (self._assign_vendedor(d_['codigo_vendedor'])),
                produto_id = lambda d_: (self._assign_produto(d_['codigo_produto'])),
                )
            .drop(columns=['codigo_vendedor','codigo_produto'])
            .pipe(self.convert_to_date, {"data": "%Y-%m-%d"}))
    
    def _assign_vendedor(self, codigo_vendedor:pd.Series)->pd.Series:
        """Assina o vendedor_id de acordo com o código do vendedor"""
        filtro = {
            'codigo__in': [i for i in codigo_vendedor.unique() if pd.notna(i)]}
        fields = ['codigo','id']
        qs = (
            Vendedor.objects
            .filter(**filtro)
            .values(*fields)
            )
        frn_map = {
            f"{i['codigo']}": i['id']
            for i in qs
        }
        return (codigo_vendedor).map(frn_map)
    
    def _assign_produto(self, codigo_produto:pd.Series)->pd.Series:
        """Assina o produto_id de acordo com o código do produto"""
        filtro = {
            'codigo__in': ["Prod " + i for i in codigo_produto.unique() if pd.notna(i)]}
        fields = ['codigo','id']
        qs = (
            Produto.objects
            .filter(**filtro)
            .values(*fields)
            )
        frn_map = {
            f"{i['codigo']}": i['id']
            for i in qs
        }
        return ("Prod " + codigo_produto).map(frn_map)
    
    def _delete_old_records(self):
        """Deleta os registros antigos do model, de acordo com o filtro aplicado"""
        # Verificar se há registros no banco
        if FatoVenda.objects.exists():
            # Se houver registros, realizar a exclusão
            filtro = {
                'data__range' : [self.filters['data_inicio'], self.filters['data_fim']]
            }
            n_deleted, _ = FatoVenda.objects.filter(**filtro).delete()
            self.log['n_deleted'] = n_deleted
        else:
            # Se não houver registros, retornar o log
            self.log['message'] = "Não há registros a serem deletados."
    
    def _create_new_records(self):
        """Cria os novos registros no banco."""
        try:
            # Chamar a task LoadLog para salvar o registro de log e obter o ID gerado
            load_log_task = LoadLogView(log=self.log, load=f"{FatoVenda}")
            load_log_task.run()
            load_log_id = load_log_task.get_last_inserted_id()  # Aqui você obtém o ID gerado pela task LoadLog

            # Atualizar o campo que você deseja associar ao ID da LoadLog em cada objeto criado
            objs = [FatoVenda(**vals) for vals in self.dataset.pipe(self.prepare_to_json).to_dict('records')]
            for obj in objs:
                obj.load_log_id = load_log_id

            # Usar bulk_create para criar todos os objetos de uma vez
            FatoVenda.objects.bulk_create(objs=objs, batch_size=500)
            self.log['n_inserted'] = len(objs)
            self.log['message'] = "Dados Carregados com sucesso!"
        except Exception as e:
            # Tratar a exceção em caso de falha na criação de registros
            self.log['create_error'] = str(e)
    
@shared_task(
    name='relatorios.load_fato_venda',
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=5,
    retry_kwargs={'max_retries': 3})
def load_fato_venda_async(self, versao_id: int, **filters) -> LoadFatoVenda:
    with LoadFatoVenda(**filters) as load:
        log = load.run()
    return log