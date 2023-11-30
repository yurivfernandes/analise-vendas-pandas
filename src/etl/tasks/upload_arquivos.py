import pandas as pd
import importlib
import os
from app.utils.DataFrameGenerics import DataFrameGenerics
from datetime import datetime
from django.apps import apps
from django.db import models
from rest_framework.views import APIView
from .load_log import LoadLogView

class UploadArquivos(DataFrameGenerics, APIView):  
    TIPOS_DJANGO_PYTHON = {
            models.AutoField: int,
            models.IntegerField: int,
            models.SmallIntegerField: int,
            models.PositiveIntegerField: int,
            models.PositiveSmallIntegerField: int,
            models.BooleanField: bool,
            models.NullBooleanField: bool,
            models.FloatField: float,
            models.DecimalField: float,
            models.CharField: str,
            models.TextField: str,
            models.DateField: datetime,
            models.DateTimeField: datetime,
        }
    
    def __init__(self, sheet: pd.DataFrame, tabela: str):
        super().__init__()
        self.sheet = sheet
        self.tabela = tabela
        self.log = {
            'started_at': datetime.utcnow(),
            'finished_at': None,
            'n_processed': len(self.sheet),
            'n_inserted': 0
        }
    
    def run(self) -> dict:
        self._get_campos_model()

        self._set_dataset()

        self._test_dataset()

        self._transform_data()

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

    def _get_campos_model(self) -> None:
        """Busca os campos da model na qual serão salvos os dados do arquivo"""
        arquivo_model = self._encontrar_arquivo_model()
        self.model_name = self.tabela  # Armazena o nome do model para uso posterior no método delete_old_records

        self.fields = {}
        module_name = arquivo_model.replace(".py", "")
        try:
            module = importlib.import_module(f"etl.models.{module_name}")
        except ImportError:
            raise ValueError(f"Arquivo {arquivo_model} não encontrado ou não é um módulo válido.")

        # Encontrar a definição da classe do model
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if hasattr(attr, "_meta") and hasattr(attr._meta, "model_name") and attr._meta.model_name.lower() == self.tabela.lower():
                model_class = attr
                break
        else:
            raise ValueError(f"Model com o nome '{self.tabela}' não encontrado.")

        # Encontrar os campos do model e obter os tipos Python correspondentes
        for field in model_class._meta.fields:
            # Obter o nome, tipo Python e a opção 'null' do campo
            nome_campo = field.name
            tipo_python = self._get_tipo_python(field)
            is_nullable = field.null  # Indica se o campo permite valores nulos

            if nome_campo != 'id':
                self.fields[nome_campo.upper()] = {
                    'name': nome_campo,
                    'type': tipo_python,
                    'null': is_nullable,
                }
    
    def _encontrar_arquivo_model(self):
        """Obtém o nome arquivo na pasta models"""
        caminho_pasta_models = os.path.join(os.getcwd(), "src\\etl\\models")
        arquivos_models = [f for f in os.listdir(caminho_pasta_models) if f.endswith(".py")]

        # Procura o arquivo que contém o model com o nome da classe informado
        for arquivo_model in arquivos_models:
            try:
                module_name = arquivo_model.replace(".py", "")
                module = importlib.import_module(f"etl.models.{module_name}")
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if hasattr(attr, "_meta") and hasattr(attr._meta, "model_name"):
                        if attr._meta.object_name == self.tabela:
                            return arquivo_model
            except ImportError:
                pass

        raise ValueError(f"Model com o nome '{self.tabela}' não encontrado.")

    def _get_tipo_python(self, field):
        """Obtem o nome do tipo em pyton para incluir no mapeamento dos dados"""
        # Obter o tipo Python correspondente ao tipo do campo do model
        tipo_python = self.TIPOS_DJANGO_PYTHON.get(type(field), type(field))

        # Verificar se o campo permite valores nulos
        if hasattr(field, 'null'):
            tipo_python = f"{tipo_python} (null=True)" if field.null else f"{tipo_python} (null=False)"

        return tipo_python
    
    def _set_dataset(self) -> None:
        self.dataset = (
            self.sheet
            .pipe(self._assign_headers)
            .dropna(axis=1, how='all'))
    
    def _assign_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Promove a linha com dados à cabeçalho"""
        data_start = 0
        
        # Encontre a linha que contém os dados (primeira linha com algum valor não nulo)
        for idx, row in df.iterrows():
            if any(pd.notna(row)):
                data_start = idx
                break

        if data_start >= len(df):
            raise ValueError("A linha de início dos dados está além do tamanho do DataFrame.")

        # Promova a linha com dados para o cabeçalho
        if data_start > 0:
            df = df.iloc[data_start:]
            df.columns = df.iloc[0].astype(str).str.upper()
            df = df.iloc[1:]
            df = df.reset_index(drop=True)

        return df
        
    def _test_dataset(self) -> None:
        """Valida todas as colunas e linhas, verificando se o tipo de dados está correto e, se necessário, exclui linhas duplicadas."""
        self._validate_columns()

        # Verificar se o modelo tem restrições de chave única
        model = self._get_model_by_name()
        has_unique_constraints = hasattr(model._meta, 'unique_together') and model._meta.unique_together

        if has_unique_constraints:
            # Verificar campos duplicados no dataset
            duplicate_rows = self.dataset[self.dataset.duplicated(keep=False)]
            if not duplicate_rows.empty:
                print("Existem campos duplicados no dataset. Linhas duplicadas serão excluídas antes de salvar os dados.")
                self.dataset.drop_duplicates(inplace=True, ignore_index=True)
        else:
            print("O modelo não possui restrições de chave única. Valores duplicados no dataset são permitidos.")

        tests = []
        for col in self.dataset.columns:
            condition = self.dataset[col].isnull()
            message = f"O campo '{col}' está vazio nas linhas: []"
            tests.append({'name': f'validate_{col}', 'condition': condition, 'message': message})

        for tst in tests:
            if any(tst['condition']):
                linhas = [str(i + 2) for i in self.dataset.index[tst['condition']]]
                raise ValueError(tst['message'].replace('[]', ', '.join(linhas)))
    
    def _validate_columns(self) -> None:
        """Garante que todas as colunas necessárias existem no dataframe"""
        missing_fields = [
            field_name
            for field_name, field_info in self.fields.items()
            if field_info["name"].upper() != "LOAD_LOG" and not any(self.dataset.columns.str.contains(field_info["name"], case=False, na=False))
        ]

        if missing_fields:
            self.log["finished_at"] = datetime.now()
            missing_fields_str = ", ".join(missing_fields)
            self.log["message"] = f"A planilha não se encontra no padrão esperado. Os seguintes campos estão faltando: {missing_fields_str}. Por favor, garantir que as nomenclaturas estão corretas e que todas as colunas existam."
    
    def _transform_data(self) -> None:
        """Efetua as transformações necessárias no dataset."""
        self.dataset.columns = [col.replace('_', '').lower() for col in self.dataset.columns]
        self.dataset = (
            self.dataset
            .rename(columns={
                col: field_info['name']
                for col in self.dataset.columns
                for field_info in self.fields.values()
                if field_info['name'].lower() in col.lower()
            })
            .loc[:, [field_info['name'].replace('_', ' ') for field_info in self.fields.values() if field_info['name'].lower() != 'load_log']]
            .pipe(self._set_data_types)
        )
        self.dataset.columns = [col.replace(' ', '_').lower() for col in self.dataset.columns]

    def _set_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Define os tipos de dados das colunas de acordo com self.fields e converte o dataframe para esses tipos"""
        data_types = {}

        for field_info in self.fields.values():
            col_lower = field_info['name'].lower()
            matching_cols = [col for col in df.columns if col.lower() == col_lower]

            if matching_cols:
                data_type = field_info['type']
                is_nullable = ' (null=True)' in data_type

                # Converter os tipos de data para datetime
                if 'date' in data_type.lower() or 'time' in data_type.lower():
                     df[matching_cols[0]] = pd.to_datetime(df[matching_cols[0]], errors='coerce', format='%d/%m/%Y')

                # Definir o tipo correto do pandas com base na informação de valores nulos
                if 'int' in data_type:
                    dtype = 'Int64' if is_nullable else 'int'
                elif 'float' in data_type:
                    dtype = 'float' if is_nullable else 'float'
                elif 'bool' in data_type:
                    dtype = 'boolean' if is_nullable else 'bool'
                else:
                    dtype = 'object' if is_nullable else 'str'

                data_types[matching_cols[0]] = dtype

        return df.astype(data_types)
    
    def _delete_old_records(self):
        """Deleta os registros antigos do model, independente do que há dentro"""
        model = self._get_model_by_name()

        # Verificar se há registros no banco
        if model.objects.exists():
            # Se houver registros, realizar a exclusão
            n_deleted, _ = model.objects.all().delete()
            self.log['n_deleted'] = n_deleted
        else:
            # Se não houver registros, retornar o log
            self.log['message'] = "Não há registros a serem deletados."

    def _get_model_by_name(self):
        """Retorna o modelo Django com base no nome da classe do model"""
        for model in apps.get_models():
            if model.__name__ == self.model_name:
                return model

        raise ValueError(f"Model com o nome '{self.model_name}' não encontrado.")
    
    def _create_new_records(self):
        model = self._get_model_by_name()

        try:
            # Chamar a task LoadLog para salvar o registro de log e obter o ID gerado
            load_log_task = LoadLogView(log=self.log, load=f"{model}")
            load_log_task.run()
            load_log_id = load_log_task.get_last_inserted_id()  # Aqui você obtém o ID gerado pela task LoadLog

            # Atualizar o campo que você deseja associar ao ID da LoadLog em cada objeto criado
            objs = [model(**vals) for vals in self.dataset.pipe(self.prepare_to_json).to_dict('records')]
            for obj in objs:
                obj.load_log_id = load_log_id

            # Usar bulk_create para criar todos os objetos de uma vez
            model.objects.bulk_create(objs=objs, batch_size=500)
            self.log['n_inserted'] = len(objs)
            self.log['message'] = "Dados Carregados com sucesso!"
        except Exception as e:
            # Tratar a exceção em caso de falha na criação de registros
            self.log['create_error'] = str(e)