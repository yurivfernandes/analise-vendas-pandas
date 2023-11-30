import unittest
import pandas as pd
from datetime import datetime
from app.utils.DataFrameGenerics import DataFrameGenerics
from etl.models import Produto 
from rest_framework.views import APIView
from ..tasks import UploadArquivos 



class TestUploadArquivos(unittest.TestCase, APIView):

    def setUp(self):
        # Dados de teste
        data = {
            "ID": [1, 2, 3],
            "NOME": ["João", "Maria", "Pedro"],
            "IDADE": [30, 25, 40],
            "DATA_NASCIMENTO": ["1991-01-01", "1996-05-15", "1981-11-30"]
        }
        self.sheet = pd.DataFrame(data)
        self.tabela = "Produto"  # Substitua pelo nome correto do modelo

    def test_run_method(self):
        # Testa se o método run é executado sem erros
        uploader = UploadArquivos(self.sheet, self.tabela)
        log = uploader.run()
        self.assertIsInstance(log, dict)

    def test_get_campos_model(self):
        # Testa se o método _get_campos_model encontra os campos corretos do modelo
        uploader = UploadArquivos(self.sheet, self.tabela)
        uploader._get_campos_model()
        self.assertIsNotNone(uploader.fields)

    def test_set_dataset(self):
        # Testa se o método _set_dataset remove colunas vazias
        uploader = UploadArquivos(self.sheet, self.tabela)
        uploader._set_dataset()
        self.assertNotIn('UNNAMED', uploader.dataset.columns)

    def test_transform_data(self):
        # Testa se o método _transform_data converte as colunas corretamente
        uploader = UploadArquivos(self.sheet, self.tabela)
        uploader._get_campos_model()
        uploader._set_dataset()
        uploader._transform_data()

        for field_info in uploader.fields.values():
            col_lower = field_info['name'].lower()
            matching_cols = [col for col in uploader.dataset.columns if col.lower() == col_lower]

            for col in matching_cols:
                dtype = uploader.dataset[col].dtype
                if 'int' in field_info['type']:
                    self.assertIn(dtype, [int, pd.Int64Dtype()])
                elif 'float' in field_info['type']:
                    self.assertIn(dtype, [float])
                elif 'bool' in field_info['type']:
                    self.assertIn(dtype, [bool])
                elif 'date' in field_info['type'].lower() or 'time' in field_info['type'].lower():
                    self.assertIn(dtype, [datetime])

    def test_delete_old_records(self):
        # Testa se o método _delete_old_records deleta registros do modelo
        uploader = UploadArquivos(self.sheet, self.tabela)
        uploader._delete_old_records()
        self.assertEqual(uploader.log.get('n_deleted', 0), 0)

    def test_create_new_records(self):
        # Testa se o método _create_new_records cria registros no modelo
        uploader = UploadArquivos(self.sheet, self.tabela)
        uploader._get_campos_model()
        uploader._set_dataset()
        uploader._transform_data()
        uploader._create_new_records()

        model = getattr(Produto, self.tabela)
        self.assertEqual(model.objects.count(), len(uploader.dataset))

        for row in uploader.dataset.to_dict('records'):
            obj = model.objects.filter(**row)
            self.assertTrue(obj.exists())

if __name__ == '__main__':
    unittest.main()