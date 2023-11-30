from django.db import models
from .load_log import LoadLog

class Produto(models.Model):
    codigo = models.TextField(null= False, blank= False)
    grupo = models.TextField(null= False, blank= False)
    linha = models.TextField(null= False, blank= False)
    fornecedor = models.TextField(null= False, blank= False)
    custo = models.DecimalField(max_digits=11, decimal_places=2, default=0, null = False)
    load_log = models.ForeignKey(LoadLog, on_delete=models.SET_NULL, null=True)

    class Meta:
        db_table = 'elt_produto'
        unique_together = ['codigo', 'grupo','linha','fornecedor']
        verbose_name = 'Cadastro de Produto'
        verbose_name_plural = 'Cadastro de Produtos'