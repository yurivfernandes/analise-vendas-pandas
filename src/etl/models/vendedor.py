from django.db import models
from .load_log import LoadLog

class Vendedor(models.Model):
    codigo = models.TextField(null= False, blank= False)
    vendedor = models.TextField(null= False, blank= False)
    equipe_vendas = models.TextField(null= False, blank= False)
    foto = models.URLField()
    load_log = models.ForeignKey(LoadLog, on_delete=models.SET_NULL, null=True)

    class Meta:
        db_table = 'elt_vendedor'
        unique_together = ['codigo', 'equipe_vendas']
        verbose_name = 'Cadastro de Vendedores'
        verbose_name_plural = 'Cadastro de Vendedores'