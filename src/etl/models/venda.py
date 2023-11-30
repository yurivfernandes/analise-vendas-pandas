from django.db import models
from .load_log import LoadLog
from django.utils import timezone

class Venda(models.Model):
    data = models.DateTimeField(default=timezone.now)
    nfe = models.IntegerField(null=False)
    codigo_produto = models.TextField(null= False, blank= False)
    codigo_vendedor = models.TextField(null= False, blank= False)
    equipe_vendas = models.TextField(null= False, blank= False)
    quantidade = models.IntegerField(null=False)
    preco_unitario = models.DecimalField(max_digits=11, decimal_places=2, default=0, null = False)
    load_log = models.ForeignKey(LoadLog, on_delete=models.SET_NULL, null=True)

    class Meta:
        db_table = 'elt_venda'
        unique_together = ['data','nfe','codigo_produto']
        verbose_name = 'Histórico de vendas'
        verbose_name_plural = 'Histórico de vendas'