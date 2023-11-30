from django.db import models
from etl.models import LoadLog, Vendedor, Produto
from django.utils import timezone

class FatoVenda(models.Model):
    data = models.DateTimeField(default=timezone.now)
    nfe = models.IntegerField(null=False)
    produto = models.ForeignKey(Produto, on_delete=models.CASCADE)
    vendedor = models.ForeignKey(Vendedor, on_delete=models.CASCADE)
    equipe_vendas = models.TextField(null= False, blank= False)
    quantidade = models.IntegerField(null=False)
    preco_unitario = models.DecimalField(max_digits=11, decimal_places=2, default=0, null = False)
    load_log = models.ForeignKey(LoadLog, on_delete=models.SET_NULL, null=True)

    class Meta:
        db_table = 'fato_venda'
        unique_together = ['data','nfe','produto']
        verbose_name = 'Histórico de vendas'
        verbose_name_plural = 'Histórico de vendas'