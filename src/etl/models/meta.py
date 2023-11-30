from django.db import models
from .load_log import LoadLog

class Meta(models.Model):
    mes = models.TextField(null= False, blank= False)
    ano = models.IntegerField(null=False)
    vendedor = models.TextField(null= False, blank= False)
    faturamento = models.DecimalField(max_digits=11, decimal_places=2, default=0, null = False)
    margem_bruta = models.DecimalField(max_digits=11, decimal_places=2, default=0, null = False)
    notas_emitidas = models.IntegerField(null=False)
    load_log = models.ForeignKey(LoadLog, on_delete=models.SET_NULL, null=True)

    class Meta:
        db_table = 'elt_meta'
        unique_together = ['mes', 'ano','vendedor']
        verbose_name = 'Cadastro de Metas'
        verbose_name_plural = 'Cadastro de Metass'