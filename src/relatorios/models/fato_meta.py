from django.db import models
from etl.models import LoadLog, Vendedor
from django.utils import timezone

class FatoMeta(models.Model):
    data = models.DateTimeField(default=timezone.now)
    vendedor = models.ForeignKey(Vendedor, on_delete=models.CASCADE)
    faturamento = models.DecimalField(max_digits=11, decimal_places=2, default=0, null = False)
    margem_bruta = models.DecimalField(max_digits=11, decimal_places=2, default=0, null = False)
    notas_emitidas = models.IntegerField(null=False)
    load_log = models.ForeignKey(LoadLog, on_delete=models.SET_NULL, null=True)

    class Meta:
        db_table = 'fato_meta'
        unique_together = ['data','vendedor']
        verbose_name = 'Cadastro de Metas'
        verbose_name_plural = 'Cadastro de Metas'