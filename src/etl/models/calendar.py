from django.db import models
from .load_log import LoadLog

class Calendar(models.Model):
    data = models.DateTimeField()
    feriado = models.CharField(max_length=100, null= True, blank= True)
    dia_semana = models.CharField(max_length=10, null= False, blank= False)
    dia_util = models.BooleanField(default=False, db_index=True)
    ponto_facultativo = models.BooleanField(default=False, db_index=True)
    ano = models.IntegerField(null=True)
    mes = models.IntegerField(null=True)
    dia = models.IntegerField(null=True)
    mes_ano = models.CharField(max_length=50)
    trimestre = models.CharField(max_length=2)
    load_log = models.ForeignKey(LoadLog, on_delete=models.SET_NULL, null=True)

    class Meta:
        db_table = 'elt_calendar'
        verbose_name = 'Cadadario'