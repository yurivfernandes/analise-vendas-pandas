from django.db import models
from django.utils import timezone

class LoadLog(models.Model):
    started_at = models.DateTimeField(default = timezone.now)
    finished_at  = models.DateTimeField(null = True)
    n_processed = models.IntegerField(null= True)
    n_inserted = models.IntegerField(null= True)
    n_deleted = models.IntegerField(null= True)
    n_updated = models.IntegerField(null= True)
    details = models.TextField(null= True, blank= True)
    params = models.CharField(max_length= 255, null= True)
    load = models.CharField(max_length=100, null=True, blank=True)
    
    class Meta:
        db_table = 'elt_load_log'
        verbose_name = 'log de processo de carga'
        verbose_name_plural = 'log de processos de cargas'