import json
from app.utils.DataFrameGenerics import DataFrameGenerics
from django.utils import timezone
from traceback import format_tb
from etl.models import LoadLog

class LoadPipeline(DataFrameGenerics):
    """Classe genérica com métodos comuns aos processos de carga automatizados relacionados \
        aos apps do [planejamento]"""
    def __init__(self,**kwargs) -> None:
        self.log = {
            "started_at": timezone.now(),
            "finished_at": None,
            "n_inserted": 0,
            "n_deleted": 0,
            "n_updated": 0,
            "details": None
        }
        self.load = None
        self.params: dict = {}
        super().__init__()
        pass

    def __enter__(self):
        self.load = LoadLog(
            started_at=timezone.now(),
            params=json.dumps(self.params, ensure_ascii=False),
            load=self.__class__.__name__)
        self.load.save()
        self.log["load"] = self.load.id
        return self

    def __exit__(self, type, value, traceback):
        self.log["finished_at"] = timezone.now()
        if type:
            self.log["details"] = "Load failed: {}\n{}\n".format(
                str(value),# + " - " + value.__repr__(),
                "".join(format_tb(traceback)),
            )
        for k, v in self.log.items():
            if k == "load":
                continue
            setattr(self.load, k, v)
        self.load.save()

    def terminate_log(self, **kwargs) -> None:
        for k,v in kwargs.items():
            if k in self.log:
                self.log[k] = v
        self.log["finished_at"] = timezone.now()