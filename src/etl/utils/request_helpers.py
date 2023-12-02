import pandas as pd
from datetime import datetime
from rest_framework.request import Request

def fetch_data_compound_request(request: Request) -> dict:
    """Helper que trata uma requisição composta de datas e 
        retorna data inicio e data fim"""
    
    data_inicio = None
    data_fim = None
    ponto_facultativo = []
    if request.data.get('data_inicio',[]):
        data_inicio = request.data.get('data_inicio')

    if request.data.get('data_fim',[]):
        data_fim = request.data.get('data_fim')
    
    if request.data.get('ano_inicio',[]):
        data_inicio = str(request.data.get('ano_inicio'))+"-01-01"
    
    if request.data.get('ano_fim',[]):
        data_fim = str(request.data.get('ano_fim'))+"-12-31"
    
    if request.data.get('mes_ano_inicio',[]):
        data_inicio = str(request.data.get('mes_ano_inicio'))+"-01"
    
    if request.data.get('mes_ano_fim',[]):
        data_fim = str(request.data.get('mes_ano_inicio'))+"-31"
    
    if request.data.get('ponto_facultativo',[]):
        ponto_facultativo = request.data.get('ponto_facultativo')

    if data_inicio == None:
        data_inicio = str(datetime.now().replace(day=1, month=1))
    
    if data_fim == None:
        data_fim = str(datetime.now().replace(day=31, month=12))
    
    return {
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "ponto_facultativo": ponto_facultativo}