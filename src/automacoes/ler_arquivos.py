import os
import pandas as pd

def contar_arquivos_pdf(caminho_pasta):
    resultados = {}
    total_arquivos = 0 
    for pasta_atual, subpastas, arquivos in os.walk(caminho_pasta):
        if not subpastas: 
            cliente, periodo = os.path.split(pasta_atual)[-2:]
            qtd_pdfs = sum(1 for arquivo in arquivos if arquivo.lower().endswith('.pdf'))
            resultados[f'{cliente} - {periodo}'] = qtd_pdfs
            total_arquivos += qtd_pdfs
    resultados['Total de Arquivos'] = total_arquivos
    return resultados

def salvar_em_excel(resultados: dict, caminho_arquivo: str, nome_arquivo: str):
    df = pd.DataFrame(list(resultados.items()), columns=['Nome da Pasta', 'Quantidade de PDFs'])
    caminho_arquivo = os.path.join(caminho_arquivo, nome_arquivo)
    df.to_excel(caminho_arquivo, index=False)
    print(f'Dados salvos em {caminho_arquivo}')

caminho_do_diretorio = r'C:\Users\YuriVianaFernandes\Documents\Docs Sacaneados'
resultados = contar_arquivos_pdf(caminho_do_diretorio)
caminho_salvar_arquivo = r'C:\Users\YuriVianaFernandes\Documents\Docs Sacaneados'

salvar_em_excel(
    resultados=resultados,
    caminho_arquivo=caminho_salvar_arquivo,
    nome_arquivo="resultado.xlsx")