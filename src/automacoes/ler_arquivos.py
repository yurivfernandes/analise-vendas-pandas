import os
import pandas as pd

def contar_arquivos_pdf(caminho_pasta):
    # Inicializa um dicionário para armazenar os resultados
    resultados = {}
    total_arquivos = 0  # Adiciona uma variável para rastrear o total de arquivos

    # Percorre os diretórios e arquivos no caminho especificado
    for pasta_atual, subpastas, arquivos in os.walk(caminho_pasta):
        if not subpastas:  # Verifica se não há mais subpastas
            # Extrai o nome do cliente e do período da última pasta
            cliente, periodo = os.path.split(pasta_atual)[-2:]

            # Conta os arquivos com extensão '.pdf' na pasta atual
            qtd_pdfs = sum(1 for arquivo in arquivos if arquivo.lower().endswith('.pdf'))

            # Adiciona o resultado ao dicionário
            resultados[f'{cliente} - {periodo}'] = qtd_pdfs
            total_arquivos += qtd_pdfs  # Incrementa o total de arquivos

    # Adiciona uma entrada ao dicionário para o total de arquivos
    resultados['Total de Arquivos'] = total_arquivos

    return resultados

def salvar_em_excel(resultados: dict, caminho_arquivo: str, nome_arquivo: str):
    # Converte o dicionário para um DataFrame do pandas
    df = pd.DataFrame(list(resultados.items()), columns=['Nome da Pasta', 'Quantidade de PDFs'])

    # Combina o caminho do diretório com o nome do arquivo
    caminho_arquivo = os.path.join(caminho_arquivo, nome_arquivo)

    # Salva o DataFrame em um arquivo Excel
    df.to_excel(caminho_arquivo, index=False)
    print(f'Dados salvos em {caminho_arquivo}')

# Caminho do diretório que você quer analisar
caminho_do_diretorio = r'C:\Users\YuriVianaFernandes\Documents\Docs Sacaneados'
# Chama a função para contar os arquivos PDF
resultados = contar_arquivos_pdf(caminho_do_diretorio)

# Nome do arquivo Excel de saída
caminho_salvar_arquivo = r'C:\Users\YuriVianaFernandes\Documents\Docs Sacaneados'
salvar_em_excel(
    resultados=resultados,
    caminho_arquivo=caminho_salvar_arquivo,
    nome_arquivo="resultado.xlsx")