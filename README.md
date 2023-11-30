# analise-vendas-pandas
Desenvolvi um projeto abrangente de análise de vendas em Python, utilizando as renomadas bibliotecas pandas e numpy. Destaco a implementação de uma API que desempenha um papel crucial no processo de ETL (Extração, Transformação e Carga) de dados. Esta API recebe do frontend o nome da tabela do banco de dados e um arquivo, realizando o ETL de forma eficiente. Durante esse processo, os dados são testados de acordo com os tipos definidos nos modelos (models) antes de serem persistidos no banco de dados.

O ecossistema do projeto é enriquecido com uma API dedicada à execução de testes unitários, complementada pelas models personalizadas que desenvolvi para receber e validar os dados. Para garantir a rastreabilidade e monitoramento, implementei a funcionalidade de registro (log), gravando informações essenciais em uma tabela do banco de dados.

Além disso, destaco a presença de um módulo de relatórios que oferece uma análise detalhada por equipe de vendas, produto, vendas e vendedor. Esse módulo proporciona uma visão abrangente das métricas essenciais para o negócio.

No âmbito da automação, o projeto inclui duas tarefas configuradas para utilizar o Celery, prontas para efetuar cargas nas tabelas "fato meta" e "venda". Essas tarefas são desenhadas para operar de maneira assíncrona, melhorando a eficiência e a escalabilidade do sistema.

Este projeto serve como um exemplo representativo das minhas habilidades em Python, destacando a capacidade de integrar bibliotecas poderosas para análise de dados, criar APIs robustas para manipulação e validação de dados, e implementar tarefas assíncronas para automação eficiente.