# ANP-Pipeline
Esse repositório tem como intuito criar um pipeline de dados para consumir os dados do site da Agência Nacional do Petróleo (ANP).


## Como executar
Para executar este pipeline, é necessário conter:

    - Docker;
    - Docker-compose.

Com as ferramentas necessárias e na pasta raiz deste repositório, execute o seguinte comando:
`docker-compose up --build`

## Agendamento
Para agendar o processo para executar mais de uma vez, por exemplo, a cada 10 minutos, altere o CRONTAB no arquivo python/Dockerfile


## Testes 
Os testes realizados, verificam se os dados salvos nas tabelas do banco de dados estão consistentes com os dados crus.
Os dados crus estão em: https://github.com/iurizambotto/anp-pipeline/raw/main/data/vendas-combustiveis-m3.xls

Para verificar os testes, acesse: `python/tests/`

