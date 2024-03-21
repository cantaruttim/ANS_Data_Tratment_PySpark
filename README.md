# Dashoard Health - Python

<hr >

## Objetivo

<hr >

O Objetivo principal desse projeto era realizar a apresentação de indicadores de saúde, contidos nas bases da ANS, para que pudessemos analisar e apresentá-los via Python utilizando a biblioteca  `streamlit`. Por meio desta biblioteca, podemos montar o layout da página web onde o dashboard seria apresentado.

## Dados

<hr >

Os dados utilizados estão contidos no site https://dadosabertos.ans.gov.br/FTP/PDA/
Os arquivos utilizados foram:

1. Produtos e prestadores hospitalares: https://dadosabertos.ans.gov.br/FTP/PDA/produtos_e_prestadores_hospitalares/

2. Dados de Beneficiários por Operadora: https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/

## Desafios

<hr >
Sem sombra de dúvidas, a quantidade de dados aqui foi um grande desafio. O que me forçou a trabalhar com o `pyspark` um framework baseado em `java` e `scala`, que realizou a consulta dos dados para que o dashboard pudesse ser criado. O que me fez aprender um pouco sobre como trabalhar de forma real com o spark. Além de conseguir me ajustar a demanda de dados disponibilizada pela ANS das operadoras de saúde.
