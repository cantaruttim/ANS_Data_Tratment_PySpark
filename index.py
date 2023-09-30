from glob import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

## ========== CONFIGURAÇÕES ==========

spark = SparkSession.builder.appName("Operadoras").getOrCreate()

## ========== LENDO ARQUIVOS ==========

# lendo todos os arquivos dos beneficiários ativos com Spark
beneficiarios_ativos_estados = sorted(glob(r'dadosFTP/SIB/Ativos/*.csv'))
beneficiariosAtivos = spark.read.option('header', 'true').\
    csv(beneficiarios_ativos_estados,
    inferSchema=True, sep=";")


# Filtro o dataframe com os dados que estão vazios para o motivo de cancelamento
Beneficiarios_Ativos_filtro = beneficiariosAtivos.filter(beneficiariosAtivos.CD_BENE_MOTIV_CANCELAMENTO.isNull())
#beneficiariosAtivos.show()

# lendo os arquivos dos prestadores hospitalares com Spark
PP_HOSPITALARES_FILES = sorted(glob(r'dadosFTP/Produtos e Prestadores Hospitalares/*.csv'))
PP_HOSPITALARES = spark.read.option('header', 'true').\
    csv(PP_HOSPITALARES_FILES,
    inferSchema=True, sep=";")


## adicionando a quantidade de anos
beneficiariosAtivos = beneficiariosAtivos.withColumn("Data_Atual", lit( current_date() ))

# transformando para string as duas colunas
beneficiariosAtivos = beneficiariosAtivos.withColumn("Data_Atual", col("Data_Atual").cast("string"))
beneficiariosAtivos = beneficiariosAtivos.withColumn("DT_NASCIMENT0", col("DT_NASCIMENTO").cast("string")) 

#extrair o ano da coluna DATA_ANO
beneficiariosAtivos = beneficiariosAtivos.withColumn("DATA_ANO", lit(date_format('Data_Atual', 'yyy').alias('DATA_ANO')))
beneficiariosAtivos = beneficiariosAtivos.withColumn("DT_NASCIMENT0", col("DT_NASCIMENTO").cast("integer")) 
beneficiariosAtivos = beneficiariosAtivos.withColumn("DATA_ANO", col("DATA_ANO").cast("integer"))

# ANOS DE VIDA
tb_idade_anos = beneficiariosAtivos.select("CD_OPERADORA", "TP_SEXO", "DATA_ANO", "DT_NASCIMENTO")
tb_idade_anos = tb_idade_anos.withColumn("Anos", col("DATA_ANO") - col("DT_NASCIMENTO") )

# tamanho do dataframe
tamanho = tb_idade_anos.count()


#### TRATAMENTO DA TABELA DE PP_HOSPITALARES
# alterando o tipo de string para inteiro da coluna CD_OPERADORA
PP_HOSPITALARES = PP_HOSPITALARES.withColumn("CD_OPERADORA", col("CD_OPERADORA").cast("integer"))

                                            #PK              
PP_HOSPITALARES = PP_HOSPITALARES.select("ID_PLANO", "CD_OPERADORA", "CONTRATACAO", "DE_TIPO_CONTRATACAO", "SG_UF")

# PP_HOSPITALARES.count() # 37.944.888

### ==================== DASHBOARD ====================

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

## Populações & Indicadores

## retirando os valores das operadoas
operadoras = tb_idade_anos.select("CD_OPERADORA")
# Converta a coluna em uma lista de valores
valores_operadoras = [row.CD_OPERADORA for row in operadoras.distinct().collect()]
valores_operadoras = set(valores_operadoras) # conjunto set
valores_operadoras = sorted(list(valores_operadoras)) # converte em lista de forma crescente

operadoras = {"Operadoras":valores_operadoras}

df = pd.DataFrame(operadoras)

operadora = df['Operadoras'][6]
tamanhoOperadora = tb_idade_anos.filter(tb_idade_anos.CD_OPERADORA == operadora).count() # Total de beneficiários da operadora
tamanhoHOSPITALARES = PP_HOSPITALARES.count() # 37.944.888

### CÁLCULOS COM BASE NA METODOLOGIA PROPOSTA EM https://www.gov.br/ans/pt-br/arquivos/acesso-a-informacao/perfil-do-setor/dados-e-indicadores-do-setor/informacoes-gerais/manual-sala-de-situacao.pdf
### Até 15 anos
P15 = tb_idade_anos.filter(tb_idade_anos.Anos <= 15) \
    .where(tb_idade_anos.CD_OPERADORA == operadora).count() # Bradesco =  702.840

### 60 anos ou mais
P60 = tb_idade_anos.filter(tb_idade_anos.Anos >= 60) \
    .where(tb_idade_anos.CD_OPERADORA == operadora).count() # Bradesco =  285.620

### Entre 15 e 59 anos
P1559 = tb_idade_anos.filter((tb_idade_anos.Anos >= 15) & (tb_idade_anos.Anos <= 59)) \
    .where(tb_idade_anos.CD_OPERADORA == operadora).count() # Bradesco =  2.361.728

#### BENEFICIÁRIOS POR CONTRATAÇÃO
## PROPORÇÃO DE BENEFICIÁRIOS POR CONTRATAÇÃO

# COLETIVO EMPRESARIAl
COLETIVO_EMPRESARIAL = PP_HOSPITALARES.filter(PP_HOSPITALARES.DE_TIPO_CONTRATACAO == "COLETIVO EMPRESARIAL") \
    .where(PP_HOSPITALARES.CD_OPERADORA == operadora).count() # Bradesco = 916.006

# INDIVIDUAL OU FAMILIAR
INDIVIDUAL_FAMILIAR = PP_HOSPITALARES.filter(PP_HOSPITALARES.DE_TIPO_CONTRATACAO == "INDIVIDUAL OU FAMILIAR") \
    .where(PP_HOSPITALARES.CD_OPERADORA == operadora).count() # Bradesco = 79.582 

# COLETIVO POR ADESAO
COLETIVO_ADESAO = PP_HOSPITALARES.filter(PP_HOSPITALARES.DE_TIPO_CONTRATACAO == "COLETIVO POR ADESÃO") \
    .where(PP_HOSPITALARES.CD_OPERADORA == operadora).count() # Bradesco = 00


TIPO_BENEFICIARIOS = ["COLETIVO EMPRESARIAL", "INDIVIDUAL OU FAMILIAR", "COLETIVO POR ADESÃO"]
COUNT_BENEFICIARIOS = [COLETIVO_EMPRESARIAL, INDIVIDUAL_FAMILIAR, COLETIVO_ADESAO]

#### Beneficiários em Planos Coletivos

## em relação aquela operadora, qual o percentual de beneficiários em planos coletivos?

COLETIVO = COLETIVO_EMPRESARIAL + COLETIVO_ADESAO # 
TOTAL = COLETIVO_EMPRESARIAL + INDIVIDUAL_FAMILIAR + COLETIVO_ADESAO

BENEFICIARIOS_COLETIVO = (COLETIVO / TOTAL) * 100

## CÁLCULOS DOS INDICADORES
Idosos = (P60 / tamanhoOperadora) * 100 # 8.62

RazaoDependencia = ((P15 + P60) / P1559) * 100 # 41.85

IndiceEnvelhecimento = (P60 / P15) * 100 # 40.64

MediaAnos = tb_idade_anos.select(tb_idade_anos['Anos']).summary('mean').collect()[0]['Anos']
MediaAnos = float(MediaAnos)

# ====== GRÁFICO =======

fig = go.Figure()
fig.add_trace(go.Pie(
    labels=TIPO_BENEFICIARIOS, values=COUNT_BENEFICIARIOS, hole=.0
    ))



# ======= LAYOUT =======
import streamlit as st
import plost

#### Sidebar
st.set_page_config(layout='wide', initial_sidebar_state='expanded')


st.sidebar.header('Operadoras')
cd_operadoras = st.sidebar.selectbox('Selecione a Operadora',
                                     placeholder="Selecione a Operadora", 
                                     index=6, 
                                     options= df['Operadoras'].values
                                     )

st.sidebar.markdown('''
---
Created by [Matheus de Almeida Cantarutti](https://www.linkedin.com/in/matheusalmeidacantarutti/).
''')

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


### Dashboard
st.markdown('### Indicadores Carteira')

col1, col2, col3 = st.columns(3)
col4, col5, col6 = st.columns(3)

# Row 1
col1, col2, col3 = st.columns((2.5,2.5,2.5))
with col1:
    col1.metric(label="Percentual de Idosos", value=f'{Idosos:.1f}'"%")
with col2:
    col2.metric(label="Razão de Dependência", value=f'{RazaoDependencia:.1f}'"%")
with col3:
    col3.metric(label="Índice de Envelhecimento", value=f'{IndiceEnvelhecimento:.1f}'"%")

# Row 2
col4, col5, col6 = st.columns((2.5,2.5,2.5))
with col4:
    col4.metric(label="Idade Média", value=f'{MediaAnos:.1f}'" Anos")
with col5:
    st.markdown("### Beneficiários em Planos Coletivos")
    col5.metric(label="Beneficiários em Planos coletivos", value=f'{BENEFICIARIOS_COLETIVO:.1f}'"%")
with col6:
    col6.plotly_chart(fig)