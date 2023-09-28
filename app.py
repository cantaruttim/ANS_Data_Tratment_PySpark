from glob import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

## ========== CONFIGURAÇÕES ==========

tab_card = {'height': '100%'}

config_graph={"displayModeBar": False, "showTips": False}


spark = SparkSession.builder.appName("Operadoras").getOrCreate()
# spark

## ========== LENDO ARQUIVOS ==========

# lendo todos os arquivos dos beneficiários ativos com Spark
beneficiarios_ativos_estados = sorted(glob(r'dadosFTP/SIB/Ativos/*.csv'))
beneficiariosAtivos = spark.read.option('header', 'true').\
    csv(beneficiarios_ativos_estados,
    inferSchema=True, sep=";")

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
PP_HOSPITALARES = PP_HOSPITALARES.select("ID_PLANO", "CD_OPERADORA", "CONTRATACAO", "DE_TIPO_CONTRATACAO")

### ==================== DASH ====================

# tentativa com o dash
import dash
from dash import dcc
import dash_core_components as html
from dash.dependencies import Input, Output, ClientsideFunction
import dash_bootstrap_components as dbc

import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt

## Populações & Indicadores
### CÁLCULOS COM BASE NA METODOLOGIA PROPOSTA EM https://www.gov.br/ans/pt-br/arquivos/acesso-a-informacao/perfil-do-setor/dados-e-indicadores-do-setor/informacoes-gerais/manual-sala-de-situacao.pdf

##Exemplo BRADESCO 005711
operadora = 5711  ## ====>>> MUDE APENAS ESTE PARÂMETRO!
tamanhoOperadora = tb_idade_anos.filter(tb_idade_anos.CD_OPERADORA == operadora).count() # Total de beneficiários da operadora
tamanhoHOSPITALARES = PP_HOSPITALARES.count() # 37.944.888

### Até 15 anos
P15 = tb_idade_anos.filter(tb_idade_anos.Anos <= 15) \
    .where(tb_idade_anos.CD_OPERADORA == operadora).count() # 702.840

### 60 anos ou mais
P60 = tb_idade_anos.filter(tb_idade_anos.Anos >= 60) \
    .where(tb_idade_anos.CD_OPERADORA == operadora).count() # 285.620

### Entre 15 e 59 anos
P1559 = tb_idade_anos.filter((tb_idade_anos.Anos >= 15) & (tb_idade_anos.Anos <= 59)) \
    .where(tb_idade_anos.CD_OPERADORA == operadora).count() # 2.361.728

#### BENEFICIÁRIOS POR CONTRATAÇÃO
## PROPORÇÃO DE BENEFICIÁRIOS POR CONTRATAÇÃO

# COLETIVO EMPRESARIAl
COLETIVO_EMPRESARIAL = PP_HOSPITALARES.filter(PP_HOSPITALARES.DE_TIPO_CONTRATACAO == "COLETIVO EMPRESARIAL") \
    .where(PP_HOSPITALARES.CD_OPERADORA == operadora).count() # 916.006

# INDIVIDUAL OU FAMILIAR
INDIVIDUAL_FAMILIAR = PP_HOSPITALARES.filter(PP_HOSPITALARES.DE_TIPO_CONTRATACAO == "INDIVIDUAL OU FAMILIAR") \
    .where(PP_HOSPITALARES.CD_OPERADORA == operadora).count() # 79.582 

# COLETIVO POR ADESAO
COLETIVO_ADESAO = PP_HOSPITALARES.filter(PP_HOSPITALARES.DE_TIPO_CONTRATACAO == "COLETIVO POR ADESÃO") \
    .where(PP_HOSPITALARES.CD_OPERADORA == operadora).count() # 00


TIPO_BENEFICIARIOS = ["COLETIVO EMPRESARIAL", "INDIVIDUAL OU FAMILIAR", "COLETIVO POR ADESÃO"]
COUNT_BENEFICIARIOS = [COLETIVO_EMPRESARIAL, INDIVIDUAL_FAMILIAR, COLETIVO_ADESAO]

fig5 = px.pie(values=COUNT_BENEFICIARIOS, names=TIPO_BENEFICIARIOS)
fig5.show()

## CÁLCULOS DOS INDICADORES
Idosos = (P60 / tamanhoOperadora) * 100 # 8.62%

RazaoDependencia = ((P15 + P60) / P1559) * 100 # 41.85

IndiceEnvelhecimento = (P60 / P15) * 100 # 40.64%

MediaAnos = tb_idade_anos.select(tb_idade_anos['Anos']).summary('mean').collect()[0]['Anos']
MediaAnos = float(MediaAnos)


# ======= GRÁFICOS =======

fig1 = go.Figure()
fig1.add_trace(go.Indicator(mode='number+delta',
        title= {"text": f"<span style='font-size:85%'>Percentual de Idosos<span><br>"},
        value= Idosos,
        number= {'suffix':'%'},
        delta= {'relative':True, 'valueformat':'.1%', 'reference':Idosos}               
))


fig2 = go.Figure()
fig2.add_trace(go.Indicator(mode='number+delta',
        title= {"text": f"<span style='font-size:85%'>Razão de Dependência<span><br>"},
        value= RazaoDependencia,
        number= {'suffix':'%'},
        delta= {'relative':True, 'valueformat':'.1%', 'reference':RazaoDependencia}               
))


fig3 = go.Figure()
fig3.add_trace(go.Indicator(mode='number+delta',
        title= {"text": f"<span style='font-size:85%'>Índice de Envelhecimento<span><br>"},
        value= IndiceEnvelhecimento,
        number= {'suffix':'%'},
        delta= {'relative':True, 'valueformat':'.1%', 'reference':IndiceEnvelhecimento}               
))

fig4 = go.Figure()
fig4.add_trace(go.Indicator(mode='number+delta',
        title= {"text": f"<span style='font-size:85%'>Idade Média<span><br>"},
        value= MediaAnos,
        number= {'suffix':' Anos'},
        delta= {'relative':True, 'valueformat':'.1%', 'reference':MediaAnos}               
))


fig5 = go.Figure()
fig5.add_trace(go.Pie(
    labels=TIPO_BENEFICIARIOS, values=COUNT_BENEFICIARIOS, hole=.0
    ))


# ======= LAYOUT ======= 

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

app.layout = dbc.Container(

    # Main Row
    dbc.Row([

        # Row 1
        dbc.Row([

            # Coluna 1.1
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='card1',  figure=fig1, className='dbc', config=config_graph)
                    ]),
                ], style=tab_card),
            ]),

            # Coluna 1.2
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='card2', figure=fig2, className='dbc', config=config_graph)
                    ]),
                ], style=tab_card),
            ]),

            # Col 1.3
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='card3', figure=fig3, className='dbc', config=config_graph)
                    ]),
                ], style=tab_card),
            ]),
        ]), # Fim da Row 1

        # Row 2
        dbc.Row([

            # Col 2.1
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='card4', figure=fig4, className='dbc', config=config_graph)
                    ]),
                ], style=tab_card),
            ]),

            # Col 2.2
            dbc.Col([

                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='card5', className='dbc', figure=fig5, config=config_graph)
                    ]),
                ], style=tab_card),
            ]),
            
            # Col 3
            dbc.Col([
                
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='card6', className='dbc', config=config_graph)
                    ]),
                ],style=tab_card),
            ]),

        ]), # fim da Row 2

    ])

)


if __name__ == "__main__":
    app.run_server(debug=False)