import streamlit as st
import pandas as pd
import plotly.graph_objects as go


# ======= LAYOUT =======

st.set_page_config(layout='wide')


#### Sidebar
st.sidebar.header("Operadoras de saúde")

## RANKING DAS 15 MELHORES OPERADORAS - POR BENEFICIÁRIO
operadoras = [368253, 359017, 5711, 326305, 6246, 339679, 343889, 701, 393321, 352501, 304701, 346659, 348520, 302147, 335690]
# operadoras = sorted(operadoras)

st.sidebar.markdown('''
---
Created by [Matheus A. Cantarutti](https://www.linkedin.com/in/matheusalmeidacantarutti/).
''')

# with open('style.css') as f:
#     st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


# ### Dashboard
st.markdown('### Indicadores Carteira')

col1, col2, col3 = st.columns(3)
col4, col5, col6 = st.columns(3)


# ======= LAYOUT =======

if st.sidebar.selectbox('Selecione a Operadora', index=0, options=operadoras) == operadoras[0]:
    df = pd.read_csv("dados Selecionados/dados_hapvida.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=1, options=operadoras) == operadoras[1]:
    df = pd.read_csv("dados Selecionados/dados_notredame.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=2, options=operadoras) == operadoras[2]:
    df = pd.read_csv("dados Selecionados/dados_bradesco.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=3, options=operadoras) == operadoras[3]:
    df = pd.read_csv("dados Selecionados/dados_amilassistenciamedica.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=4, options=operadoras) == operadoras[4]:
    df = pd.read_csv("dados Selecionados/dados_sulamerica.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=5, options=operadoras) == operadoras[5]:
    df = pd.read_csv("dados Selecionados/dados_unimednacional.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=6, options=operadoras) == operadoras[6]:
    df = pd.read_csv("dados Selecionados/dados_unimedbelohorizonte.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=7, options=operadoras) == operadoras[7]:
    df = pd.read_csv("dados Selecionados/dados_unimedsegurossaude.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=8, options=operadoras) == operadoras[8]:
    df = pd.read_csv("dados Selecionados/dados_unimedrio.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=9, options=operadoras) == operadoras[9]:
    df = pd.read_csv("dados Selecionados/dados_unimedportoalegre.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=10, options=operadoras) == operadoras[10]:
    df = pd.read_csv("dados Selecionados/dados_unimedcuritiba.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=11, options=operadoras) == operadoras[11]:
    df = pd.read_csv("dados Selecionados/dados_caixadeassistencia.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=12, options=operadoras) == operadoras[12]:
    df = pd.read_csv("dados Selecionados/dados_notredameminas.csv", sep=",")

elif st.sidebar.selectbox('Selecione a Operadora', index=13, options=operadoras) == operadoras[13]:
    df = pd.read_csv("dados Selecionados/dados_preventsenior.csv", sep=",")

else:
    df = pd.read_csv("dados Selecionados/dados_unimedcampinas.csv", sep=",")


df.drop('Unnamed: 0', axis=1, inplace=True)

Idosos = float((df['P60'].values / df['Beneficiários Totais'].values) * 100) 
RazaoDependencia = float(((df['P15'].values + df['P60'].values) / df['P1559'].values) * 100) 
IndiceEnvelhecimento = float((df['P60'].values / df['P15'].values) * 100)
MediaAnos = float(df['IDADE MÉDIA'].values)
BENEFICIARIOS_COLETIVO = float((df['TOTAL EM COLETIVO'].values / df['TOTAL'].values) * 100)

TIPO_BENEFICIARIOS = ["COLETIVO EMPRESARIAL", "INDIVIDUAL OU FAMILIAR", "COLETIVO POR ADESÃO"]
COUNT_BENEFICIARIOS = [float(df['COLETIVO EMPRESARIAL'].values), float(df['INDIVIDUAL OU FAMILIAR'].values), float(df['COLETIVO ADESÃO'].values)]


fig = go.Figure()
fig.add_trace(go.Pie(
    labels=TIPO_BENEFICIARIOS, values=COUNT_BENEFICIARIOS, hole=.0
))

col1, col2, col3 = st.columns((2.5,2.5,2.5))

    # Linha 1
with col1:
    col1.metric(label="Percentual de Idosos", value=f'{Idosos:.1f}'"%")
with col2:
    col2.metric(label="Razão de Dependência", value=f'{RazaoDependencia:.1f}'"%")
with col3:
    col3.metric(label="Índice de Envelhecimento", value=f'{IndiceEnvelhecimento:.1f}'"%")

    # Linha 2
col4, col5, col6 = st.columns((2.5,2.5,2.5))
        
with col4:
    col4.metric(label="Idade Média", value=f'{MediaAnos:.1f}'" Anos")
with col5:
    st.markdown("### Beneficiários em Planos Coletivos")
    col5.metric(label="Beneficiários em Planos coletivos", value=f'{BENEFICIARIOS_COLETIVO:.1f}'"%")
with col6:
    col6.plotly_chart(fig)


