# -*- coding: utf-8 -*-
"""
analise_financeira_app.py

Este script implementa um aplicativo web interativo usando a biblioteca Streamlit
para análise financeira, incluindo controle de finanças pessoais, valuation de
empresas, modelos de saúde financeira (Fleuriet e Z-Score) e precificação de
opções pelo modelo de Black-Scholes com análise avançada.

O código foi revisado com base em um TCC sobre valuation que utiliza os modelos
EVA e EFV, bem como o modelo de Hamada para ajuste do beta.
Versão 21: Aprimora a clareza da interface na aba Black-Scholes. Adiciona uma
            coluna de "Interpretação para Leigos" na tabela de análise técnica
            e melhora a redação do glossário das Gregas para facilitar o
            entendimento por parte de todos os usuários.
"""

import os
import pandas as pd
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zipfile import ZipFile
from datetime import datetime, date
from pathlib import Path
import warnings
import numpy as np
import io
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError
from scipy.stats import norm
import pandas_ta as ta
import logging
from typing import Dict, Any

# Ignorar avisos para uma saída mais limpa
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURAÇÕES GERAIS E LAYOUT DA PÁGINA
# ==============================================================================
st.set_page_config(layout="wide", page_title="Painel de Controle Financeiro", page_icon="📈")

# Chave da API Alpha Vantage
ALPHA_VANTAGE_API_KEY = "G34YKVWF0XCPVMZV"

# Estilo CSS aprimorado para temas claro e escuro, com melhor UX
st.markdown("""
<style>
    /* 1. Definição de Variáveis de Cor para Tema Claro (Padrão) */
    :root {
        --primary-bg: #F0F2F6;
        --secondary-bg: #FFFFFF;
        --widget-bg: #FFFFFF;
        --primary-accent: #007BFF;
        --secondary-accent: #28a745;
        --positive-accent: #28a745;
        --negative-accent: #DC3545;
        --text-color: #212529;
        --header-color: #000000;
        --border-color: #DEE2E6;
        --tab-active-bg: #E9ECEF;
        --tab-inactive-text: #6C757D;
        --table-header-bg: #F8F9FA;
        --table-row-hover-bg: #F1F3F5;
    }

    /* 2. Sobrescrita das Variáveis para Tema Escuro */
    [data-theme="dark"] {
        --primary-bg: #0A0A1A;
        --secondary-bg: #1A1A2E;
        --widget-bg: #16213E;
        --primary-accent: #00F6FF;
        --secondary-accent: #39FF14;
        --positive-accent: #00FF87;
        --negative-accent: #FF5252;
        --text-color: #F8F9FA;
        --header-color: #FFFFFF;
        --border-color: #5372F0;
        --tab-active-bg: #323A52;
        --tab-inactive-text: #A0A4B8;
        --table-header-bg: #16213E;
        --table-row-hover-bg: #323A52;
    }

    /* 3. Estilos Gerais que usam as variáveis (funcionam para ambos os temas) */
    body {
        color: var(--text-color);
        background-color: var(--primary-bg);
    }

    .main.block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    h1, h2, h3 {
        color: var(--header-color);
    }

    /* Título com Gradiente Adaptativo */
    [data-theme="light"] h1 {
        background: -webkit-linear-gradient(45deg, #007BFF, #0056b3);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    [data-theme="dark"] h1 {
        background: -webkit-linear-gradient(45deg, var(--primary-accent), var(--positive-accent));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-shadow: 0 0 10px rgba(0, 246, 255, 0.3);
    }

    /* Abas */
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: transparent;
        border-bottom: 2px solid transparent;
        transition: all 0.3s;
        color: var(--tab-inactive-text);
    }
    .stTabs [aria-selected="true"] {
        color: var(--primary-accent);
        border-bottom: 2px solid var(--primary-accent);
        background-color: var(--tab-active-bg);
    }
    [data-theme="dark"] .stTabs [aria-selected="true"] {
        box-shadow: 0 2px 15px -5px var(--primary-accent);
    }

    /* Métricas */
    .stMetric {
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 20px;
        background-color: var(--secondary-bg);
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.05);
    }
    [data-theme="dark"] .stMetric {
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
    }
    .stMetric label { color: var(--text-color); }
    .stMetric > div[data-testid="stMetricValue"] { 
        color: var(--header-color) !important; 
        font-size: 2.25rem; /* Ajuste para caber melhor */
    }
    .stMetric > div[data-testid="stMetricDelta"] > div[data-testid="stMetricDelta"] {
        color: var(--positive-accent) !important;
    }
    .stMetric > div[data-testid="stMetricDelta"] > div[data-testid="stMetricDelta"].st-ae {
        color: var(--negative-accent) !important;
    }

    /* Botões */
    .stButton > button {
        border-radius: 8px;
        border: 1px solid var(--primary-accent);
        background-color: transparent;
        color: var(--primary-accent);
        transition: all 0.3s ease-in-out;
    }
    .stButton > button:hover {
        background-color: var(--primary-accent);
        color: var(--secondary-bg);
    }
    [data-theme="dark"] .stButton > button {
        box-shadow: 0 0 5px var(--primary-accent);
    }
    [data-theme="dark"] .stButton > button:hover {
        box-shadow: 0 0 20px var(--primary-accent);
    }

    /* Expanders e Formulários */
    [data-testid="stExpander"] {
        background-color: var(--secondary-bg);
        border: 1px solid var(--border-color);
        border-radius: 8px;
    }
    [data-testid="stExpander"] summary {
        font-size: 1.1em;
        font-weight: 600;
        color: var(--text-color) !important; /* CORREÇÃO DE COR */
    }
    
    /* Cor do texto geral e labels dos widgets */
    .stMarkdown, .stSelectbox > label, .stDateInput > label, .stNumberInput > label, .stTextInput > label, .stSlider > label, .stCheckbox > label {
        color: var(--text-color) !important; /* CORREÇÃO DE COR */
    }
    
    /* Estilização de Tabelas (st.dataframe, st.table, st.data_editor) */
    .stDataFrame, .stTable {
        border: 1px solid var(--border-color);
        border-radius: 8px;
        overflow: hidden; /* Garante que o border-radius seja aplicado nos cantos */
    }
    .stDataFrame thead, .stTable thead {
        background-color: var(--table-header-bg);
    }
    .stDataFrame th, .stTable th {
        color: var(--header-color);
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .stDataFrame tbody tr:hover, .stTable tbody tr:hover {
        background-color: var(--table-row-hover-bg);
    }
    .stDataFrame td, .stTable td {
        color: var(--text-color);
    }
</style>""", unsafe_allow_html=True)


CONFIG = {
    "DIRETORIO_BASE": Path.home() / "Documentos" / "Analise_Financeira_Automatizada",
    "URL_BASE_CVM": 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/',
    "CONTAS_CVM": {
        # DRE
        "RECEITA_LIQUIDA": "3.01", "EBIT": "3.05", "DESPESAS_FINANCEIRAS": "3.07",
        "LUCRO_ANTES_IMPOSTOS": "3.09", "IMPOSTO_DE_RENDA_CSLL": "3.10", "LUCRO_LIQUIDO": "3.11",
        # Balanço Patrimonial Ativo
        "CAIXA": "1.01.01", "CONTAS_A_RECEBER": "1.01.03", "ESTOQUES": "1.01.04",
        "ATIVO_CIRCULANTE": "1.01", "ATIVO_NAO_CIRCULANTE": "1.02",
        "ATIVO_IMOBILIZADO": "1.02.01", "ATIVO_INTANGIVEL": "1.02.03", "ATIVO_TOTAL": "1",
        # Balanço Patrimonial Passivo
        "FORNECEDORES": "2.01.02", "DIVIDA_CURTO_PRAZO": "2.01.04",
        "PASSIVO_CIRCULANTE": "2.01", "DIVIDA_LONGO_PRAZO": "2.02.01",
        "PASSIVO_NAO_CIRCULANTE": "2.02", "PATRIMONIO_LIQUIDO": "2.03", "PASSIVO_TOTAL": "2",
        # DFC
        "DEPRECIACAO_AMORTIZACAO": "6.01",
    },
    "HISTORICO_ANOS_CVM": 5,
    "MEDIA_ANOS_CALCULO": 3,
    "PERIODO_BETA_IBOV": "5y",
    "TAXA_CRESCIMENTO_PERPETUIDADE": 0.04
}
CONFIG["DIRETORIO_DADOS_CVM"] = CONFIG["DIRETORIO_BASE"] / "CVM_DATA"
CONFIG["DIRETORIO_DADOS_EXTRAIDOS"] = CONFIG["DIRETORIO_BASE"] / "CVM_EXTRACTED"

# ==============================================================================
# LÓGICA DE DADOS GERAL (CVM, MERCADO, ETC.)
# ==============================================================================

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    """Cria uma sessão de requests com retentativa automática."""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

@st.cache_data
def get_stock_data(ticker_sa, period="2y", interval="1d"):
    """
    Busca dados históricos de um ativo com sistema de fallback de 3 níveis.
    Tenta yfinance -> brapi -> Alpha Vantage.
    """
    # 1. Tenta com yfinance
    try:
        df = yf.download(ticker_sa, period=period, interval=interval, progress=False, auto_adjust=True)
        if not df.empty:
            df.columns = [col.lower() for col in df.columns]
            return df
    except Exception:
        pass

    # 2. Fallback para brapi API
    try:
        ticker_sem_sa = ticker_sa.replace(".SA", "")
        range_map = {"2y": "2y", "5y": "5y", "1y": "1y"}
        brapi_range = range_map.get(period, "2y")
        
        response = requests_retry_session().get(f"https://brapi.dev/api/quote/{ticker_sem_sa}?range={brapi_range}&interval={interval}")
        response.raise_for_status()
        data = response.json()
        
        if 'results' in data and data['results']:
            hist_data = data['results'][0].get('historicalDataPrice')
            if hist_data:
                df = pd.DataFrame(hist_data)
                df['date'] = pd.to_datetime(df['date'], unit='s')
                df = df.set_index('date')
                df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
                if 'adj close' not in df.columns: df['adj close'] = df['close']
                return df[['open', 'high', 'low', 'close', 'adj close', 'volume']]
    except Exception:
        pass

    # 3. Fallback para Alpha Vantage
    try:
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": ticker_sa,
            "outputsize": "full",
            "apikey": ALPHA_VANTAGE_API_KEY
        }
        response = requests_retry_session().get("https://www.alphavantage.co/query", params=params)
        response.raise_for_status()
        data = response.json()
        
        if "Time Series (Daily)" in data:
            df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient='index')
            df.index = pd.to_datetime(df.index)
            df = df.rename(columns={
                '1. open': 'open', '2. high': 'high', '3. low': 'low', 
                '4. close': 'close', '5. adjusted close': 'adj close', '6. volume': 'volume'
            })
            df = df.apply(pd.to_numeric)
            df = df.sort_index()
            return df
    except Exception:
        return None

    return None


@st.cache_data
def setup_diretorios():
    """Cria os diretórios locais para armazenar os dados da CVM (se permitido)."""
    try:
        CONFIG["DIRETORIO_DADOS_CVM"].mkdir(parents=True, exist_ok=True)
        CONFIG["DIRETORIO_DADOS_EXTRAIDOS"].mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        return False

@st.cache_data(show_spinner=False)
def preparar_dados_cvm(anos_historico):
    """
    Baixa e processa os dados anuais da CVM para os demonstrativos financeiros.
    """
    ano_final = datetime.today().year
    ano_inicial = ano_final - anos_historico
    demonstrativos_consolidados = {}
    
    with st.spinner(f"Verificando e baixando dados da CVM de {ano_inicial} a {ano_final-1}..."):
        
        for ano in range(ano_inicial, ano_final):
            nome_zip = f'dfp_cia_aberta_{ano}.zip'
            url_zip = f'{CONFIG["URL_BASE_CVM"]}{nome_zip}'

            try:
                response = requests_retry_session().get(url_zip, timeout=60)
                response.raise_for_status()
                zip_buffer = io.BytesIO(response.content)

                with ZipFile(zip_buffer, 'r') as z:
                    for tipo in ['DRE', 'BPA', 'BPP', 'DFC_MI']:
                        nome_arquivo_csv = f'dfp_cia_aberta_{tipo}_con_{ano}.csv'
                        if nome_arquivo_csv in z.namelist():
                            with z.open(nome_arquivo_csv) as f:
                                df_anual = pd.read_csv(f, sep=';', encoding='ISO-8859-1', low_memory=False)
                                if tipo.lower() not in demonstrativos_consolidados:
                                    demonstrativos_consolidados[tipo.lower()] = pd.DataFrame()
                                demonstrativos_consolidados[tipo.lower()] = pd.concat([demonstrativos_consolidados[tipo.lower()], df_anual], ignore_index=True)
                        else:
                            st.warning(f"Arquivo {nome_arquivo_csv} não encontrado no zip do ano {ano}.")

            except requests.exceptions.RequestException as e:
                st.error(f"Falha ao baixar dados da CVM para o ano {ano} após múltiplas tentativas. Servidor pode estar offline. Erro: {e}")
                continue
            except Exception as e:
                st.warning(f"Erro ao processar dados do ano {ano}. Erro: {e}")
                continue

    return demonstrativos_consolidados


@st.cache_data
def carregar_mapeamento_ticker_cvm():
    """
    Carrega o mapeamento de tickers e códigos CVM a partir de uma string embutida.
    Esta lista foi corrigida para garantir que cada empresa tenha seu código CVM correto.
    """
    mapeamento_csv_data = """CD_CVM;Ticker;Nome_Empresa
25330;ALLD3;ALLIED TECNOLOGIA S.A.
10456;ALPA4;ALPARGATAS S.A.
25275;AVLL3;ALPHAVILLE S.A.
21490;ALUP11;ALUPAR INVESTIMENTO S.A.
24961;AMBP3;AMBIPAR PARTICIPACOES E EMPREENDIMENTOS S.A.
23264;ABEV3;AMBEV S.A.
20990;AMER3;AMERICANAS S.A.
23248;ANIM3;ANIMA HOLDING S.A.
12823;APTI4;SIDERURGICA J. L. ALIPERTI S.A.
26069;ARML3;ARMAC LOCACAO LOGISTICA E SERVICOS S.A.
19771;ARTR3;ARTERIS S.A.
18687;ATMP3;ATMA PARTICIPAÇÕES S.A.
24171;CRFB3;ATACADÃO S.A.
26590;AURA33;AURA MINERALS INC.
26620;AURE3;AUREN ENERGIA S.A.
24112;AZUL4;AZUL S.A.
11975;AZEV4;AZEVEDO & TRAVASSOS S.A.
1520;BDLL4;BARDELLA S.A. INDS. MECANICAS
23990;BAHI3;BAHEMA S.A.
19321;B3SA3;B3 S.A. - BRASIL, BOLSA, BALCÃO
14349;BAZA3;BANCO DA AMAZONIA S.A.
20562;BBAS3;BANCO DO BRASIL S.A.
20554;BBDC3;BANCO BRADESCO S.A.
20554;BBDC4;BANCO BRADESCO S.A.
21091;BBRK3;BR BROKERS PARTICIPAÇÕES S.A.
23438;BBSE3;BB SEGURIDADE PARTICIPAÇÕES S.A.
21210;BEEF3;MINERVA S.A.
23000;BIDI11;BANCO INTER S.A.
23000;BIDI4;BANCO INTER S.A.
19305;BIOM3;BIOMM S.A.
21932;BMGB4;BANCO BMG S.A.
1023;BMIN4;BANCO MERCANTIL DE INVESTIMENTOS S.A.
25150;BMOB3;BEMOBI MOBILE TECH S.A.
416;BNBR3;BANCO DO NORDESTE DO BRASIL S.A.
21511;BOAS3;BOA VISTA SERVIÇOS S.A.
20382;BPAC11;BANCO BTG PACTUAL S.A.
20382;BPAC5;BANCO BTG PACTUAL S.A.
20695;BPAN4;BANCO PAN S.A.
21649;BRAP4;BRADESPAR S.A.
18330;BRFS3;BRF S.A.
21245;BRGE11;CONSORCIO ALFA DE ADMINISTRACAO S.A.
21245;BRGE12;CONSORCIO ALFA DE ADMINISTRACAO S.A.
21245;BRGE3;CONSORCIO ALFA DE ADMINISTRACAO S.A.
21245;BRGE6;CONSORCIO ALFA DE ADMINISTRACAO S.A.
21245;BRGE8;CONSORCIO ALFA DE ADMINISTRACAO S.A.
21385;BRIV4;ALFA HOLDINGS S.A.
20471;BRKM5;BRASKEM S.A.
21800;BRML3;BR MALLS PARTICIPAÇÕES S.A.
19844;BRPR3;BR PROPERTIES S.A.
20087;BRSR6;BANCO DO ESTADO DO RIO GRANDE DO SUL S.A.
19658;BSLI4;BANCO DE BRASILIA S.A.
25380;CASH3;MÉLIUZ S.A.
21622;CAML3;CAMIL ALIMENTOS S.A.
2473;CCRO3;CCR S.A.
19860;CEAB3;C&A MODAS S.A.
2429;CEBR6;COMPANHIA ENERGETICA DE BRASILIA - CEB
1495;CEDO4;CIA DE FIACAO E TECIDOS CEDRO E CACHOEIRA
21171;CEEB3;COMPANHIA DE ELETRICIDADE DO ESTADO DA BAHIA - COELBA
19810;CEGR3;CEG S.A.
20447;CELE5;CENTRAIS ELETRICAS DE SANTA CATARINA S.A. - CELESC
20447;CELE6;CENTRAIS ELETRICAS DE SANTA CATARINA S.A. - CELESC
19348;CEMG4;COMPANHIA ENERGETICA DE MINAS GERAIS - CEMIG
18849;CEMG3;COMPANHIA ENERGETICA DE MINAS GERAIS - CEMIG
21104;CEPE5;CELPE - CIA ENERGETICA DE PERNAMBUCO
21104;CEPE6;CELPE - CIA ENERGETICA DE PERNAMBUCO
18814;CGAS5;COMPANHIA DE GAS DE SAO PAULO - COMGAS
24601;CGRA4;GRAZZIOTIN S.A.
19666;CIEL3;CIELO S.A.
20230;CLSC4;CENTRAIS ELETRICAS DE SANTA CATARINA S.A.
19348;CMIG3;COMPANHIA ENERGETICA DE MINAS GERAIS - CEMIG
21067;COCE5;COELCE S.A.
17973;COGN3;COGNA EDUCACAO S.A.
20687;CPFE3;CPFL ENERGIA S.A.
21819;CPLE3;COMPANHIA PARANAENSE DE ENERGIA - COPEL
21819;CPLE6;COMPANHIA PARANAENSE DE ENERGIA - COPEL
21819;CPLE11;COMPANHIA PARANAENSE DE ENERGIA - COPEL
19836;CSAN3;COSAN S.A.
19445;CSMG3;COMPANHIA DE SANEAMENTO DE MINAS GERAIS
4030;CSNA3;COMPANHIA SIDERURGICA NACIONAL
24399;CSRN5;CIA ENERGETICA DO RIO GRANDE DO NORTE - COSERN
24399;CSRN6;CIA ENERGETICA DO RIO GRANDE DO NORTE - COSERN
21032;CTKA4;KARSTEN S.A.
23081;CTNM4;COMPANHIA DE TECIDOS NORTE DE MINAS - COTEMINAS
25089;CTSA4;SANTANENSE S.A.
21030;CURY3;CURY CONSTRUTORA E INCORPORADORA S.A.
23310;CVCB3;CVC BRASIL OPERADORA E AGENCIA DE VIAGENS S.A.
14460;CYRE3;CYRELA BRAZIL REALTY S.A. EMPREEND E PART
25537;DASA3;DASA S.A.
21991;DIRR3;DIRECIONAL ENGENHARIA S.A.
25232;DMMO3;DOMMO ENERGIA S.A.
25356;DOTZ3;DOTZ S.A.
16567;DEXP3;DEXCO S.A.
16567;DEXP4;DEXCO S.A.
16869;ECOR3;ECORODOVIAS INFRAESTRUTURA E LOGÍSTICA S.A.
16648;EGIE3;ENGIE BRASIL ENERGIA S.A.
2437;ELET3;CENTRAIS ELETRICAS BRASILEIRAS S.A. - ELETROBRAS
2437;ELET6;CENTRAIS ELETRICAS BRASILEIRAS S.A. - ELETROBRAS
25510;ELMD3;ELETROMIDIA S.A.
16993;EMAE4;EMAE - EMPRESA METROPOLITANA DE ÁGUAS E ENERGIA S.A.
9425;EMBR3;EMBRAER S.A.
22491;ENAT3;ENAUTA PARTICIPAÇÕES S.A.
19763;ENBR3;ENERGIAS DO BRASIL S.A.
19280;ENEV3;ENEVA S.A.
15253;ENGI11;ENERGISA S.A.
15253;ENGI4;ENERGISA S.A.
25259;ENJU3;ENJOEI.COM.BR ATIVIDADES DE INTERNET S.A.
18309;EQPA3;EQUATORIAL PARÁ DISTRIBUIDORA DE ENERGIA S.A.
18309;EQPA5;EQUATORIAL PARÁ DISTRIBUIDORA DE ENERGIA S.A.
18309;EQPA7;EQUATORIAL PARÁ DISTRIBUIDORA DE ENERGIA S.A.
19992;EQTL3;EQUATORIAL ENERGIA S.A.
22036;ESPA3;ESPAÇOLASER SERVIÇOS ESTÉTICOS S.A.
14217;ESTR4;ESTRELA MANUFATURA DE BRINQUEDOS S.A.
5762;ETER3;ETERNIT S.A.
5770;EUCA4;EUCATEX S.A. INDÚSTRIA E COMÉRCIO
20524;EVEN3;EVEN CONSTRUTORA E INCORPORADORA S.A.
20874;EZTC3;EZTEC EMPREENDIMENTOS E PARTICIPAÇÕES S.A.
20621;FESA4;FERTILIZANTES HERINGER S.A.
20621;FHER3;FERTILIZANTES HERINGER S.A.
19623;FLRY3;FLEURY S.A.
6211;FRAS3;FRAS-LE S.A.
6211;FRAS4;FRAS-LE S.A.
20557;GFSA3;GAFISA S.A.
3980;GGBR4;GERDAU S.A.
3980;GGBR3;GERDAU S.A.
8656;GOAU4;METALURGICA GERDAU S.A.
25186;GMAT3;GRUPO MATEUS S.A.
19313;GOLL4;GOL LINHAS AÉREAS INTELIGENTES S.A.
19615;GRND3;GRENDENE S.A.
4669;GUAR3;GUARARAPES CONFECCOES S.A.
24396;HAPV3;HAPVIDA PARTICIPAÇÕES E INVESTIMENTOS S.A.
22675;HBSA3;HIDROVIAS DO BRASIL S.A.
25402;HBRE3;HBR REALTY EMPREENDIMENTOS IMOBILIARIOS S.A.
6629;HETA4;HERCULES S.A. - FABRICA DE TALHERES
20877;HBOR3;HELBOR EMPREENDIMENTOS S.A.
18913;HYPE3;HYPERA S.A.
25747;IFCM3;INFRICOMMERCE CXAAS S.A.
8672;IGTI11;IGUATEMI S.A.
20494;IGTA3;IGUATEMI EMPRESA DE SHOPPING CENTERS S.A.
7595;INEP3;INEPAR S/A INDUSTRIA E CONSTRUCOES
7595;INEP4;INEPAR S/A INDUSTRIA E CONSTRUCOES
25453;INTB3;INTELBRAS S.A.
2429;IRBR3;IRANI PAPEL E EMBALAGEM S.A.
7617;ITSA4;ITAUSA S.A.
7617;ITSA3;ITAUSA S.A.
19348;ITUB4;ITAÚ UNIBANCO HOLDING S.A.
19348;ITUB3;ITAÚ UNIBANCO HOLDING S.A.
24860;JALL3;JALLES MACHADO S.A.
20221;JBSS3;JBS S.A.
7811;JFEN3;JOÃO FORTES ENGENHARIA S.A.
2441;JHSF3;JHSF PARTICIPACOES S.A.
13285;JOPA4;JOSAPAR JOAQUIM OLIVEIRA S.A. PARTICIPACOES
22020;JSLG3;JSL S.A.
7870;KEPL3;KEPLER WEBER S.A.
12653;KLBN11;KLABIN S.A.
12653;KLBN4;KLABIN S.A.
12653;KLBN3;KLABIN S.A.
25062;LAVV3;LAVVI EMPREENDIMENTOS IMOBILIARIOS S.A.
19299;LIGT3;LIGHT S.A.
8133;LREN3;LOJAS RENNER S.A.
24910;LWSA3;LOCAWEB SERVICOS DE INTERNET S.A.
23272;LOGG3;LOG COMMERCIAL PROPERTIES E PARTICIPACOES S.A.
20710;LOGN3;LOG-IN LOGISTICA INTERMODAL S.A.
20370;LPSB3;LPS BRASIL - CONSULTORIA DE IMOIS S.A.
20060;LUPA3;LUPATECH S.A.
8192;LUXM4;TREVISA INVESTIMENTOS S.A.
25895;LVTC3;LIVETECH DA BAHIA INDÚSTRIA E COMÉRCIO S.A.
25267;MBLY3;MOBLY S.A.
20468;MDIA3;M. DIAS BRANCO S.A. INDUSTRIA E COMERCIO DE ALIMENTOS
21606;MDNE3;MOURA DUBEUX ENGENHARIA S.A.
23574;MEAL3;INTERNATIONAL MEAL COMPANY ALIMENTACAO S.A.
23426;MEGA3;OMEGA ENERGIA S.A.
25062;MELK3;MELNICK DESENVOLVIMENTO IMOBILIARIO S.A.
22470;MGLU3;MAGAZINE LUIZA S.A.
22012;MILS3;MILLS ESTRUTURAS E SERVICOS DE ENGENHARIA S.A.
19852;MMXM3;MMX MINERACAO E METALICOS S.A.
1333;MOAR3;MONT ARANHA S.A.
24610;MODL11;BANCO MODAL S.A.
23825;MOVI3;MOVIDA PARTICIPACOES S.A.
20123;MRFG3;MARFRIG GLOBAL FOODS S.A.
21626;MRVE3;MRV ENGENHARIA E PARTICIPACOES S.A.
24730;MTRE3;MITRE REALTY EMPREENDIMENTOS E PARTICIPACOES S.A.
20982;MULT3;MULTIPLAN - EMPREENDIMENTOS IMOBILIARIOS S.A.
11932;MYPK3;IOCHP-MAXION S.A.
15888;NEOE3;NEOENERGIA S.A.
25399;NGRD3;NEOGRID PARTICIPACOES S.A.
25421;NINJ3;GETNINJAS S.A.
24783;NTCO3;NATURA &CO HOLDING S.A.
20214;ODPV3;ODONTOPREV S.A.
23507;OFSA3;OUROFINO S.A.
11312;OIBR3;OI S.A.
11312;OIBR4;OI S.A.
22327;OMGE3;OMEGA GERACAO S.A.
21952;OPCT3;OCEANPACT SERVICOS MARITIMOS S.A.
21928;OSXB3;OSX BRASIL S.A.
22710;PARD3;INSTITUTO HERMES PARDINI S.A.
94;PATI4;PANATLANTICA S.A.
14826;PCAR3;COMPANHIA BRASILEIRA DE DISTRIBUIÇÃO
20530;PDGR3;PDG REALTY S.A. EMPREENDIMENTOS E PARTICIPACOES
9512;PETR3;PETRÓLEO BRASILEIRO S.A. - PETROBRAS
9512;PETR4;PETRÓLEO BRASILEIRO S.A. - PETROBRAS
25089;PETZ3;PET CENTER COMERCIO E PARTICIPACOES S.A.
20346;PFRM3;PROFARMA DISTRIBUIDORA DE PRODUTOS FARMACEUTICOS S.A.
20360;PGMN3;PAGUE MENOS COMERCIO DE PRODUTOS ALIMENTICIOS S.A.
21881;PRIO3;PETRORIO S.A.
25130;PLPL3;PLANO & PLANO DESENVOLVIMENTO IMOBILIARIO S.A.
9393;PMAM3;PARANAPANEMA S.A.
8451;POMO4;MARCOPOLO S.A.
8451;POMO3;MARCOPOLO S.A.
26247;PORT3;WILSON SONS S.A.
20362;POSI3;POSITIVO TECNOLOGIA S.A.
21881;PRIO3;PRIO S.A.
24236;PRNR3;PRINER SERVICOS INDUSTRIAIS S.A.
16659;PSSA3;PORTO SEGURO S.A.
13773;PTBL3;PORTOBELLO S.A.
20095;QUAL3;QUALICORP CONSULTORIA E CORRETORA DE SEGUROS S.A.
5258;RADL3;RAIA DROGASIL S.A.
17450;RAIL3;RUMO S.A.
2429;RANI3;IRANI PAPEL E EMBALAGEM S.A.
14109;RAPT4;RANDON S.A. IMPLEMENTOS E PARTICIPACOES
24821;RDOR3;REDE D'OR SAO LUIZ S.A.
19132;RECV3;PETRORECONCAVO S.A.
17059;RENT3;LOCALIZA RENT A CAR S.A.
12572;RCSL4;RECRUSUL S.A.
7510;ROMI3;INDUSTRIAS ROMI S.A.
25502;RRRP3;3R PETROLEUM OLEO E GAS S.A.
16306;RSID3;ROSSI RESIDENCIAL S.A.
20532;SANB11;BANCO SANTANDER (BRASIL) S.A.
20532;SANB3;BANCO SANTANDER (BRASIL) S.A.
20532;SANB4;BANCO SANTANDER (BRASIL) S.A.
18627;SAPR11;COMPANHIA DE SANEAMENTO DO PARANA - SANEPAR
18627;SAPR4;COMPANHIA DE SANEAMENTO DO PARANA - SANEPAR
20050;SBFG3;GRUPO SBF S.A.
14443;SBSP3;COMPANHIA DE SANEAMENTO BASICO DO ESTADO DE SAO PAULO - SABESP
23221;SEER3;SER EDUCACIONAL S.A.
25160;SEQL3;SEQUOIA LOGISTICA E TRANSPORTES S.A.
25003;SIMH3;SIMPAR S.A.
20290;SLCE3;SLC AGRICOLA S.A.
10472;SLED4;SARAIVA S.A. L IVREIROS EDITORES
25448;SMFT3;SMARTFIT ESCOLA DE GINASTICA E DANCA S.A.
20516;SMTO3;SAO MARTINHO S.A.
25020;SOMA3;GRUPO DE MODA SOMA S.A.
22173;SQIA3;SINQIA S.A.
17892;STBP3;SANTOS BRASIL PARTICIPACOES S.A.
20652;SULA11;SUL AMERICA S.A.
13986;SUZB3;SUZANO S.A.
21040;SYNE3;SYN PROP & TECH S.A
20520;TAEE11;TRANSMISSORA ALIANCA DE ENERGIA ELETRICA S.A.
20520;TAEE4;TRANSMISSORA ALIANCA DE ENERGIA ELETRICA S.A.
6173;TASA4;TAURUS ARMAS S.A.
20506;TCSA3;TECNISA S.A.
22519;TECN3;TECHNOS S.A.
21148;TEND3;CONSTRUTORA TENDA S.A.
20825;TGMA3;TEGMA GESTAO LOGISTICA S.A.
17020;TIMS3;TIM S.A.
19992;TOTS3;TOTVS S.A.
21130;TRIS3;TRISUL S.A.
20597;TRPL4;ISA CTEEP - COMPANHIA DE TRANSMISSAO DE ENERGIA ELETRICA PAULISTA
6343;TUPY3;TUPY S.A.
18465;UGPA3;ULTRAPAR PARTICIPACOES S.A.
11592;UNIP6;UNIPAR CARBOCLORO S.A.
14320;USIM5;USINAS SIDERURGICAS DE MINAS GERAIS S.A. - USIMINAS
14320;USIM3;USINAS SIDERURGICAS DE MINAS GERAIS S.A. - USIMINAS
4170;VALE3;VALE S.A.
24716;VAMO3;VAMOS LOCACAO DE CAMINHOES, MAQUINAS E EQUIPAMENTOS S.A.
24295;VBBR3;VIBRA ENERGIA S.A.
6505;VIIA3;VIA S.A.
25709;VITT3;VITTIA FERTILIZANTES E BIOLOGICOS S.A.
24448;VIVA3;VIVARA PARTICIPACOES S.A.
17793;VIVT3;TELEFONICA BRASIL S.A.
20028;VLID3;VALID SOLUCOES S.A.
11762;VULC3;VULCABRAS S.A.
5410;WEGE3;WEG S.A.
23590;WIZC3;WIZ CO PARTICIPAÇÕES E CORRETAGEM DE SEGUROS S.A.
21075;YDUQ3;YDUQS PARTICIPACOES S.A.
25801;REDE3;REDE ENERGIA PARTICIPAÇÕES S.A.
25810;GGPS3;GPS PARTICIPAÇÕES E EMPREENDIMENTOS S.A.
24627;BLAU3;BLAU FARMACÊUTICA S.A.
25860;BRBI11;BRBI BR PARTNERS S.A
25879;KRSA3;KORA SAÚDE PARTICIPAÇÕES S.A.
25895;LVTC3;LIVETECH DA BAHIA INDÚSTRIA E COMÉRCIO S.A.
23230;RAIZ4;RAÍZEN S.A.
25950;TTEN3;TRÊS TENTOS AGROINDUSTRIAL S.A.
25984;CBAV3;COMPANHIA BRASILEIRA DE ALUMINIO
26000;LAND3;TERRA SANTA PROPRIEDADES AGRÍCOLAS S.A.
26026;DESK3;DESKTOP S.A
26034;MLAS3;GRUPO MULTI S.A.
26050;FIQE3;UNIFIQUE TELECOMUNICAÇÕES S.A.
26069;ARML3;ARMAC LOCAÇÃO LOGÍSTICA E SERVIÇOS S.A.
26077;TRAD3;TC S.A.
26123;ONCO3;ONCOCLÍNICAS DO BRASIL SERVIÇOS MÉDICOS S.A.
26174;AURE3;AUREN OPERAÇÕES S.A.
26247;PORT3;WILSON SONS S.A.
26441;SRNA3;SERENA ENERGIA S.A.
26484;NEXP3;NEXPE PARTICIPAÇÕES S.A.
"""
    try:
        # CORREÇÃO: O separador foi alterado para ponto e vírgula para corresponder ao novo formato da string.
        df = pd.read_csv(io.StringIO(mapeamento_csv_data), sep=';', encoding='utf-8')
        df.columns = df.columns.str.strip()
        df.rename(columns={'Ticker': 'TICKER', 'CD_CVM': 'CD_CVM'}, inplace=True, errors='ignore')
        df = df.dropna(subset=['TICKER', 'CD_CVM'])
        df['CD_CVM'] = pd.to_numeric(df['CD_CVM'], errors='coerce').astype('Int64')
        df['TICKER'] = df['TICKER'].astype(str).str.strip().str.upper()
        df = df.dropna(subset=['CD_CVM']).drop_duplicates(subset=['TICKER'])
        
        return df
    except Exception as e:
        st.error(f"Falha ao carregar o mapeamento de tickers. Erro: {e}")
        return pd.DataFrame()

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
def consulta_bc(codigo_bcb):
    """Consulta a API do Banco Central para obter dados como a taxa Selic."""
    try:
        url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados/ultimos/1?formato=json'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return float(data[0]['valor']) / 100.0 if data else None
    except Exception as e:
        raise Exception(f"Erro ao consultar a API do Banco Central. Código: {codigo_bcb}. Erro: {e}")

@st.cache_data(show_spinner=False)
def obter_dados_mercado(periodo_ibov):
    """Busca dados de mercado como Selic, Ibovespa e prêmio de risco."""
    with st.spinner("Buscando dados de mercado (Selic, Ibovespa)..."):
        try:
            selic_anual = consulta_bc(1178)
        except Exception:
            selic_anual = None
        
        risk_free_rate = selic_anual if selic_anual is not None else 0.15
        
        ibov = yf.download('^BVSP', period=periodo_ibov, progress=False)
        if not ibov.empty and 'Adj Close' in ibov.columns:
            retorno_anual_mercado = ((1 + ibov['Adj Close'].pct_change().mean()) ** 252) - 1
        else:
            retorno_anual_mercado = 0.12
            
        premio_risco_mercado = retorno_anual_mercado - risk_free_rate
    return risk_free_rate, retorno_anual_mercado, premio_risco_mercado, ibov

def obter_historico_metrica(df_empresa, codigo_conta):
    """
    Extrai o histórico anual de uma conta contábil específica da CVM.
    """
    metric_df = df_empresa[(df_empresa['CD_CONTA'] == codigo_conta) & (df_empresa['ORDEM_EXERC'] == 'ÚLTIMO')]
    if metric_df.empty:
        return pd.Series(dtype=float)
    
    metric_df['DT_REFER'] = pd.to_datetime(metric_df['DT_REFER'])
    metric_df = metric_df.sort_values('DT_REFER').groupby(metric_df['DT_REFER'].dt.year).last()
    
    return metric_df['VL_CONTA'].sort_index()


# ==============================================================================
# ABA 1: CONTROLE FINANCEIRO
# ==============================================================================

def inicializar_session_state():
    """Inicializa o estado da sessão para simular um banco de dados."""
    if 'transactions' not in st.session_state:
        st.session_state.transactions = pd.DataFrame(columns=['Data', 'Tipo', 'Categoria', 'Subcategoria ARCA', 'Valor', 'Descrição'])
    if 'categories' not in st.session_state:
        st.session_state.categories = {
            'Receita': ['Salário', 'Freelance'], 
            'Despesa': ['Moradia', 'Alimentação', 'Transporte', 'Saúde', 'Vestuário'], 
            'Investimento': ['Ações BR', 'REITs (FII)', 'Caixa', 'Ações Internacionais']
        }
    if 'goals' not in st.session_state:
        st.session_state.goals = {
            'Reserva de Emergência': {'meta': 10000.0, 'atual': 0.0},
            'Liberdade Financeira': {'meta': 1000000.0, 'atual': 0.0}
        }

def format_large_number(num):
    """Formata números grandes para exibição em cards (k, M)."""
    if abs(num) >= 1_000_000:
        return f"R$ {num/1_000_000:.1f}M"
    if abs(num) >= 1_000:
        return f"R$ {num/1_000:.1f}k"
    return f"R$ {num:,.2f}"

def ui_controle_financeiro():
    """Renderiza a interface completa da aba de Controle Financeiro."""
    st.header("Dashboard de Controle Financeiro Pessoal")
    
    col_filter1, col_filter2, col_filter3 = st.columns([1, 1, 1])

    data_inicio = col_filter1.date_input("Data de Início", value=datetime.now() - pd.Timedelta(days=365), format="DD/MM/YYYY")
    data_fim = col_filter2.date_input("Data de Fim", value=datetime.now(), format="DD/MM/YYYY")
    tipo_filtro = col_filter3.selectbox("Filtrar por Tipo", ["Todos", "Receita", "Despesa", "Investimento"])

    st.divider()

    df_trans = st.session_state.transactions.copy()
    if not df_trans.empty:
        df_trans['Data'] = pd.to_datetime(df_trans['Data'])
        df_filtrado = df_trans[(df_trans['Data'].dt.date >= data_inicio) & (df_trans['Data'].dt.date <= data_fim)]
        if tipo_filtro != "Todos":
            df_filtrado = df_filtrado[df_filtrado['Tipo'] == tipo_filtro]
    else:
        df_filtrado = pd.DataFrame()

    if not df_filtrado.empty:
        total_receitas = df_filtrado[df_filtrado['Tipo'] == 'Receita']['Valor'].sum()
        total_despesas = df_filtrado[df_filtrado['Tipo'] == 'Despesa']['Valor'].sum()
        total_investido = df_filtrado[df_filtrado['Tipo'] == 'Investimento']['Valor'].sum()
        saldo_periodo = total_receitas - total_despesas - total_investido
    else:
        total_receitas, total_despesas, total_investido, saldo_periodo = 0, 0, 0, 0

    st.subheader("Resumo do Período")
    col_card1, col_card2, col_card3, col_card4 = st.columns(4)
    col_card1.metric("Receitas", format_large_number(total_receitas))
    col_card2.metric("Despesas", format_large_number(total_despesas))
    col_card3.metric("Investimentos", format_large_number(total_investido))
    col_card4.metric("Saldo", format_large_number(saldo_periodo))
    
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        with st.expander("➕ Novo Lançamento", expanded=True):
            with st.form("new_transaction_form", clear_on_submit=True):
                data = st.date_input("Data", datetime.now(), format="DD/MM/YYYY")
                tipo = st.selectbox("Tipo", ["Receita", "Despesa", "Investimento"])
                
                opcoes_categoria = st.session_state.categories.get(tipo, []) + ["--- Adicionar Nova Categoria ---"]
                categoria_selecionada = st.selectbox("Categoria", options=opcoes_categoria, key=f"cat_{tipo}")
                
                categoria_final = None
                if categoria_selecionada == "--- Adicionar Nova Categoria ---":
                    nova_categoria = st.text_input("Nome da Nova Categoria", key=f"new_cat_{tipo}")
                    if nova_categoria: categoria_final = nova_categoria
                else:
                    categoria_final = categoria_selecionada
                
                sub_arca = categoria_final if tipo == "Investimento" else None
                
                valor = st.number_input("Valor (R$)", min_value=0.0, format="%.2f")
                descricao = st.text_input("Descrição (opcional)")
                submitted = st.form_submit_button("Adicionar Lançamento")

                if submitted and categoria_final:
                    if tipo != "Investimento" and categoria_final not in st.session_state.categories[tipo]:
                        st.session_state.categories[tipo].append(categoria_final)

                    nova_transacao = pd.DataFrame([{'Data': data, 'Tipo': tipo, 'Categoria': categoria_final, 'Subcategoria ARCA': sub_arca, 'Valor': valor, 'Descrição': descricao}])
                    st.session_state.transactions = pd.concat([st.session_state.transactions, nova_transacao], ignore_index=True).reset_index(drop=True)
                    st.success("Lançamento adicionado!")
                    st.rerun()
    
    with col2:
        with st.expander("🎯 Metas Financeiras", expanded=True):
            meta_selecionada = st.selectbox("Selecione a meta para definir", options=list(st.session_state.goals.keys()))
            novo_valor_meta = st.number_input("Definir Valor Alvo (R$)", min_value=0.0, value=st.session_state.goals[meta_selecionada]['meta'], format="%.2f")
            if st.button("Atualizar Meta"):
                st.session_state.goals[meta_selecionada]['meta'] = novo_valor_meta
                st.success(f"Meta '{meta_selecionada}' atualizada!")
    
    st.divider()

    st.subheader("Análise Histórica")
    if not df_trans.empty:
        neon_palette = ['#00F6FF', '#39FF14', '#FF5252', '#F2A30F', '#7B2BFF']
        
        df_arca = df_trans[df_trans['Tipo'] == 'Investimento'].groupby('Subcategoria ARCA')['Valor'].sum()
        if not df_arca.empty:
            fig_arca = px.pie(df_arca, values='Valor', names=df_arca.index, title="Composição dos Investimentos (ARCA)", 
                                hole=.4, color_discrete_sequence=neon_palette)
            fig_arca.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
                                    legend_font_color='var(--text-color)', title_font_color='var(--header-color)')
            fig_arca.update_traces(textinfo='percent+label', textfont_size=14)
            st.plotly_chart(fig_arca, use_container_width=True)
        else:
            st.info("Nenhum investimento ARCA registrado.")
            
        st.divider()
        
        df_investimento_filtrado = df_trans[
            (df_trans['Tipo'] == 'Investimento') & 
            (df_trans['Data'].dt.date >= data_inicio) & 
            (df_trans['Data'].dt.date <= data_fim)
        ].copy()

        patrimonio_inicial = df_trans[
            (df_trans['Data'].dt.date < data_inicio) & 
            (df_trans['Tipo'] == 'Investimento')
        ]['Valor'].sum()
        
        df_investimento_diario = df_investimento_filtrado.set_index('Data').resample('D')['Valor'].sum().fillna(0)
        df_patrimonio_filtrado = df_investimento_diario.cumsum() + patrimonio_inicial
        
        if not df_patrimonio_filtrado.empty:
            fig_evol_patrimonio_investimento = px.line(df_patrimonio_filtrado, 
                                                        y=df_patrimonio_filtrado.values, 
                                                        title="Evolução do Patrimônio (Investimentos)", 
                                                        labels={'index': 'Data', 'y': 'Patrimônio Total'},
                                                        markers=True, 
                                                        template="plotly_dark")
            fig_evol_patrimonio_investimento.update_layout(paper_bgcolor='rgba(0,0,0,0)', 
                                                            plot_bgcolor='rgba(0,0,0,0)', 
                                                            font_color='var(--text-color)', 
                                                            title_font_color='var(--header-color)',
                                                            yaxis_title='Patrimônio Total (R$)')
            st.plotly_chart(fig_evol_patrimonio_investimento, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            df_monthly = df_trans.set_index('Data').groupby([pd.Grouper(freq='M'), 'Tipo'])['Valor'].sum().unstack(fill_value=0)
            fig_evol_tipo = px.bar(df_monthly, x=df_monthly.index, 
                                    y=[col for col in ['Receita', 'Despesa', 'Investimento'] if col in df_monthly.columns], 
                                    title="Evolução Mensal por Tipo", barmode='group', 
                                    color_discrete_map={'Receita': '#00F6FF', 'Despesa': '#FF5252', 'Investimento': '#39FF14'})
            fig_evol_tipo.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
                                        legend_font_color='var(--text-color)', title_font_color='var(--header-color)')
            st.plotly_chart(fig_evol_tipo, use_container_width=True)
        with col2:
            df_monthly['Patrimonio'] = (df_monthly.get('Receita', 0) - df_monthly.get('Despesa', 0)).cumsum()
            fig_evol_patrimonio = px.line(df_monthly, x=df_monthly.index, y='Patrimonio', title="Evolução Patrimonial", markers=True, template="plotly_dark")
            fig_evol_patrimonio.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
                                                legend_font_color='var(--text-color)', title_font_color='var(--header-color)')
            st.plotly_chart(fig_evol_patrimonio, use_container_width=True)
    else:
        st.info("Adicione transações para visualizar os gráficos de evolução.")

    with st.expander("📜 Histórico de Transações", expanded=True):
        if not df_trans.empty:
            df_para_editar = df_trans.copy()
            df_para_editar['Excluir'] = False
            
            colunas_config = {
                "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "Valor": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f"),
                "Subcategoria ARCA": st.column_config.TextColumn("ARCA")
            }

            edited_df = st.data_editor(
                df_para_editar[['Excluir', 'Data', 'Tipo', 'Categoria', 'Subcategoria ARCA', 'Valor', 'Descrição']], 
                use_container_width=True,
                column_config=colunas_config,
                hide_index=True,
                key="editor_transacoes"
            )
            
            if st.button("Excluir Lançamentos Selecionados"):
                indices_para_excluir = edited_df[edited_df['Excluir']].index
                st.session_state.transactions = st.session_state.transactions.drop(indices_para_excluir).reset_index(drop=True)
                st.success("Lançamentos excluídos!")
                st.rerun()

            edited_df_sem_excluir = edited_df.drop(columns=['Excluir'])
            st.session_state.transactions = edited_df_sem_excluir

        else:
            st.info("Nenhuma transação registrada.")


# ==============================================================================
# ABA 2: VALUATION
# ==============================================================================
def calcular_beta(ticker, ibov_data, periodo_beta):
    """Calcula o Beta de uma ação em relação ao Ibovespa de forma robusta."""
    dados_acao = yf.download(ticker, period=periodo_beta, progress=False, auto_adjust=True)['Close']
    if dados_acao.empty:
        return 1.0

    # Alinha os dataframes usando merge para garantir consistência
    dados_combinados = pd.merge(dados_acao, ibov_data['Close'], left_index=True, right_index=True, suffixes=('_acao', '_ibov')).dropna()
    
    retornos_mensais = dados_combinados.resample('M').ffill().pct_change().dropna()

    if len(retornos_mensais) < 2:
        return 1.0

    covariancia = retornos_mensais.cov().iloc[0, 1]
    variancia_mercado = retornos_mensais.iloc[:, 1].var()
    
    return covariancia / variancia_mercado if variancia_mercado != 0 else 1.0

def calcular_beta_hamada(ticker, ibov_data, periodo_beta, imposto, divida_total, market_cap):
    """
    Calcula o Beta alavancado ajustado pelo modelo de Hamada.
    """
    beta_alavancado_mercado = calcular_beta(ticker, ibov_data, periodo_beta)
    
    if (market_cap + divida_total) == 0 or market_cap == 0:
        return beta_alavancado_mercado
    
    divida_patrimonio = divida_total / market_cap
    beta_desalavancado = beta_alavancado_mercado / (1 + (1 - imposto) * divida_patrimonio)
    
    beta_realavancado = beta_desalavancado * (1 + (1 - imposto) * divida_patrimonio)
    
    return beta_realavancado

def processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params):
    """
    Executa a análise de valuation de uma única empresa, calculando EVA, EFV, WACC, etc.
    """
    (risk_free_rate, _, premio_risco_mercado, ibov_data) = market_data

    dre = demonstrativos.get('dre', pd.DataFrame())
    bpa = demonstrativos.get('bpa', pd.DataFrame())
    bpp = demonstrativos.get('bpp', pd.DataFrame())
    dfc = demonstrativos.get('dfc_mi', pd.DataFrame())
    
    if dre.empty or bpa.empty or bpp.empty or dfc.empty:
        return None, "Dados da CVM não puderam ser baixados. Análise de valuation impossível."

    empresa_dre = dre[dre['CD_CVM'] == codigo_cvm]
    empresa_bpa = bpa[bpa['CD_CVM'] == codigo_cvm]
    empresa_bpp = bpp[bpp['CD_CVM'] == codigo_cvm]
    empresa_dfc = dfc[dfc['CD_CVM'] == codigo_cvm]
    
    if any(df.empty for df in [empresa_dre, empresa_bpa, empresa_bpp, empresa_dfc]):
        return None, "Dados CVM históricos incompletos ou inexistentes para este ticker."
    
    try:
        info = yf.Ticker(ticker_sa).info
        market_cap = info.get('marketCap')
        preco_atual = info.get('currentPrice', info.get('previousClose'))
        nome_empresa = info.get('longName', ticker_sa)
        n_acoes = info.get('sharesOutstanding')
        
        if not all([market_cap, preco_atual, n_acoes, nome_empresa]):
            return None, "Dados de mercado (YFinance) incompletos."
            
    except Exception:
        return None, "Falha ao buscar dados no Yahoo Finance."
        
    C = CONFIG['CONTAS_CVM']
    
    # Extração de dados da CVM para séries históricas
    hist_ebit = obter_historico_metrica(empresa_dre, C['EBIT'])
    hist_impostos = obter_historico_metrica(empresa_dre, C['IMPOSTO_DE_RENDA_CSLL'])
    hist_lai = obter_historico_metrica(empresa_dre, C['LUCRO_ANTES_IMPOSTOS'])
    hist_rec_liquida = obter_historico_metrica(empresa_dre, C['RECEITA_LIQUIDA'])
    hist_lucro_liquido = obter_historico_metrica(empresa_dre, C['LUCRO_LIQUIDO'])
    hist_contas_a_receber = obter_historico_metrica(empresa_bpa, C['CONTAS_A_RECEBER'])
    hist_estoques = obter_historico_metrica(empresa_bpa, C['ESTOQUES'])
    hist_fornecedores = obter_historico_metrica(empresa_bpp, C['FORNECEDORES'])
    hist_ativo_imobilizado = obter_historico_metrica(empresa_bpa, C['ATIVO_IMOBILIZADO'])
    hist_ativo_intangivel = obter_historico_metrica(empresa_bpa, C['ATIVO_INTANGIVEL'])
    hist_divida_cp = obter_historico_metrica(empresa_bpp, C['DIVIDA_CURTO_PRAZO'])
    hist_divida_lp = obter_historico_metrica(empresa_bpp, C['DIVIDA_LONGO_PRAZO'])
    hist_desp_financeira = abs(obter_historico_metrica(empresa_dre, C['DESPESAS_FINANCEIRAS']))
    hist_pl_total = obter_historico_metrica(empresa_bpp, C['PATRIMONIO_LIQUIDO'])
    hist_dep_amort = obter_historico_metrica(empresa_dfc, C['DEPRECIACAO_AMORTIZACAO'])
    
    if hist_lai.sum() == 0 or hist_ebit.empty:
        return None, "Dados de Lucro/EBIT insuficientes para calcular a alíquota de imposto."

    aliquota_efetiva = abs(hist_impostos.sum()) / abs(hist_lai.sum()) if hist_lai.sum() != 0 else 0
    
    hist_nopat = hist_ebit * (1 - aliquota_efetiva)
    hist_fco = hist_nopat.add(hist_dep_amort, fill_value=0)
    hist_ncg = hist_contas_a_receber.add(hist_estoques, fill_value=0).subtract(hist_fornecedores, fill_value=0)
    hist_capital_empregado = hist_ncg.add(hist_ativo_imobilizado, fill_value=0).add(hist_ativo_intangivel, fill_value=0)
    
    df_series = pd.concat([hist_nopat, hist_fco, hist_capital_empregado, hist_divida_cp, hist_divida_lp, hist_desp_financeira, hist_pl_total, hist_rec_liquida, hist_lucro_liquido, hist_contas_a_receber, hist_estoques, hist_fornecedores, hist_ebit, hist_dep_amort], axis=1).dropna()
    df_series.columns = ['NOPAT', 'FCO', 'Capital Empregado', 'Divida CP', 'Divida LP', 'Despesas Financeiras', 'PL', 'Receita Liquida', 'Lucro Liquido', 'Contas a Receber', 'Estoques', 'Fornecedores', 'EBIT', 'Dep_Amort']

    if df_series.empty:
        return None, "Séries históricas incompletas para os cálculos anuais."
    
    hist_divida_total = df_series['Divida CP'] + df_series['Divida LP']
    hist_roic = (df_series['NOPAT'] / df_series['Capital Empregado'])
    
    divida_total_ultimo_ano = hist_divida_total.iloc[-1]
    
    beta_hamada = calcular_beta_hamada(ticker_sa, ibov_data, params['periodo_beta_ibov'], aliquota_efetiva, divida_total_ultimo_ano, market_cap)
    ke = risk_free_rate + beta_hamada * premio_risco_mercado
    ev_mercado = market_cap + divida_total_ultimo_ano
    wacc_medio = ((market_cap / ev_mercado) * ke) + ((divida_total_ultimo_ano / ev_mercado) * (df_series['Despesas Financeiras'].mean() / divida_total_ultimo_ano) * (1 - aliquota_efetiva)) if ev_mercado > 0 and divida_total_ultimo_ano > 0 else ke
    
    if wacc_medio <= params['taxa_crescimento_perpetuidade'] or pd.isna(wacc_medio):
        return None, "WACC inválido ou menor/igual à taxa de crescimento na perpetuidade. Ajuste os parâmetros."

    hist_wacc = pd.Series([wacc_medio] * len(df_series.index), index=df_series.index)
    
    hist_eva = (hist_roic - hist_wacc) * df_series['Capital Empregado']
    hist_riqueza_atual = hist_eva / hist_wacc
    
    riqueza_futura_esperada_ultimo = market_cap + divida_total_ultimo_ano - df_series['Capital Empregado'].iloc[-1]
    efv_ultimo = riqueza_futura_esperada_ultimo - hist_riqueza_atual.iloc[-1]

    hist_riqueza_futura_percentual = ((pd.Series([riqueza_futura_esperada_ultimo] * len(df_series.index), index=df_series.index) / df_series['Capital Empregado']) - 1) * 100
    hist_riqueza_atual_percentual = (hist_riqueza_atual / df_series['Capital Empregado']) * 100
    hist_efv_percentual = (hist_riqueza_futura_percentual - hist_riqueza_atual_percentual)
    hist_eva_percentual = (hist_eva / df_series['Capital Empregado']) * 100
    
    resultados = {
        'Empresa': nome_empresa, 'Ticker': ticker_sa.replace('.SA', ''), 'Preço Atual (R$)': preco_atual, 
        'Preço Justo (R$)': (riqueza_futura_esperada_ultimo + df_series['Capital Empregado'].iloc[-1] - divida_total_ultimo_ano) / n_acoes if n_acoes > 0 else 0, 
        'Margem Segurança (%)': ((riqueza_futura_esperada_ultimo + df_series['Capital Empregado'].iloc[-1] - divida_total_ultimo_ano) / n_acoes / preco_atual - 1) * 100 if n_acoes > 0 and preco_atual > 0 else -100, 
        'Market Cap (R$)': market_cap, 'Capital Empregado (R$)': df_series['Capital Empregado'].iloc[-1], 
        'Dívida Total (R$)': divida_total_ultimo_ano, 'NOPAT Médio (R$)': df_series['NOPAT'].tail(params['media_anos_calculo']).mean(), 
        'ROIC (%)': hist_roic.iloc[-1] * 100, 'Beta': beta_hamada, 'Custo do Capital (WACC %)': wacc_medio * 100, 
        'Spread (ROIC-WACC %)': (hist_roic.iloc[-1] - hist_wacc.iloc[-1]) * 100, 'EVA (R$)': hist_eva.iloc[-1], 'EFV (R$)': efv_ultimo,
        'Crescimento Vendas (%)': df_series['Receita Liquida'].pct_change().iloc[-1] * 100 if len(df_series['Receita Liquida']) > 1 else 0,
        'Margem de Lucro (%)': (df_series['Lucro Liquido'].iloc[-1] / df_series['Receita Liquida'].iloc[-1]) * 100 if df_series['Receita Liquida'].iloc[-1] != 0 else 0,
        'Dívida/Patrimônio': divida_total_ultimo_ano / df_series['PL'].iloc[-1] if df_series['PL'].iloc[-1] > 0 else np.nan,
        'Prazo Cobrança (dias)': (df_series['Contas a Receber'].iloc[-1] / df_series['Receita Liquida'].iloc[-1]) * 365 if df_series['Receita Liquida'].iloc[-1] != 0 else np.nan,
        'Prazo Pagamento (dias)': (df_series['Fornecedores'].iloc[-1] / (df_series['EBIT'].iloc[-1] + df_series['Dep_Amort'].iloc[-1] - df_series['Lucro Liquido'].iloc[-1])) * 365 if (df_series['EBIT'].iloc[-1] + df_series['Dep_Amort'].iloc[-1] - df_series['Lucro Liquido'].iloc[-1]) != 0 else np.nan,
        'Giro Estoques (vezes)': df_series['Receita Liquida'].iloc[-1] / df_series['Estoques'].iloc[-1] if df_series['Estoques'].iloc[-1] != 0 else np.nan,
        'ke': ke, 'kd': df_series['Despesas Financeiras'].mean() / divida_total_ultimo_ano if divida_total_ultimo_ano > 0 else 0,
        'hist_nopat': hist_nopat, 'hist_fco': hist_fco, 'hist_roic': hist_roic * 100, 'wacc_series': hist_wacc * 100,
        'hist_riqueza_futura_percentual': hist_riqueza_futura_percentual, 'hist_riqueza_atual_percentual': hist_riqueza_atual_percentual,
        'hist_efv_percentual': hist_efv_percentual, 'hist_eva_percentual': hist_eva_percentual
    }
    
    return resultados, "Análise concluída com sucesso."


def executar_analise_completa(ticker_map, demonstrativos, market_data, params, progress_bar):
    """Executa a análise de valuation para todas as empresas da lista."""
    todos_os_resultados = []
    total_empresas = len(ticker_map)
    for i, (index, row) in enumerate(ticker_map.iterrows()):
        ticker = row['TICKER']
        codigo_cvm = int(row['CD_CVM'])
        ticker_sa = f"{ticker}.SA"
        progress = (i + 1) / total_empresas
        progress_bar.progress(progress, text=f"Analisando {i+1}/{total_empresas}: {ticker}")
        try:
            resultados, _ = processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params)
            if resultados:
                todos_os_resultados.append(resultados)
        except Exception as e:
            st.error(f"Erro ao analisar {ticker}. Erro: {e}")
            continue
    progress_bar.empty()
    return todos_os_resultados

@st.cache_data
def convert_df_to_csv(df):
    """Converte um DataFrame para o formato CSV."""
    return df.to_csv(index=False, decimal=',', sep=';', encoding='utf-8-sig').encode('utf-8-sig')

def exibir_rankings(df_final):
    """Exibe os rankings de mercado com base nos resultados do valuation."""
    st.subheader("🏆 Rankings de Mercado")
    if df_final.empty:
        st.warning("Nenhuma empresa pôde ser analisada com sucesso para gerar os rankings.")
        return
        
    rankings = {
        "MARGEM_SEGURANCA": ("Ranking por Margem de Segurança", 'Margem Segurança (%)', ['Ticker', 'Empresa', 'Preço Atual (R$)', 'Preço Justo (R$)', 'Margem Segurança (%)']),
        "ROIC": ("Ranking por ROIC", 'ROIC (%)', ['Ticker', 'Empresa', 'ROIC (%)', 'Spread (ROIC-WACC %)']),
        "EVA": ("Ranking por EVA", 'EVA (R$)', ['Ticker', 'Empresa', 'EVA (R$)']),
        "EFV": ("Ranking por EFV", 'EFV (R$)', ['Ticker', 'Empresa', 'EFV (R$)'])
    }
    
    tab_names = [config[0] for config in rankings.values()]
    tabs = st.tabs(tab_names)
    
    for i, (nome_ranking, (titulo, coluna_sort, colunas_view)) in enumerate(rankings.items()):
        with tabs[i]:
            df_sorted = df_final.sort_values(by=coluna_sort, ascending=False).reset_index(drop=True)
            df_display = df_sorted[colunas_view].head(20).copy()
            for col in df_display.columns:
                if 'R$' in col:
                    df_display[col] = df_display[col].apply(lambda x: f'R$ {x:,.2f}' if pd.notna(x) else 'N/A')
                if '%' in col:
                    df_display[col] = df_display[col].apply(lambda x: f'{x:.2f}%' if pd.notna(x) else 'N/A')
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            csv = convert_df_to_csv(df_sorted[colunas_view])
            st.download_button(label=f"📥 Baixar Ranking Completo (.csv)", data=csv, file_name=f'ranking_{nome_ranking.lower()}.csv', mime='text/csv',)

def ui_valuation():
    """Renderiza a interface completa da aba de Valuation."""
    st.header("Análise de Valuation e Scanner de Mercado")
    tab_individual, tab_ranking = st.tabs(["Análise de Ativo Individual", "🔍 Scanner de Mercado (Ranking)"])
    
    ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
    if ticker_cvm_map_df.empty:
        st.error("Não foi possível carregar o mapeamento de tickers."); st.stop()
    
    with tab_individual:
        with st.form(key='individual_analysis_form'):
            col1, col2 = st.columns([3, 1])
            with col1:
                lista_tickers = sorted(ticker_cvm_map_df['TICKER'].unique())
                ticker_selecionado = st.selectbox("Selecione o Ticker da Empresa", options=lista_tickers, index=lista_tickers.index('PETR4'))
            with col2:
                analisar_btn = st.form_submit_button("Analisar Empresa", type="primary", use_container_width=True)
        
        with st.expander("Opções Avançadas de Valuation", expanded=False):
            col_params_1, col_params_2, col_params_3 = st.columns(3)
            with col_params_1:
                p_taxa_cresc = st.slider("Taxa de Crescimento na Perpetuidade (%)", 0.0, 10.0, CONFIG["TAXA_CRESCIMENTO_PERPETUIDADE"] * 100, 0.5) / 100
            with col_params_2:
                p_media_anos = st.number_input("Anos para Média de NOPAT/FCO", 1, CONFIG["HISTORICO_ANOS_CVM"], CONFIG["MEDIA_ANOS_CALCULO"])
            with col_params_3:
                p_periodo_beta = st.selectbox("Período para Cálculo do Beta", options=["1y", "2y", "5y", "10y"], index=2, key="beta_individual")
        
        if analisar_btn:
            demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
            market_data = obter_dados_mercado(p_periodo_beta)
            ticker_sa = f"{ticker_selecionado}.SA"
            codigo_cvm_info = ticker_cvm_map_df[ticker_cvm_map_df['TICKER'] == ticker_selecionado]
            
            if codigo_cvm_info.empty:
                st.error(f"Não foi possível encontrar o código CVM para o ticker {ticker_selecionado}.")
                st.stop()
                
            codigo_cvm = int(codigo_cvm_info.iloc[0]['CD_CVM'])
            
            params_analise = {
                'taxa_crescimento_perpetuidade': p_taxa_cresc,
                'media_anos_calculo': p_media_anos,
                'periodo_beta_ibov': p_periodo_beta,
            }

            with st.spinner(f"Analisando {ticker_selecionado}..."):
                resultados, status_msg = processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params_analise)
                
            if resultados:
                st.success(f"Análise para **{resultados['Empresa']} ({resultados['Ticker']})** concluída!")
                col1, col2, col3 = st.columns(3)
                col1.metric("Preço Atual", f"R$ {resultados['Preço Atual (R$)']:.2f}"); col2.metric("Preço Justo (DCF)", f"R$ {resultados['Preço Justo (R$)']:.2f}")
                ms_delta = resultados['Margem Segurança (%)']; col3.metric("Margem de Segurança", f"{ms_delta:.2f}%", delta=f"{ms_delta:.2f}%" if not pd.isna(ms_delta) else None)
                st.divider()

                with st.expander("📊 Gráficos de Histórico e Indicadores", expanded=True):
                    # Gráfico de NOPAT e FCO
                    df_nopat_fco = pd.DataFrame({
                        'NOPAT': resultados['hist_nopat'],
                        'FCO': resultados['hist_fco']
                    }).reset_index(names=['Ano'])
                    
                    fig_nopat_fco = go.Figure()
                    fig_nopat_fco.add_trace(go.Bar(x=df_nopat_fco['Ano'], y=df_nopat_fco['NOPAT'], name='NOPAT', marker_color='#00F6FF'))
                    fig_nopat_fco.add_trace(go.Bar(x=df_nopat_fco['Ano'], y=df_nopat_fco['FCO'], name='FCO', marker_color='#E94560'))
                    fig_nopat_fco.update_layout(title='Histórico de NOPAT e FCO', barmode='group', template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='var(--text-color)'))
                    st.plotly_chart(fig_nopat_fco, use_container_width=True)

                    st.divider()
                    
                    # Gráfico de ROIC vs WACC
                    df_roic_wacc = pd.DataFrame({
                        'ROIC': resultados['hist_roic'],
                        'WACC': resultados['wacc_series']
                    }).reset_index(names=['Ano'])
                    
                    fig_roic_wacc = go.Figure()
                    fig_roic_wacc.add_trace(go.Scatter(x=df_roic_wacc['Ano'], y=df_roic_wacc['ROIC'], mode='lines+markers', name='ROIC (%)', line=dict(color='#00FF87', width=3)))
                    fig_roic_wacc.add_trace(go.Scatter(x=df_roic_wacc['Ano'], y=df_roic_wacc['WACC'], mode='lines+markers', name='WACC (%)', line=dict(color='#E94560', width=3)))
                    fig_roic_wacc.update_layout(title='ROIC vs WACC (Indicadores de Criação de Valor)', template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='var(--text-color)'))
                    st.plotly_chart(fig_roic_wacc, use_container_width=True)

                    st.divider()

                    # Novo Gráfico de Evolução de Riqueza e EVA/EFV
                    df_evolucao = pd.DataFrame({
                        'Riqueza Futura %': resultados['hist_riqueza_futura_percentual'],
                        'Riqueza Atual %': resultados['hist_riqueza_atual_percentual'],
                        'EFV %': resultados['hist_efv_percentual'],
                        'EVA %': resultados['hist_eva_percentual']
                    }).reset_index(names=['Ano'])
                    
                    fig_evolucao = go.Figure()
                    fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Ano'], y=df_evolucao['Riqueza Futura %'], mode='lines+markers', name='Riqueza Futura %', line=dict(color='red', width=3)))
                    fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Ano'], y=df_evolucao['Riqueza Atual %'], mode='lines+markers', name='Riqueza Atual %', line=dict(color='green', width=3)))
                    fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Ano'], y=df_evolucao['EFV %'], mode='lines+markers', name='EFV %', line=dict(color='blue', dash='dash', width=2)))
                    fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Ano'], y=df_evolucao['EVA %'], mode='lines+markers', name='EVA %', line=dict(color='cyan', dash='dash', width=2)))
                    
                    fig_evolucao.update_layout(
                        title='Evolução da Riqueza, EFV e EVA na empresa Y', 
                        template="plotly_dark", 
                        paper_bgcolor='rgba(0,0,0,0)', 
                        plot_bgcolor='rgba(0,0,0,0)', 
                        font=dict(color='var(--text-color)'),
                        yaxis_title='Valores em Percentual (%)'
                    )
                    st.plotly_chart(fig_evolucao, use_container_width=True)


                with st.expander("🔢 Detalhes e Direcionadores de Valor", expanded=True):
                    st.subheader("Direcionadores de Valor")
                    
                    # Tabela com Direcionadores de Valor
                    direcionadores_operacionais = {
                        "Crescimento das Vendas (último ano)": f"{resultados['Crescimento Vendas (%)']:.2f}%",
                        "Margem de Lucro (último ano)": f"{resultados['Margem de Lucro (%)']:.2f}%",
                        "Prazo de Cobrança": f"{resultados['Prazo Cobrança (dias)']:.0f} dias",
                        "Prazo de Pagamento": f"{resultados['Prazo Pagamento (dias)']:.0f} dias",
                        "Giro dos Estoques": f"{resultados['Giro Estoques (vezes)']:.2f}x",
                    }
                    direcionadores_financiamento = {
                        "Custo do Capital Próprio (Ke)": f"{resultados['ke']*100:.2f}%",
                        "Custo do Capital de Terceiros (Kd)": f"{resultados['kd']*100:.2f}%",
                        "Estrutura de Capital (Dívida/Patrimônio)": f"{resultados['Dívida/Patrimônio']:.2f}",
                        "Beta (Risco Financeiro)": f"{resultados['Beta']:.2f}",
                    }
                    direcionadores_investimento = {
                        "ROIC": f"{resultados['ROIC (%)']:.2f}%",
                        "Capital Empregado": f"R$ {resultados['Capital Empregado (R$)']:.2f}",
                        "EVA": f"R$ {resultados['EVA (R$)']:.2f}",
                        "EFV": f"R$ {resultados['EFV (R$)']:.2f}",
                        "Riqueza Atual": f"R$ {resultados['EVA (R$)'] / (resultados['wacc_series'].iloc[-1]/100):.2f}" if (resultados['wacc_series'].iloc[-1]/100) > 0 else "N/A"
                    }
                    
                    col_op, col_fin, col_inv = st.columns(3)
                    
                    with col_op:
                        st.markdown("**Estratégias Operacionais**")
                        st.table(pd.DataFrame.from_dict(direcionadores_operacionais, orient='index', columns=['Valor']))
                    with col_fin:
                        st.markdown("**Estratégias de Financiamento**")
                        st.table(pd.DataFrame.from_dict(direcionadores_financiamento, orient='index', columns=['Valor']))
                    with col_inv:
                        st.markdown("**Estratégias de Investimento**")
                        st.table(pd.DataFrame.from_dict(direcionadores_investimento, orient='index', columns=['Valor']))
            else:
                st.error(f"Não foi possível analisar {ticker_selecionado}. Motivo: {status_msg}")

    with tab_ranking:
        st.info("Esta análise processa todas as empresas da lista, o que pode levar vários minutos.")
        if st.button("🚀 Iniciar Análise Completa e Gerar Rankings", type="primary", use_container_width=True):
            params_ranking = {'taxa_crescimento_perpetuidade': CONFIG["TAXA_CRESCIMENTO_PERPETUIDADE"], 'media_anos_calculo': CONFIG["MEDIA_ANOS_CALCULO"], 'periodo_beta_ibov': CONFIG["PERIODO_BETA_IBOV"]}
            demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
            market_data = obter_dados_mercado(params_ranking['periodo_beta_ibov'])
            progress_bar = st.progress(0, text="Iniciando análise em lote...")
            resultados_completos = executar_analise_completa(ticker_cvm_map_df, demonstrativos, market_data, params_ranking, progress_bar)
            
            if resultados_completos:
                df_final = pd.DataFrame(resultados_completos)
                st.success(f"Análise completa! {len(df_final)} de {len(ticker_cvm_map_df)} empresas foram processadas com sucesso.")
                exibir_rankings(df_final)
            else:
                st.error("A análise em lote não retornou nenhum resultado válido.")

# ==============================================================================
# ABA 3: MODELO FLEURIET (SEÇÃO CORRIGIDA E ATUALIZADA)
# ==============================================================================

def reclassificar_contas_fleuriet(df_bpa_empresa, df_bpp_empresa, contas_cvm):
    """
    Reclassifica as contas do balanço para o formato do Modelo Fleuriet.
    Isso é crucial para garantir que cada empresa tenha seus próprios valores.
    """
    try:
        # Ativo Circulante Operacional (ACO) = Contas a Receber + Estoques
        contas_a_receber = obter_historico_metrica(df_bpa_empresa, contas_cvm['CONTAS_A_RECEBER'])
        estoques = obter_historico_metrica(df_bpa_empresa, contas_cvm['ESTOQUES'])
        aco = contas_a_receber.add(estoques, fill_value=0)

        # Passivo Circulante Operacional (PCO) = Fornecedores
        pco = obter_historico_metrica(df_bpp_empresa, contas_cvm['FORNECEDORES'])

        # Ativo Permanente (AP) = Ativo Não Circulante
        ap = obter_historico_metrica(df_bpa_empresa, contas_cvm['ATIVO_NAO_CIRCULANTE'])

        # Passivo Não Circulante (PNC)
        pnc = obter_historico_metrica(df_bpp_empresa, contas_cvm['PASSIVO_NAO_CIRCULANTE'])

        # Patrimônio Líquido (PL)
        pl = obter_historico_metrica(df_bpp_empresa, contas_cvm['PATRIMONIO_LIQUIDO'])

        return aco, pco, ap, pl, pnc
    except Exception:
        # Retorna Series vazias em caso de erro para que a empresa seja pulada
        return pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)


def classificar_fleuriet(cdg, ncg, t):
    """
    Classifica a empresa em um dos 6 tipos do modelo Fleuriet, conforme a documentação.
    """
    if cdg > 0 and ncg < 0 and t > 0:
        return "Tipo 1 (Excelente Liquidez)"
    if cdg > 0 and ncg > 0 and t > 0:
        return "Tipo 2 (Sólida e Comum)"
    if cdg > 0 and ncg > 0 and t < 0:
        return "Tipo 3 (Risco de Liquidez)"
    if cdg < 0 and ncg > 0 and t < 0:
        return "Tipo 4 (Alto Risco Financeiro)"
    if cdg < 0 and ncg < 0 and t < 0:
        return "Tipo 5 (Vulnerável a Fornecedores)"
    if cdg < 0 and ncg < 0 and t > 0:
        return "Tipo 6 (Incomum, NCG financia PNC)"
    return "Indefinido"


def processar_analise_fleuriet(ticker_sa, codigo_cvm, demonstrativos):
    """
    Processa a análise de saúde financeira pelos modelos Fleuriet e Z-Score de Prado.
    Esta função foi robustecida para evitar erros e garantir cálculos corretos.
    """
    try:
        C = CONFIG['CONTAS_CVM']
        bpa = demonstrativos.get('bpa', pd.DataFrame())
        bpp = demonstrativos.get('bpp', pd.DataFrame())
        dre = demonstrativos.get('dre', pd.DataFrame())

        empresa_bpa = bpa[bpa['CD_CVM'] == codigo_cvm] if not bpa.empty else pd.DataFrame()
        empresa_bpp = bpp[bpp['CD_CVM'] == codigo_cvm] if not bpp.empty else pd.DataFrame()
        empresa_dre = dre[dre['CD_CVM'] == codigo_cvm] if not dre.empty else pd.DataFrame()
        
        if any(df.empty for df in [empresa_bpa, empresa_bpp, empresa_dre]):
            return None # Pula a empresa se dados essenciais faltam

        # Utiliza a nova função para garantir a reclassificação correta por empresa
        aco, pco, ap, pl, pnc = reclassificar_contas_fleuriet(empresa_bpa, empresa_bpp, C)
        
        if any(s.empty for s in [aco, pco, ap, pl, pnc]):
            return None # Pula se a reclassificação falhar

        # Cálculos do Modelo Fleuriet
        ncg = aco.subtract(pco, fill_value=0)
        cdg = pl.add(pnc, fill_value=0).subtract(ap, fill_value=0)
        t = cdg.subtract(ncg, fill_value=0)
        
        if any(s.empty for s in [t, ncg, cdg]):
            return None # Pula se os cálculos principais falharem

        # Verificação do Efeito Tesoura
        efeito_tesoura = False
        if len(ncg) > 1 and len(cdg) > 1:
            # fillna(0) previne erros com valores NaN
            cresc_ncg = ncg.pct_change().iloc[-1]
            cresc_cdg = cdg.pct_change().iloc[-1]
            if pd.notna(cresc_ncg) and pd.notna(cresc_cdg) and cresc_ncg > cresc_cdg and t.iloc[-1] < 0:
                efeito_tesoura = True
        
        # Bloco de busca de dados de mercado para o Z-Score
        info = yf.Ticker(ticker_sa).info
        market_cap = info.get('marketCap')
        if not market_cap: # Se não encontrar market cap, não pode calcular Z-Score
            return None
        
        ativo_total_hist = obter_historico_metrica(empresa_bpa, C['ATIVO_TOTAL'])
        passivo_total_hist = obter_historico_metrica(empresa_bpp, C['PASSIVO_TOTAL'])
        ebit_hist = obter_historico_metrica(empresa_dre, C['EBIT'])
        vendas_hist = obter_historico_metrica(empresa_dre, C['RECEITA_LIQUIDA'])

        # Garante que temos todos os dados para o Z-Score
        if any(s.empty for s in [ativo_total_hist, passivo_total_hist, ebit_hist, vendas_hist, pl]):
            return None

        # Pega o último valor de cada série
        ativo_total = ativo_total_hist.iloc[-1]
        passivo_total = passivo_total_hist.iloc[-1]
        ebit = ebit_hist.iloc[-1]
        vendas = vendas_hist.iloc[-1]
        lucro_retido = pl.iloc[-1] - pl.iloc[0] if len(pl) > 1 else 0
        
        # Evita divisão por zero
        if ativo_total == 0 or passivo_total == 0:
            return None
            
        # Variáveis do Z-Score de Prado
        X1 = cdg.iloc[-1] / ativo_total
        X2 = lucro_retido / ativo_total
        X3 = ebit / ativo_total
        X4 = market_cap / passivo_total
        X5 = vendas / ativo_total
        
        z_score = 0.038*X1 + 1.253*X2 + 2.331*X3 + 0.511*X4 + 0.824*X5
        
        if z_score < 1.81: classificacao = "Risco Elevado"
        elif z_score < 2.99: classificacao = "Zona Cinzenta"
        else: classificacao = "Saudável"
            
        # Classificação final do balanço
        tipo_fleuriet = classificar_fleuriet(cdg.iloc[-1], ncg.iloc[-1], t.iloc[-1])

        return {
            'Ticker': ticker_sa.replace('.SA', ''), 
            'Empresa': info.get('longName', ticker_sa), 
            'Ano': t.index[-1], 
            'NCG': ncg.iloc[-1], 
            'CDG': cdg.iloc[-1], 
            'Tesouraria': t.iloc[-1], 
            'Tipo Fleuriet': tipo_fleuriet, # Nova coluna
            'Efeito Tesoura': efeito_tesoura, 
            'Z-Score': z_score, 
            'Classificação Risco': classificacao
        }

    except Exception:
        # Se qualquer parte do processo falhar, retorna None para não quebrar a análise em lote.
        return None

def ui_modelo_fleuriet():
    """Renderiza a interface completa da aba do Modelo Fleuriet."""
    st.header("Análise de Saúde Financeira (Modelo Fleuriet & Z-Score)")
    st.info("""
    Esta análise utiliza os dados da CVM para avaliar a estrutura de capital de giro e o risco de insolvência das empresas.
    **Nota:** O número de empresas processadas com sucesso pode ser menor que o total, pois empresas sem dados financeiros completos ou sem capitalização de mercado são descartadas.
    """)
    
    if st.button("🚀 Iniciar Análise Fleuriet Completa", type="primary", use_container_width=True):
        ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
        demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
        
        if not demonstrativos:
            st.error("Não foi possível baixar os dados da CVM. A análise não pode continuar.")
            st.stop()

        resultados_fleuriet = []
        progress_bar = st.progress(0, text="Iniciando análise Fleuriet...")
        total_empresas = len(ticker_cvm_map_df)
        
        for i, (index, row) in enumerate(ticker_cvm_map_df.iterrows()):
            ticker = row['TICKER']
            progress_bar.progress((i + 1) / total_empresas, text=f"Analisando {i+1}/{total_empresas}: {ticker}")
            # A função de processamento agora é mais robusta
            resultado = processar_analise_fleuriet(f"{ticker}.SA", int(row['CD_CVM']), demonstrativos)
            if resultado:
                resultados_fleuriet.append(resultado)
                
        progress_bar.empty()
        
        if resultados_fleuriet:
            df_fleuriet = pd.DataFrame(resultados_fleuriet)
            st.success(f"Análise Fleuriet concluída para {len(df_fleuriet)} de {total_empresas} empresas.")
            
            ncg_medio = df_fleuriet['NCG'].mean()
            tesoura_count = df_fleuriet['Efeito Tesoura'].sum()
            risco_count = len(df_fleuriet[df_fleuriet['Classificação Risco'] == "Risco Elevado"])
            zscore_medio = df_fleuriet['Z-Score'].mean()
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("NCG Média", f"R$ {ncg_medio/1e6:.1f} M")
            col2.metric("Efeito Tesoura", f"{tesoura_count} empresas")
            col3.metric("Alto Risco (Z-Score)", f"{risco_count} empresas")
            col4.metric("Z-Score Médio", f"{zscore_medio:.2f}")
            
            # Exibe a tabela com a nova coluna de classificação
            st.dataframe(df_fleuriet[['Ticker', 'Empresa', 'NCG', 'CDG', 'Tesouraria', 'Tipo Fleuriet', 'Efeito Tesoura', 'Z-Score', 'Classificação Risco']], use_container_width=True)
            
            # Botão de download
            csv_fleuriet = convert_df_to_csv(df_fleuriet)
            st.download_button(
                label="📥 Baixar Resultados Completos (.csv)",
                data=csv_fleuriet,
                file_name='analise_fleuriet_completa.csv',
                mime='text/csv',
            )
            
        else:
            st.error("Nenhum resultado pôde ser gerado para a análise Fleuriet. Verifique a conexão e os dados da CVM.")
            
    with st.expander("📖 Metodologia e Tipos de Balanço"):
        st.markdown("""
        ### Fórmulas Base
        - **NCG (Necessidade de Capital de Giro):** `Ativo Circulante Operacional - Passivo Circulante Operacional`
        - **CDG (Capital de Giro):** `(Patrimônio Líquido + Passivo Não Circulante) - Ativo Permanente`
        - **T (Saldo de Tesouraria):** `CDG - NCG`
        - **Efeito Tesoura:** Ocorre quando a NCG cresce mais que o CDG, "comendo" a tesouraria.
        
        ### Classificação das Estruturas de Balanço
        - **Tipo 1 (CDG+, NCG-, T+):** Excelente liquidez. Fontes permanentes e ciclo financeiro geram caixa. Ex: Amazon.
        - **Tipo 2 (CDG+, NCG+, T+):** Sólida e mais comum. Fontes permanentes financiam ativos e a NCG, sobrando caixa.
        - **Tipo 3 (CDG+, NCG+, T-):** Risco de liquidez. Fontes permanentes não cobrem toda a NCG, dependendo de crédito de curto prazo.
        - **Tipo 4 (CDG-, NCG+, T-):** Alto risco. Dívidas de curto prazo financiam ativos permanentes e NCG. Muito vulnerável. Ex: OGX.
        - **Tipo 5 (CDG-, NCG-, T-):** Vulnerável. Depende de dívidas de curto prazo e do crédito de fornecedores (NCG negativa).
        - **Tipo 6 (CDG-, NCG-, T+):** Incomum. A NCG negativa é tão grande que financia parte dos ativos não circulantes e ainda gera caixa.
        """)
        
# ==============================================================================
# ABA 4: MODELO BLACK-SCHOLES
# ==============================================================================

@st.cache_data
def calcular_volatilidade_historica(ticker, periodo="1y"):
    """Calcula a volatilidade histórica anualizada de um ativo."""
    try:
        dados = get_stock_data(ticker, period=periodo)
        if dados is None or dados.empty:
            return None
        dados['log_retorno'] = np.log(dados['close'] / dados['close'].shift(1))
        # 252 dias de pregão em um ano
        volatilidade_anualizada = dados['log_retorno'].std() * np.sqrt(252)
        return volatilidade_anualizada
    except (RetryError, Exception):
        return None

@st.cache_data
def buscar_opcoes(ticker, vencimento):
    """Busca a cadeia de opções para um ticker e vencimento específicos."""
    try:
        url = f'https://opcoes.net.br/listaopcoes/completa?idAcao={ticker}&listarVencimentos=false&cotacoes=true&vencimentos={vencimento}'
        response = requests_retry_session().get(url, timeout=20)
        response.raise_for_status()
        dados = response.json()
        if 'data' in dados and 'cotacoesOpcoes' in dados['data']:
            opcoes = [[ticker, vencimento, i[0].split('_')[0], i[2], i[3], i[5], i[8]] for i in dados['data']['cotacoesOpcoes']]
            df = pd.DataFrame(opcoes, columns=['ativo_obj', 'vencimento', 'ticker', 'tipo', 'modelo', 'strike', 'preco_mercado'])
            df['strike'] = pd.to_numeric(df['strike'])
            df['preco_mercado'] = pd.to_numeric(df['preco_mercado'])
            return df
        else:
            return pd.DataFrame()
    except (RetryError, requests.exceptions.RequestException) as e:
        st.error(f"Erro ao buscar dados de opções: {e}")
        return pd.DataFrame()

def black_scholes(S, K, T, r, sigma, option_type="call"):
    """Calcula o preço de uma opção usando o modelo Black-Scholes."""
    if T <= 0 or sigma <= 0: return 0
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type.lower() == "call":
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif option_type.lower() == "put":
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    return 0

def calcular_greeks(S, K, T, r, sigma, option_type="call"):
    """Calcula as Greeks de uma opção."""
    greeks = {'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0, 'rho': 0}
    if T <= 0 or sigma <= 0: return greeks
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    greeks['gamma'] = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    greeks['vega'] = S * norm.pdf(d1) * np.sqrt(T) / 100 # Dividido por 100 para representar a mudança por 1% na vol
    
    if option_type.lower() == "call":
        greeks['delta'] = norm.cdf(d1)
        greeks['theta'] = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * norm.cdf(d2)) / 365
        greeks['rho'] = K * T * np.exp(-r * T) * norm.cdf(d2) / 100
    elif option_type.lower() == "put":
        greeks['delta'] = norm.cdf(d1) - 1
        greeks['theta'] = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * norm.cdf(-d2)) / 365
        greeks['rho'] = -K * T * np.exp(-r * T) * norm.cdf(-d2) / 100
        
    return greeks

@st.cache_data
def analise_tecnica_ativo(ticker, timeframe='daily', weekly_bias=0, thresholds=None):
    """
    Realiza a análise técnica completa e retorna um score de convergência.
    Versão robustecida para evitar KeyErrors e lidar com dados insuficientes.
    """
    if thresholds is None:
        thresholds = {'forte': 0.7, 'normal': 0.2}

    try:
        # 1. Obtenção de Dados
        if timeframe == 'weekly':
            df = get_stock_data(ticker, period="5y", interval="1wk")
        else:
            df = get_stock_data(ticker, period="2y", interval="1d")

        if df is None or df.empty:
            return "Dados Insuficientes", 0, {"Erro": "Dados históricos indisponíveis."}, "NEUTRO"

        # Garante que não há MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)

        # 2. Validação de Dados de Entrada
        min_periods_required = 20  # O indicador com maior período é o BBands(20)
        if len(df) < min_periods_required:
            msg = f"São necessários pelo menos {min_periods_required} pontos de dados, mas apenas {len(df)} foram encontrados."
            return "Dados Insuficientes", 0, {"Erro": msg}, "NEUTRO"

        # 3. Cálculo dos Indicadores
        MyStrategy = ta.Strategy(
            name="Convergencia_Opcoes",
            description="RSI, MACD, BBANDS, EMA, ADX, STOCH, PSAR",
            ta=[
                {"kind": "rsi"}, {"kind": "macd"}, {"kind": "bbands", "length": 20},
                {"kind": "ema", "length": 9}, {"kind": "ema", "length": 21},
                {"kind": "adx"}, {"kind": "stoch"}, {"kind": "psar"},
            ]
        )
        df.ta.strategy(MyStrategy)

        if df.empty or 'RSI_14' not in df.columns:
            return "Erro de Cálculo", 0, {"Erro": "Não foi possível calcular os indicadores técnicos."}, "NEUTRO"

        last = df.iloc[-1]
        sinais = {}
        valores_indicadores = {}

        # 4. Extração Segura dos Sinais
        if 'RSI_14' in last and pd.notna(last['RSI_14']):
            rsi_val = last['RSI_14']
            valores_indicadores['RSI'] = f"{rsi_val:.1f}"
            if rsi_val < 30: sinais['RSI'] = 1
            elif rsi_val > 70: sinais['RSI'] = -1
            else: sinais['RSI'] = 0
        else:
            sinais['RSI'] = 0; valores_indicadores['RSI'] = "N/A"

        if 'MACD_12_26_9' in last and 'MACDs_12_26_9' in last and pd.notna(last['MACD_12_26_9']) and pd.notna(last['MACDs_12_26_9']):
            valores_indicadores['MACD'] = f"{last['MACD_12_26_9']:.2f}"
            if last['MACD_12_26_9'] > last['MACDs_12_26_9']: sinais['MACD'] = 1
            else: sinais['MACD'] = -1
        else:
            sinais['MACD'] = 0; valores_indicadores['MACD'] = "N/A"

        if 'BBU_20_2.0' in last and 'BBL_20_2.0' in last and pd.notna(last['BBU_20_2.0']) and pd.notna(last['BBL_20_2.0']):
            bbu, bbl, close = last['BBU_20_2.0'], last['BBL_20_2.0'], last['close']
            if (bbu - bbl) > 0:
                bbp = (close - bbl) / (bbu - bbl)
                valores_indicadores['Bandas de Bollinger (%B)'] = f"{bbp:.2f}"
            else:
                valores_indicadores['Bandas de Bollinger (%B)'] = "N/A"
            if close < bbl: sinais['BOLLINGER'] = 1
            elif close > bbu: sinais['BOLLINGER'] = -1
            else: sinais['BOLLINGER'] = 0
        else:
            sinais['BOLLINGER'] = 0; valores_indicadores['Bandas de Bollinger (%B)'] = "N/A"

        if 'EMA_9' in last and 'EMA_21' in last and pd.notna(last['EMA_9']) and pd.notna(last['EMA_21']):
            valores_indicadores['EMA (9 vs 21)'] = "Cruz. Alta" if last['EMA_9'] > last['EMA_21'] else "Cruz. Baixa"
            if last['EMA_9'] > last['EMA_21']: sinais['EMA'] = 1
            else: sinais['EMA'] = -1
        else:
            sinais['EMA'] = 0; valores_indicadores['EMA (9 vs 21)'] = "N/A"

        if timeframe == 'weekly':
            weekly_bias_signal = "Alta" if sinais.get('EMA', 0) > 0 and sinais.get('MACD', 0) > 0 else ("Baixa" if sinais.get('EMA', 0) < 0 and sinais.get('MACD', 0) < 0 else "Neutro")
            return "Viés Semanal", 0, valores_indicadores, weekly_bias_signal

        if 'ADX_14' in last and 'DMP_14' in last and 'DMN_14' in last and all(pd.notna(last[c]) for c in ['ADX_14', 'DMP_14', 'DMN_14']):
            adx_val = last['ADX_14']
            valores_indicadores['ADX'] = f"{adx_val:.1f}"
            if adx_val > 25 and last['DMP_14'] > last['DMN_14']: sinais['ADX'] = 1
            elif adx_val > 25 and last['DMN_14'] > last['DMP_14']: sinais['ADX'] = -1
            else: sinais['ADX'] = 0
        else:
            sinais['ADX'] = 0; valores_indicadores['ADX'] = "N/A"

        if 'STOCHk_14_3_3' in last and pd.notna(last['STOCHk_14_3_3']):
            stoch_val = last['STOCHk_14_3_3']
            valores_indicadores['Estocástico'] = f"{stoch_val:.1f}"
            if stoch_val < 20: sinais['STOCH'] = 1
            elif stoch_val > 80: sinais['STOCH'] = -1
            else: sinais['STOCH'] = 0
        else:
            sinais['STOCH'] = 0; valores_indicadores['Estocástico'] = "N/A"

        if 'PSARl_0.02_0.2' in last and 'PSARs_0.02_0.2' in last:
            if pd.notna(last['PSARl_0.02_0.2']):
                sinais['SAR'] = 1; valores_indicadores['SAR Parabólico'] = "Alta"
            elif pd.notna(last['PSARs_0.02_0.2']):
                sinais['SAR'] = -1; valores_indicadores['SAR Parabólico'] = "Baixa"
            else:
                sinais['SAR'] = 0; valores_indicadores['SAR Parabólico'] = "Neutro"
        else:
            sinais['SAR'] = 0; valores_indicadores['SAR Parabólico'] = "N/A"

        # 5. Cálculo do Score e Sinal Final
        pesos = {'RSI': 0.20, 'MACD': 0.20, 'BOLLINGER': 0.15, 'EMA': 0.15, 'ADX': 0.10, 'STOCH': 0.08, 'SAR': 0.07}
        score = sum(pesos.get(ind, 0) * valor for ind, valor in sinais.items())
        score_ajustado = score + (0.15 * weekly_bias)
        
        tendencia_alta = sinais.get('MACD', 0) > 0 or sinais.get('EMA', 0) > 0
        momento_alta = sinais.get('RSI', 0) > 0 or sinais.get('STOCH', 0) > 0
        tendencia_baixa = sinais.get('MACD', 0) < 0 or sinais.get('EMA', 0) < 0
        momento_baixa = sinais.get('RSI', 0) < 0 or sinais.get('STOCH', 0) < 0

        if score_ajustado > thresholds['forte'] and tendencia_alta and momento_alta: sinal_final = "COMPRA FORTE"
        elif score_ajustado > thresholds['normal']: sinal_final = "COMPRA"
        elif score_ajustado < -thresholds['forte'] and tendencia_baixa and momento_baixa: sinal_final = "VENDA FORTE"
        elif score_ajustado < -thresholds['normal']: sinal_final = "VENDA"
        else: sinal_final = "NEUTRO"

        # 6. Coleta Segura de Dados Brutos para Depuração (A CAUSA DO ERRO ORIGINAL)
        raw_data_cols = ['close', 'RSI_14', 'MACD_12_26_9', 'MACDs_12_26_9', 'BBL_20_2.0', 'BBU_20_2.0', 'EMA_9', 'EMA_21']
        existing_cols = [col for col in raw_data_cols if col in df.columns]
        if existing_cols:
            valores_indicadores['raw_data'] = df[existing_cols].tail(10)

        return sinal_final, score_ajustado, valores_indicadores, "N/A"

    except Exception as e:
        # Captura qualquer erro inesperado no processo
        return "Erro", 0, {"Erro": f"Falha geral na análise técnica: {str(e)}"}, "NEUTRO"


def gerar_analise_avancada(row, vies_fundamental, sinal_tecnico, vies_semanal):
    """Gera uma recomendação de texto para uma opção, integrando todas as análises."""
    diff_percent = row['Diferença (%)']
    tipo = row['Tipo']
    
    subvalorizada = diff_percent <= -20
    
    recomendacao_final = "Aguardar"
    analise_texto = ""

    # Cenários para CALLs
    if tipo == 'CALL':
        if vies_fundamental == "Alta" and "COMPRA" in sinal_tecnico and vies_semanal == "Alta" and subvalorizada:
            recomendacao_final = "Compra Forte de CALL"
            analise_texto = "Convergência total: O ativo está subvalorizado (fundamental), a tendência semanal é de alta, o sinal técnico diário é forte e esta opção está barata. Cenário ideal para uma compra de CALL."
        elif vies_fundamental == "Alta" and "COMPRA" in sinal_tecnico and vies_semanal != "Baixa":
            recomendacao_final = "Compra de CALL"
            analise_texto = "Sinais alinhados: O viés fundamentalista e técnico são de alta, com tendência semanal favorável. Boa oportunidade para uma compra de CALL."
        elif vies_fundamental == "Alta" and "VENDA" in sinal_tecnico:
            recomendacao_final = "Aguardar (Conflito)"
            analise_texto = "Sinais conflitantes: O ativo está subvalorizado no longo prazo (fundamental), mas a tendência técnica de curto prazo é de baixa. Comprar uma CALL agora seria ir contra a maré. Aguarde a reversão da tendência técnica."
        else:
            recomendacao_final = "Não Recomendado"
            analise_texto = "A operação não é recomendada. Os sinais fundamentalista, técnico ou de tendência semanal não suportam uma estratégia de alta para esta CALL no momento."

    # Cenários para PUTs
    if tipo == 'PUT':
        if vies_fundamental == "Baixa" and "VENDA" in sinal_tecnico and vies_semanal == "Baixa" and subvalorizada:
            recomendacao_final = "Compra Forte de PUT"
            analise_texto = "Convergência total: O ativo está sobrevalorizado (fundamental), a tendência semanal é de baixa, o sinal técnico diário é forte e esta opção está barata. Cenário ideal para uma compra de PUT."
        elif vies_fundamental == "Baixa" and "VENDA" in sinal_tecnico and vies_semanal != "Alta":
            recomendacao_final = "Compra de PUT"
            analise_texto = "Sinais alinhados: O viés fundamentalista e técnico são de baixa, com tendência semanal favorável. Boa oportunidade para uma compra de PUT."
        elif vies_fundamental == "Baixa" and "COMPRA" in sinal_tecnico:
            recomendacao_final = "Aguardar (Conflito)"
            analise_texto = "Sinais conflitantes: O ativo está sobrevalorizado no longo prazo (fundamental), mas a tendência técnica de curto prazo é de alta. Comprar uma PUT agora seria arriscado. Aguarde a reversão da tendência técnica."
        else:
            recomendacao_final = "Não Recomendado"
            analise_texto = "A operação não é recomendada. Os sinais fundamentalista, técnico ou de tendência semanal não suportam uma estratégia de baixa para esta PUT no momento."

    return recomendacao_final, analise_texto


def ui_black_scholes():
    """Renderiza a interface da aba Black-Scholes."""
    st.header("Precificação de Opções e Análise Avançada")
    st.info("""
    **Como funciona o vencimento de opções no Brasil?**
    As opções na B3 (bolsa brasileira) vencem sempre na **terceira sexta-feira de cada mês**. 
    Para encontrar opções com liquidez, escolha uma data de vencimento futura que corresponda a uma terceira sexta-feira.
    """)
    
    ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
    lista_tickers = sorted(ticker_cvm_map_df['TICKER'].unique())
    
    with st.form("black_scholes_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            ticker_selecionado = st.selectbox("Selecione o Ativo Objeto", options=lista_tickers, index=lista_tickers.index('PETR4'))
        with col2:
            data_vencimento = st.date_input("Data de Vencimento", value=datetime.today() + pd.Timedelta(days=30), format="DD/MM/YYYY")
        with col3:
            st.write("") # Espaçamento
            st.write("") # Espaçamento
            analisar_opcoes_btn = st.form_submit_button("Analisar Opções", use_container_width=True)

    with st.expander("Opções Avançadas de Análise Técnica", expanded=False):
        st.markdown("""
        Esta seção permite ajustar a sensibilidade do modelo de análise técnica. Os limiares definem o quão forte a pontuação dos indicadores precisa ser para gerar um sinal de compra ou venda.

        - **Limiar para Sinal FORTE:** Define a pontuação mínima para um sinal ser considerado "Forte". Requer que múltiplos indicadores de tendência e momento estejam alinhados.
        - **Limiar para Sinal NORMAL:** Define a pontuação mínima para um sinal "Normal".

        **Como ajustar:**
        - **Valores mais altos** (ex: 0.8 para Forte) tornam o modelo **mais seletivo e exigente**, gerando menos sinais, porém mais confiáveis.
        - **Valores mais baixos** (ex: 0.4 para Forte) tornam o modelo **mais sensível**, gerando mais sinais, que podem incluir mais "falsos positivos".
        
        *Obs: Os valores são independentes e não precisam somar 1.*
        """)
        col_t1, col_t2 = st.columns(2)
        with col_t1:
            threshold_forte = st.slider("Limiar para Sinal FORTE", 0.1, 1.0, 0.65, 0.05)
        with col_t2:
            threshold_normal = st.slider("Limiar para Sinal NORMAL", 0.1, 1.0, 0.25, 0.05)
        
        debug_mode = st.checkbox("Ativar Modo de Depuração da Análise Técnica")
        
        thresholds_config = {'forte': threshold_forte, 'normal': threshold_normal}

    if analisar_opcoes_btn:
        ticker_sa = f"{ticker_selecionado}.SA"
        
        with st.spinner(f"Realizando análise completa para {ticker_selecionado}..."):
            try:
                # 1. Análise Fundamentalista (Valuation)
                codigo_cvm_info = ticker_cvm_map_df[ticker_cvm_map_df['TICKER'] == ticker_selecionado]
                codigo_cvm = int(codigo_cvm_info.iloc[0]['CD_CVM'])
                demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
                market_data = obter_dados_mercado(CONFIG["PERIODO_BETA_IBOV"])
                params_analise = {'taxa_crescimento_perpetuidade': CONFIG["TAXA_CRESCIMENTO_PERPETUIDADE"], 'media_anos_calculo': CONFIG["MEDIA_ANOS_CALCULO"], 'periodo_beta_ibov': CONFIG["PERIODO_BETA_IBOV"]}
                
                resultados_valuation, status_msg = processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params_analise)
                
                if resultados_valuation is None:
                    st.error(f"Falha na Análise Fundamentalista: {status_msg}. A análise de opções não pode continuar.")
                    st.stop()

                if resultados_valuation['Margem Segurança (%)'] > 15:
                    vies_fundamental = "Alta"
                elif resultados_valuation['Margem Segurança (%)'] < -15:
                    vies_fundamental = "Baixa"
                else:
                    vies_fundamental = "Neutro"
                st.session_state['vies_fundamental_bs'] = vies_fundamental

                # 2. Análise Técnica (Multi-Timeframe)
                _, _, _, vies_semanal = analise_tecnica_ativo(ticker_sa, timeframe='weekly')
                st.session_state['vies_semanal_bs'] = vies_semanal
                weekly_bias_value = 1 if vies_semanal == "Alta" else (-1 if vies_semanal == "Baixa" else 0)

                sinal_tecnico, _, detalhes_tecnicos, _ = analise_tecnica_ativo(ticker_sa, timeframe='daily', weekly_bias=weekly_bias_value, thresholds=thresholds_config)
                st.session_state['sinal_tecnico_bs'] = sinal_tecnico
                st.session_state['detalhes_tecnicos_bs'] = detalhes_tecnicos
                
                # 3. Dados de Mercado e Opções
                selic_anual = market_data[0]
                preco_atual_ativo = resultados_valuation['Preço Atual (R$)']
                st.session_state['preco_atual_ativo_bs'] = preco_atual_ativo
                
                vol_historica = calcular_volatilidade_historica(ticker_sa)
                if vol_historica is None: vol_historica = 0.30
                st.session_state['vol_historica_bs'] = vol_historica
                
                vencimento_str = data_vencimento.strftime('%Y-%m-%d')
                df_opcoes = buscar_opcoes(ticker_selecionado, vencimento_str)
                if df_opcoes.empty:
                    st.warning(f"Nenhuma opção encontrada para {ticker_selecionado} com vencimento em {data_vencimento.strftime('%d/%m/%Y')}.")
                    st.stop()
                
                # 4. Cálculos de Black-Scholes
                T = (data_vencimento - date.today()).days / 365.0
                resultados = []
                for _, row in df_opcoes.iterrows():
                    preco_bs = black_scholes(preco_atual_ativo, row['strike'], T, selic_anual, vol_historica, row['tipo'])
                    greeks = calcular_greeks(preco_atual_ativo, row['strike'], T, selic_anual, vol_historica, row['tipo'])
                    diferenca_percentual = ((row['preco_mercado'] - preco_bs) / preco_bs * 100) if preco_bs > 0 else 0
                    
                    res_temp = {'Diferença (%)': diferenca_percentual, 'Tipo': row['tipo'], 'Strike': row['strike']}
                    recomendacao, analise_detalhada = gerar_analise_avancada(res_temp, vies_fundamental, sinal_tecnico, vies_semanal)
                    
                    res = {
                        'Ticker': row['ticker'], 'Tipo': row['tipo'], 'Strike': row['strike'],
                        'Preço Mercado': row['preco_mercado'], 'Preço Teórico (BS)': preco_bs,
                        'Recomendação': recomendacao, 'Análise Detalhada': analise_detalhada,
                        **{k.capitalize(): v for k, v in greeks.items()}
                    }
                    resultados.append(res)
                
                df_resultados = pd.DataFrame(resultados)
                st.session_state['df_resultados_bs'] = df_resultados

            except Exception as e:
                st.error(f"Ocorreu um erro inesperado durante a análise completa: {e}")
                import traceback
                st.error(traceback.format_exc())
                st.stop()

    if 'df_resultados_bs' in st.session_state:
        st.subheader("Diagnóstico do Ativo Subjacente")
        vies_fundamental = st.session_state.get('vies_fundamental_bs', "N/A")
        sinal_tecnico = st.session_state.get('sinal_tecnico_bs', "N/A")
        vies_semanal = st.session_state.get('vies_semanal_bs', "N/A")
        detalhes_tecnicos = st.session_state.get('detalhes_tecnicos_bs', {})
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Viés Fundamentalista (Longo Prazo)", vies_fundamental)
        col2.metric("Viés de Tendência (Semanal)", vies_semanal)
        col3.metric("Sinal Técnico (Diário)", sinal_tecnico)

        with st.expander("Detalhes da Análise Técnica Diária"):
            if isinstance(detalhes_tecnicos, dict) and 'Erro' not in detalhes_tecnicos:
                # Dicionário de interpretações para leigos
                interpretacoes = {
                    'RSI': "Mede a força do movimento. Abaixo de 30 indica 'sobrevenda' (potencial de alta). Acima de 70, 'sobrecompra' (potencial de baixa).",
                    'MACD': "Indica o momento do ativo. Valores positivos sugerem momento de alta; negativos, de baixa.",
                    'Bandas de Bollinger (%B)': "Mostra se o preço está 'caro' ou 'barato'. Abaixo de 0, o preço cruzou a banda inferior (sinal de compra). Acima de 1, cruzou a superior (sinal de venda).",
                    'EMA (9 vs 21)': "Indica a tendência de curto prazo. 'Cruz. Alta' é um sinal otimista; 'Cruz. Baixa' é pessimista.",
                    'ADX': "Mede a força da tendência. Acima de 25 indica uma tendência forte (seja de alta ou baixa). Abaixo de 20, uma tendência fraca ou lateral.",
                    'Estocástico': "Similar ao RSI, mede o momento. Abaixo de 20 é 'sobrevenda' (potencial de alta), acima de 80 é 'sobrecompra' (potencial de baixa).",
                    'SAR Parabólico': "Mostra a direção da tendência. Quando os pontos estão abaixo do preço, a tendência é de alta."
                }
                
                # Prepara os dados para a tabela
                dados_tabela = []
                for indicador, valor in detalhes_tecnicos.items():
                    if indicador != 'raw_data':
                        dados_tabela.append({
                            "Indicador": indicador,
                            "Valor/Sinal": valor,
                            "Interpretação para Leigos": interpretacoes.get(indicador, "Análise de tendência/momento.")
                        })
                
                if dados_tabela:
                    df_tabela = pd.DataFrame(dados_tabela)
                    st.dataframe(df_tabela, use_container_width=True, hide_index=True)
                
                # Se o modo de depuração estiver ativo, mostra os dados brutos
                if debug_mode and 'raw_data' in detalhes_tecnicos:
                    st.markdown("##### Dados Brutos dos Indicadores (Últimos 10 dias)")
                    st.dataframe(detalhes_tecnicos['raw_data'])
            else:
                st.warning(f"Não foi possível exibir os detalhes da análise técnica. Motivo: {detalhes_tecnicos.get('Erro', 'desconhecido')}")

        
        st.divider()

        df_resultados = st.session_state['df_resultados_bs']
        
        st.subheader("Resultados da Análise de Opções")
        
        df_calls = df_resultados[df_resultados['Tipo'] == 'CALL'].copy()
        df_puts = df_resultados[df_resultados['Tipo'] == 'PUT'].copy()

        tab_calls, tab_puts = st.tabs(["Opções de Compra (Calls)", "Opções de Venda (Puts)"])

        def exibir_tabela_e_analise(df, tipo_opcao):
            if df.empty:
                st.info(f"Nenhuma opção de {tipo_opcao} encontrada para este vencimento.")
                return

            st.dataframe(df[['Ticker', 'Strike', 'Preço Mercado', 'Preço Teórico (BS)', 'Recomendação', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']],
                            use_container_width=True, hide_index=True,
                            column_config={
                                "Strike": st.column_config.NumberColumn("Strike", format="R$ %.2f"),
                                "Preço Mercado": st.column_config.NumberColumn("Preço Mercado", format="R$ %.4f"),
                                "Preço Teórico (BS)": st.column_config.NumberColumn("Preço Teórico", format="R$ %.4f"),
                                "Delta": st.column_config.NumberColumn(format="%.3f"),
                                "Gamma": st.column_config.NumberColumn(format="%.3f"),
                                "Vega": st.column_config.NumberColumn(format="%.3f"),
                                "Theta": st.column_config.NumberColumn(format="%.3f"),
                                "Rho": st.column_config.NumberColumn(format="%.3f"),
                            })
            
            st.markdown("---")
            st.markdown("#### 🔍 Análise Detalhada da Opção")
            
            opcoes_disponiveis = df['Ticker'].tolist()
            if opcoes_disponiveis:
                opcao_selecionada = st.selectbox("Selecione uma opção para ver a análise completa:", options=opcoes_disponiveis, key=f"select_{tipo_opcao}")
                analise = df[df['Ticker'] == opcao_selecionada]['Análise Detalhada'].iloc[0]
                st.success(analise)

        with tab_calls:
            exibir_tabela_e_analise(df_calls, "CALL")

        with tab_puts:
            exibir_tabela_e_analise(df_puts, "PUT")
        
        with st.expander("📖 Glossário das Gregas (O que significam?)"):
            st.markdown("""
            As **"Greeks" (Gregas)** são um conjunto de indicadores que medem a sensibilidade do preço de uma opção a diferentes fatores de risco. Entendê-las é fundamental para gerenciar o risco de suas operações.

            - **Delta (Δ):** Mede a velocidade da opção. Indica o quanto o preço da opção tende a mudar para cada R$ 1,00 de variação no preço do ativo-objeto.
              - *Exemplo:* Um Delta de 0.60 significa que, se a ação subir R$ 1,00, o preço da opção de compra (CALL) tende a valorizar R$ 0,60.

            - **Gamma (Γ):** Mede a aceleração do Delta. Mostra o quão rápido o Delta de uma opção muda conforme o preço do ativo-objeto se altera.
              - *Exemplo:* Um Gamma alto significa que o Delta é muito sensível, mudando rapidamente. Isso é comum em opções "no dinheiro" (ATM) e próximas do vencimento.

            - **Vega (ν):** Mede o impacto da volatilidade. Indica o quanto o preço da opção muda para cada 1% de variação na volatilidade do ativo.
              - *Exemplo:* Se você acredita que a volatilidade do mercado vai aumentar, deve procurar opções com Vega positivo e alto, pois elas se beneficiarão mais desse movimento.

            - **Theta (Θ):** Mede o custo do tempo. Indica o quanto o preço da opção perde de valor a cada dia que passa, devido à aproximação do vencimento (decaimento temporal).
              - *Exemplo:* Um Theta de -0.05 significa que a opção perde R$ 0,05 de seu valor extrínseco por dia, mantendo os outros fatores constantes. É o "aluguel" que se paga por manter a posição.

            - **Rho (ρ):** Mede o impacto dos juros. Indica a sensibilidade do preço da opção a uma variação de 1% na taxa de juros livre de risco.
              - *Exemplo:* Geralmente, tem um impacto menor no preço de opções de curto prazo, mas é relevante para opções de longo prazo (LEAPs).
            """)

# ==============================================================================
# ESTRUTURA PRINCIPAL DO APP
# ==============================================================================
def main():
    """Função principal que orquestra o layout do aplicativo Streamlit."""
    st.title("Sistema de Controle Financeiro e Análise de Investimentos")
    inicializar_session_state()
    
    # Abas para navegação entre as diferentes funcionalidades
    tab1, tab2, tab3, tab4 = st.tabs(["💲 Controle Financeiro", "📈 Análise de Valuation", "🔬 Modelo Fleuriet", "🤖 Black-Scholes"])
    
    with tab1:
        ui_controle_financeiro()
        
    with tab2:
        ui_valuation()
        
    with tab3:
        ui_modelo_fleuriet()

    with tab4:
        ui_black_scholes()

if __name__ == "__main__":
    main()
