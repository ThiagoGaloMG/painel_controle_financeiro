# -*- coding: utf-8 -*-
import os
import pandas as pd
import yfinance as yf
import requests
from zipfile import ZipFile
from datetime import datetime
from pathlib import Path
import warnings
import numpy as np
import io
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Ignorar avisos para uma saída mais limpa
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURAÇÕES GERAIS E LAYOUT DA PÁGINA
# ==============================================================================
st.set_page_config(layout="wide", page_title="Painel de Controle Financeiro", page_icon="📈")

# Estilo CSS para um tema escuro e profissional com efeito Neon
st.markdown("""
<style>
    /* Paleta de Cores Neon Profissional */
    :root {
        --primary-bg: #0A0A1A; /* Fundo carvão profundo, quase preto */
        --secondary-bg: #1A1A2E; /* Fundo secundário azul/roxo escuro */
        --widget-bg: #16213E; /* Fundo dos widgets */
        --primary-accent: #00F6FF; /* Ciano neon vibrante */
        --secondary-accent: #E94560; /* Vermelho/rosa neon para contraste */
        --positive-accent: #00FF87; /* Verde neon */
        --text-color: #E0E0E0; /* Cinza claro para texto, menos cansativo */
        --header-color: #FFFFFF; /* Branco puro para títulos */
        --border-color: #5372F0; /* Borda azul sutil */
    }

    body {
        color: var(--text-color);
        background-color: var(--primary-bg);
    }

    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Título com Gradiente Neon */
    h1 {
        background: -webkit-linear-gradient(45deg, var(--primary-accent), var(--positive-accent));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-shadow: 0 0 10px rgba(0, 246, 255, 0.3);
    }
    
    h2, h3 {
        color: var(--header-color);
    }

    /* Abas com Efeito Neon */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: transparent;
        border-bottom: 2px solid var(--secondary-bg);
        transition: all 0.3s;
    }
    .stTabs [aria-selected="true"] {
        color: var(--primary-accent);
        border-bottom: 2px solid var(--primary-accent);
        box-shadow: 0 2px 15px -5px var(--primary-accent);
    }

    /* Métricas com Borda Neon Sutil */
    .stMetric {
        border: 1px solid var(--secondary-bg);
        border-radius: 8px;
        padding: 20px;
        background-color: var(--secondary-bg);
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
    }
    .stMetric > div:nth-child(2) { /* O valor da métrica */
        color: var(--header-color);
    }

    /* Botões com Efeito Neon */
    .stButton > button {
        border-radius: 8px;
        border: 1px solid var(--primary-accent);
        background-color: transparent;
        color: var(--primary-accent);
        transition: all 0.3s ease-in-out;
        box-shadow: 0 0 5px var(--primary-accent);
    }
    .stButton > button:hover {
        background-color: var(--primary-accent);
        color: var(--primary-bg);
        box-shadow: 0 0 20px var(--primary-accent);
    }
    .stButton > button:active {
        transform: scale(0.98);
    }

    /* Expanders */
    [data-testid="stExpander"] {
        background-color: var(--secondary-bg);
        border: 1px solid var(--border-color);
        border-radius: 8px;
    }
    [data-testid="stExpander"] summary {
        font-size: 1.1em;
        font-weight: 600;
        color: var(--header-color);
    }

    /* Barras de progresso */
    [data-testid="stProgressBar"] > div {
        background-image: linear-gradient(90deg, var(--primary-accent), var(--positive-accent));
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

@st.cache_data
def setup_diretorios():
    try:
        CONFIG["DIRETORIO_DADOS_CVM"].mkdir(parents=True, exist_ok=True)
        CONFIG["DIRETORIO_DADOS_EXTRAIDOS"].mkdir(parents=True, exist_ok=True)
        return True
    except Exception:
        return False

@st.cache_data(show_spinner=False)
def preparar_dados_cvm(anos_historico):
    ano_final = datetime.today().year
    ano_inicial = ano_final - anos_historico
    with st.spinner(f"Verificando e baixando dados da CVM de {ano_inicial} a {ano_final-1}..."):
        demonstrativos_consolidados = {}
        tipos_demonstrativos = ['DRE', 'BPA', 'BPP', 'DFC_MI']
        for tipo in tipos_demonstrativos:
            lista_dfs_anuais = []
            for ano in range(ano_inicial, ano_final):
                nome_arquivo_csv = f'dfp_cia_aberta_{tipo}_con_{ano}.csv'
                caminho_arquivo = CONFIG["DIRETORIO_DADOS_EXTRAIDOS"] / nome_arquivo_csv
                if not caminho_arquivo.exists():
                    nome_zip = f'dfp_cia_aberta_{ano}.zip'
                    caminho_zip = CONFIG["DIRETORIO_DADOS_CVM"] / nome_zip
                    url_zip = f'{CONFIG["URL_BASE_CVM"]}{nome_zip}'
                    try:
                        response = requests.get(url_zip, stream=True, timeout=60)
                        response.raise_for_status()
                        with open(caminho_zip, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
                        with ZipFile(caminho_zip, 'r') as z:
                            if nome_arquivo_csv in z.namelist():
                                z.extract(nome_arquivo_csv, CONFIG["DIRETORIO_DADOS_EXTRAIDOS"])
                            else: continue
                    except Exception: continue
                if caminho_arquivo.exists():
                    try:
                        df_anual = pd.read_csv(caminho_arquivo, sep=';', encoding='ISO-8859-1', low_memory=False)
                        lista_dfs_anuais.append(df_anual)
                    except Exception: continue
            if lista_dfs_anuais:
                demonstrativos_consolidados[tipo.lower()] = pd.concat(lista_dfs_anuais, ignore_index=True)
    return demonstrativos_consolidados

@st.cache_data
def carregar_mapeamento_ticker_cvm():
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
24430;BIOM3;BIOMM S.A.
21932;BMGB4;BANCO BMG S.A.
1023;BMIN4;BANCO MERCANTIL DE INVESTIMENTOS S.A.
19615;BMOB3;BEMOBI TECH S.A.
416;BNBR3;BANCO DO NORDESTE DO BRASIL S.A.
21511;BOAS3;BOA VISTA SERVIÇOS S.A.
20382;BPAC11;BANCO BTG PACTUAL S.A.
20382;BPAC5;BANCO BTG PACTUAL S.A.
20695;BPAN4;BANCO PAN S.A.
21649;BRAP4;BRADESPAR S.A.
21657;BRFS3;BRF S.A.
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
22610;COGN3;COGNA EDUCAÇÃO S.A.
20687;CPFE3;CPFL ENERGIA S.A.
21819;CPLE3;COMPANHIA PARANAENSE DE ENERGIA - COPEL
21819;CPLE6;COMPANHIA PARANAENSE DE ENERGIA - COPEL
21819;CPLE11;COMPANHIA PARANAENSE DE ENERGIA - COPEL
21481;CSAN3;COSAN S.A.
14624;CSMG3;COMPANHIA DE SANEAMENTO DE MINAS GERAIS - COPASA
20725;CSNA3;COMPANHIA SIDERURGICA NACIONAL
24399;CSRN5;CIA ENERGETICA DO RIO GRANDE DO NORTE - COSERN
24399;CSRN6;CIA ENERGETICA DO RIO GRANDE DO NORTE - COSERN
21032;CTKA4;KARSTEN S.A.
23081;CTNM4;COMPANHIA DE TECIDOS NORTE DE MINAS - COTEMINAS
25089;CTSA4;SANTANENSE S.A.
22343;CURY3;CURY CONSTRUTORA E INCORPORADORA S.A.
22555;CVCB3;CVC BRASIL OPERADORA E AGENCIA DE VIAGENS S.A.
22598;CYRE3;CYRELA BRAZIL REALTY S.A. EMPREENDIMENTOS E PARTICIPAÇÕES
25537;DASA3;DIAGNOSTICOS DA AMERICA S.A.
21991;DIRR3;DIRECIONAL ENGENHARIA S.A.
25232;DMMO3;DOMMO ENERGIA S.A.
25356;DOTZ3;DOTZ S.A.
25305;DEXP3;DEXCO S.A.
25305;DEXP4;DEXCO S.A.
22831;ECOR3;ECORODOVIAS INFRAESTRUTURA E LOGISTICA S.A.
19720;EGIE3;ENGIE BRASIL ENERGIA S.A.
21690;ELET3;CENTRAIS ELETRICAS BRASILEIRAS S.A. - ELETROBRAS
21690;ELET6;CENTRAIS ELETRICAS BRASILEIRAS S.A. - ELETROBRAS
25510;ELMD3;ELETROMIDIA S.A.
23197;EMAE4;EMAE - EMPRESA METROPOLITANA DE AGUAS E ENERGIA S.A.
20589;EMBR3;EMBRAER S.A.
22491;ENAT3;ENAUTA PARTICIPAÇÕES S.A.
22653;ENBR3;ENERGIAS DO BRASIL S.A.
24413;ENEV3;ENEVA S.A.
22670;ENGI11;ENERGISA S.A.
22670;ENGI4;ENERGISA S.A.
25054;ENJU3;ENJOEI S.A.
19965;EQPA3;EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A.
19965;EQPA5;EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A.
19965;EQPA7;EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A.
20331;EQTL3;EQUATORIAL ENERGIA S.A.
22036;ESPA3;ESPAÇOLASER SERVIÇOS ESTÉTICOS S.A.
14217;ESTR4;ESTRELA MANUFATURA DE BRINQUEDOS S.A.
19607;ETER3;ETERNIT S.A.
22087;EUCA4;EUCATEX S.A. INDUSTRIA E COMERCIO
23213;EVEN3;EVEN CONSTRUTORA E INCORPORADORA S.A.
22539;EZTC3;EZ TEC EMPREENDIMENTOS E PARTICIPACOES S.A.
20480;FESA4;FERTILIZANTES HERINGER S.A.
20480;FHER3;FERTILIZANTES HERINGER S.A.
23462;FLRY3;FLEURY S.A.
25768;FRAS3;FRAS-LE S.A.
25768;FRAS4;FRAS-LE S.A.
25709;GFSA3;GAFISA S.A.
20628;GGBR4;GERDAU S.A.
19922;GGBR3;GERDAU S.A.
19922;GOAU4;METALURGICA GERDAU S.A.
22211;GMAT3;GRUPO MATEUS S.A.
23205;GOLL4;GOL LINHAS AEREAS INTELIGENTES S.A.
25020;GRND3;GRENDENE S.A.
20833;GUAR3;GUARARAPES CONFECCOES S.A.
23981;HAPV3;HAPVIDA PARTICIPAÇÕES E INVESTIMENTOS S.A.
22483;HBSA3;HIDROVIAS DO BRASIL S.A.
22181;HBRE3;HBR REALTY EMPREENDIMENTOS IMOBILIARIOS S.A.
22181;HETA4;HERCULES S.A. - FABRICA DE TALHERES
22181;HGTX3;CIA. HERING
22181;HBOR3;HEL नाइथBOR EMPREENDIMENTOS S.A.
22181;HYPE3;HYPERA S.A.
21008;IFCM3;INFRICOMMERCE CXAAS S.A.
24550;IGTI11;IGUA SANEAMENTO S.A.
24550;IGTA3;IGUATEMI EMPRESA DE SHOPPING CENTERS S.A.
22980;INEP3;INEPAR S/A INDUSTRIA E CONSTRUCOES
22980;INEP4;INEPAR S/A INDUSTRIA E CONSTRUCOES
25464;INTB3;INTELBRAS S.A.
20340;IRBR3;IRB-BRASIL RESSEGUROS S.A.
23411;ITSA4;ITAUSA S.A.
23411;ITSA3;ITAUSA S.A.
20249;ITUB4;ITAU UNIBANCO HOLDING S.A.
20249;ITUB3;ITAU UNIBANCO HOLDING S.A.
22327;JALL3;JALLES MACHADO S.A.
20307;JBSS3;JBS S.A.
22645;JFEN3;JOAO FORTES ENGENHARIA S.A.
2441;JHSF3;JHSF PARTICIPACOES S.A.
25750;JOPA4;JOSAPAR JOAQUIM OLIVEIRA S.A. PARTICIPACOES
25750;JSLG3;JSL S.A.
25750;KEPL3;KEPLER WEBER S.A.
21300;KLBN11;KLABIN S.A.
21300;KLBN4;KLABIN S.A.
21300;KLBN3;KLABIN S.A.
25677;LAVV3;LAVVI EMPREENDIMENTOS IMOBILIARIOS S.A.
23103;LIGT3;LIGHT S.A.
22432;LREN3;LOJAS RENNER S.A.
25596;LWSA3;LOCAWEB SERVICOS DE INTERNET S.A.
22149;LOGG3;LOG COMMERCIAL PROPERTIES E PARTICIPACOES S.A.
25291;LOGN3;LOG-IN LOGISTICA INTERMODAL S.A.
25291;LPSB3;LPS BRASIL - CONSULTORIA DE IMOIS S.A.
25291;LUPA3;LUPATECH S.A.
23272;LUXM4;TREVISA INVESTIMENTOS S.A.
25413;LVBI11;LIVETECH DA BAHIA INDUSTRIA E COMERCIO S.A.
23280;MBLY3;MOBLY S.A.
23280;MDIA3;M. DIAS BRANCO S.A. INDUSTRIA E COMERCIO DE ALIMENTOS
23280;MDNE3;MOURA DUBEUX ENGENHARIA S.A.
23280;MEAL3;IMC S.A.
23280;MEGA3;OMEGA ENERGIA S.A.
23280;MELK3;MELNICK DESENVOLVIMENTO IMOBILIARIO S.A.
23280;MGLU3;MAGAZINE LUIZA S.A.
23280;MILS3;MILLS ESTRUTURAS E SERVICOS DE ENGENHARIA S.A.
23280;MMXM3;MMX MINERACAO E METALICOS S.A.
23280;MOAR3;MONT ARANHA S.A.
23280;MODL11;BANCO MODAL S.A.
23280;MOVI3;MOVIDA PARTICIPACOES S.A.
23280;MRFG3;MARFRIG GLOBAL FOODS S.A.
23280;MRVE3;MRV ENGENHARIA E PARTICIPACOES S.A.
23280;MTRE3;MITRE REALTY EMPREENDIMENTOS E PARTICIPACOES S.A.
23280;MULT3;MULTIPLAN - EMPREENDIMENTOS IMOBILIARIOS S.A.
23280;MYPK3;IOCHP-MAXION S.A.
23280;NEOE3;NEOENERGIA S.A.
23280;NGRD3;NEOGRID PARTICIPACOES S.A.
23280;NINJ3;GETNINJAS S.A.
23280;NTCO3;NATURA &CO HOLDING S.A.
23280;ODPV3;ODONTOPREV S.A.
23280;OFSA3;OI S.A.
23280;OIBR3;OI S.A.
23280;OIBR4;OI S.A.
23280;OMGE3;OMEGA GERACAO S.A.
23280;OPCT3;OCEANPACT SERVICOS MARITIMOS S.A.
23280;OSXB3;OSX BRASIL S.A.
23280;PARD3;INSTITUTO HERMES PARDINI S.A.
23280;PATI4;PANATLANTICA S.A.
23280;PCAR3;COMPANHIA BRASILEIRA DE DISTRIBUICAO
23280;PDGR3;PDG REALTY S.A. EMPREENDIMENTOS E PARTICIPACOES
23280;PETR3;PETROLEO BRASILEIRO S.A. - PETROBRAS
23280;PETR4;PETROLEO BRASILEIRO S.A. - PETROBRAS
23280;PETZ3;PET CENTER COMERCIO E PARTICIPACOES S.A.
23280;PFRM3;PROFARMA DISTRIBUIDORA DE PRODUTOS FARMACEUTICOS S.A.
23280;PGMN3;PAGUE MENOS COMERCIO DE PRODUTOS ALIMENTICIOS S.A.
23280;PINN3;PETRORIO S.A.
23280;PLPL3;PLANO & PLANO DESENVOLVIMENTO IMOBILIARIO S.A.
23280;PMAM3;PARANAPANEMA S.A.
23280;POMO4;MARCOPOLO S.A.
23280;POMO3;MARCOPOLO S.A.
23280;PORT3;WILSON SONS S.A.
23280;POSI3;POSITIVO TECNOLOGIA S.A.
23280;PRIO3;PETRORIO S.A.
23280;PRNR3;PRINER SERVICOS INDUSTRIAIS S.A.
23280;PSSA3;PORTO SEGURO S.A.
23280;PTBL3;PORTOBELLO S.A.
23280;QUAL3;QUALICORP CONSULTORIA E CORRETORA DE SEGUROS S.A.
23280;RADL3;RAIA DROGASIL S.A.
23280;RAIL3;RUMO S.A.
23280;RANI3;IRANI PAPEL E EMBALAGEM S.A.
23280;RAPT4;RANDON S.A. IMPLEMENTOS E PARTICIPACOES
23280;RDOR3;REDE D'OR SAO LUIZ S.A.
23280;RECV3;PETRORECONCAVO S.A.
23280;RENT3;LOCALIZA RENT A CAR S.A.
23280;RCSL4;RECRUSUL S.A.
23280;ROMI3;INDUSTRIAS ROMI S.A.
23280;RRRP3;3R PETROLEUM OLEO E GAS S.A.
23280;RSID3;ROSSI RESIDENCIAL S.A.
23280;SANB11;BANCO SANTANDER (BRASIL) S.A.
23280;SANB3;BANCO SANTANDER (BRASIL) S.A.
23280;SANB4;BANCO SANTANDER (BRASIL) S.A.
23280;SAPR11;COMPANHIA DE SANEAMENTO DO PARANA - SANEPAR
23280;SAPR4;COMPANHIA DE SANEAMENTO DO PARANA - SANEPAR
23280;SBFG3;GRUPO SBF S.A.
23280;SBSP3;COMPANHIA DE SANEAMENTO BASICO DO ESTADO DE SAO PAULO - SABESP
23280;SEER3;SER EDUCACIONAL S.A.
23280;SEQL3;SEQUOIA LOGISTICA E TRANSPORTES S.A.
23280;SIMH3;SIMPAR S.A.
23280;SLCE3;SLC AGRICOLA S.A.
23280;SLED4;SARAIVA S.A. L IVREIROS EDITORES
23280;SMFT3;SMARTFIT ESCOLA DE GINASTICA E DANCA S.A.
23280;SMTO3;SAO MARTINHO S.A.
23280;SOMA3;GRUPO DE MODA SOMA S.A.
23280;SQIA3;SINQIA S.A.
23280;STBP3;SANTOS BRASIL PARTICIPACOES S.A.
23280;SULA11;SUL AMERICA S.A.
23280;SUZB3;SUZANO S.A.
23280;TAEE11;TRANSMISSORA ALIANCA DE ENERGIA ELETRICA S.A.
23280;TAEE4;TRANSMISSORA ALIANCA DE ENERGIA ELETRICA S.A.
23280;TASA4;TAURUS ARMAS S.A.
23280;TCSA3;TC S.A.
23280;TECN3;TECHNOS S.A.
23280;TEND3;CONSTRUTORA TENDA S.A.
23280;TGMA3;TEGMA GESTAO LOGISTICA S.A.
23280;TIMS3;TIM S.A.
23280;TOTS3;TOTVS S.A.
23280;TRIS3;TRISUL S.A.
23280;TRPL4;ISA CTEEP - COMPANHIA DE TRANSMISSAO DE ENERGIA ELETRICA PAULISTA
23280;TUPY3;TUPY S.A.
23280;UGPA3;ULTRAPAR PARTICIPACOES S.A.
23280;UNIP6;UNIPAR CARBOCLORO S.A.
23280;USIM5;USINAS SIDERURGICAS DE MINAS GERAIS S.A. - USIMINAS
23280;USIM3;USINAS SIDERURGICAS DE MINAS GERAIS S.A. - USIMINAS
23280;VALE3;VALE S.A.
23280;VAMO3;VAMOS LOCACAO DE CAMINHOES, MAQUINAS E EQUIPAMENTOS S.A.
23280;VBBR3;VIBRA ENERGIA S.A.
23280;VIIA3;VIA S.A.
23280;VITT3;VITTIA FERTILIZANTES E BIOLOGICOS S.A.
23280;VIVA3;VIVARA PARTICIPACOES S.A.
23280;VIVT3;TELEFONICA BRASIL S.A.
23280;VLID3;VALID SOLUCOES S.A.
23280;VULC3;VULCABRAS S.A.
23280;WEGE3;WEG S.A.
23280;WIZS3;WIZ SOLUCOES E CORRETAGEM DE SEGUROS S.A.
23280;YDUQ3;YDUQS PARTICIPACOES S.A.
25801;REDE3;REDE ENERGIA PARTICIPAÇÕES S.A.
25810;GGPS3;GPS PARTICIPAÇÕES E EMPREENDIMENTOS S.A.
25836;BLAU3;BLAU FARMACÊUTICA S.A.
25860;BRBI11;BRBI BR PARTNERS S.A
25879;KRSA3;KORA SAÚDE PARTICIPAÇÕES S.A.
25895;LVTC3;LIVETECH DA BAHIA INDÚSTRIA E COMÉRCIO S.A.
25917;RAIZ4;RAÍZEN S.A.
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
        df = pd.read_csv(io.StringIO(mapeamento_csv_data), sep=';', encoding='utf-8')
        df.columns = df.columns.str.strip()
        df.rename(columns={'Ticker': 'TICKER', 'CD_CVM': 'CD_CVM'}, inplace=True, errors='ignore')
        df = df.dropna(subset=['TICKER', 'CD_CVM'])
        df['CD_CVM'] = pd.to_numeric(df['CD_CVM'], errors='coerce').astype('Int64')
        df['TICKER'] = df['TICKER'].astype(str).str.strip().str.upper()
        df = df.dropna(subset=['CD_CVM']).drop_duplicates(subset=['TICKER'])
        return df
    except Exception:
        return pd.DataFrame()

def consulta_bc(codigo_bcb):
    try:
        url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados/ultimos/1?formato=json'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return float(data[0]['valor']) / 100.0 if data else None
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def obter_dados_mercado(periodo_ibov):
    with st.spinner("Buscando dados de mercado (Selic, Ibovespa)..."):
        selic_anual = consulta_bc(1178)
        risk_free_rate = selic_anual if selic_anual is not None else 0.105
        ibov = yf.download('^BVSP', period=periodo_ibov, progress=False)
        if not ibov.empty and 'Adj Close' in ibov.columns:
            retorno_anual_mercado = ((1 + ibov['Adj Close'].pct_change().mean()) ** 252) - 1
        else:
            retorno_anual_mercado = 0.12
        premio_risco_mercado = retorno_anual_mercado - risk_free_rate
    return risk_free_rate, retorno_anual_mercado, premio_risco_mercado, ibov

def obter_historico_metrica(df_empresa, codigo_conta):
    metric_df = df_empresa[(df_empresa['CD_CONTA'] == codigo_conta) & (df_empresa['ORDEM_EXERC'] == 'ÚLTIMO')]
    if metric_df.empty: return pd.Series(dtype=float)
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
        st.session_state.categories = {'Receita': ['Salário', 'Freelance'], 'Despesa': ['Moradia', 'Alimentação', 'Transporte'], 'Investimento': ['Ações BR', 'FIIs', 'Ações INT', 'Caixa']}
    if 'goals' not in st.session_state:
        st.session_state.goals = {
            'Reserva de Emergência': {'meta': 10000.0, 'atual': 0.0},
            'Liberdade Financeira': {'meta': 1000000.0, 'atual': 0.0}
        }

def ui_controle_financeiro():
    """Renderiza a interface completa da aba de Controle Financeiro."""
    st.header("Dashboard de Controle Financeiro Pessoal")
    
    col1, col2 = st.columns(2)
    with col1:
        with st.expander("➕ Novo Lançamento", expanded=True):
            with st.form("new_transaction_form", clear_on_submit=True):
                data = st.date_input("Data", datetime.now())
                tipo = st.selectbox("Tipo", ["Receita", "Despesa", "Investimento"])
                
                # --- LÓGICA DE CATEGORIAS APRIMORADA ---
                categoria_final = None
                sub_arca = None

                if tipo == "Investimento":
                    # Campo ARCA aparece somente para Investimentos
                    categoria_final = st.selectbox("Categoria (Metodologia ARCA)", options=st.session_state.categories['Investimento'], key="arca_cat")
                    sub_arca = categoria_final
                else:
                    # Lógica para criar categorias de Receita/Despesa
                    opcoes_categoria = st.session_state.categories[tipo] + ["--- Adicionar Nova Categoria ---"]
                    categoria_selecionada = st.selectbox(f"Categoria de {tipo}", options=opcoes_categoria, key=f"cat_{tipo}")
                    
                    if categoria_selecionada == "--- Adicionar Nova Categoria ---":
                        nova_categoria = st.text_input("Nome da Nova Categoria", key=f"new_cat_{tipo}")
                        if nova_categoria: # Usa a nova categoria se o usuário digitar algo
                            categoria_final = nova_categoria
                    else:
                        categoria_final = categoria_selecionada

                valor = st.number_input("Valor (R$)", min_value=0.0, format="%.2f")
                descricao = st.text_input("Descrição (opcional)")
                submitted = st.form_submit_button("Adicionar Lançamento")

                if submitted and categoria_final:
                    # Adiciona a nova categoria à lista da sessão, se for nova
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

    df_trans = st.session_state.transactions.copy()
    if not df_trans.empty:
        df_trans['Data'] = pd.to_datetime(df_trans['Data'])
    
    invest_produtivos = df_trans[(df_trans['Tipo'] == 'Investimento') & (df_trans['Subcategoria ARCA'].isin(['Ações BR', 'FIIs', 'Ações INT']))]['Valor'].sum()
    caixa = df_trans[(df_trans['Tipo'] == 'Investimento') & (df_trans['Subcategoria ARCA'] == 'Caixa')]['Valor'].sum()
    
    st.session_state.goals['Liberdade Financeira']['atual'] = invest_produtivos
    st.session_state.goals['Reserva de Emergência']['atual'] = caixa

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Distribuição ARCA")
        df_arca = df_trans[df_trans['Tipo'] == 'Investimento'].groupby('Subcategoria ARCA')['Valor'].sum()
        if not df_arca.empty:
            fig_arca = px.pie(df_arca, values='Valor', names=df_arca.index, title="Composição dos Investimentos", hole=.3, template="plotly_dark")
            fig_arca.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', legend_font_color='#ECEDEE', title_font_color='#ECEDEE')
            fig_arca.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_arca, use_container_width=True)
        else:
            st.info("Nenhum investimento ARCA registrado.")

    with col2:
        st.subheader("Reserva de Emergência")
        meta = st.session_state.goals['Reserva de Emergência']['meta']
        atual = st.session_state.goals['Reserva de Emergência']['atual']
        progresso = (atual / meta) if meta > 0 else 0
        st.metric("Valor em Caixa", f"R$ {atual:,.2f}")
        st.progress(min(progresso, 1.0), text=f"{progresso:.1%} da meta de R$ {meta:,.2f}")

    with col3:
        st.subheader("Liberdade Financeira")
        meta = st.session_state.goals['Liberdade Financeira']['meta']
        atual = st.session_state.goals['Liberdade Financeira']['atual']
        progresso = (atual / meta) if meta > 0 else 0
        st.metric("Investimentos Produtivos", f"R$ {atual:,.2f}")
        st.progress(min(progresso, 1.0), text=f"{progresso:.1%} da meta de R$ {meta:,.2f}")
        
    st.divider()

    st.subheader("Análise Histórica")
    if not df_trans.empty:
        df_monthly = df_trans.set_index('Data').groupby([pd.Grouper(freq='M'), 'Tipo'])['Valor'].sum().unstack(fill_value=0)
        col1, col2 = st.columns(2)
        with col1:
            fig_evol_tipo = px.bar(df_monthly, x=df_monthly.index, y=[col for col in ['Receita', 'Despesa', 'Investimento'] if col in df_monthly.columns], title="Evolução Mensal por Tipo", barmode='group', template="plotly_dark")
            fig_evol_tipo.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', legend_font_color='#ECEDEE', title_font_color='#ECEDEE')
            st.plotly_chart(fig_evol_tipo, use_container_width=True)
        with col2:
            df_monthly['Patrimonio'] = (df_monthly.get('Receita', 0) - df_monthly.get('Despesa', 0)).cumsum()
            fig_evol_patrimonio = px.line(df_monthly, x=df_monthly.index, y='Patrimonio', title="Evolução Patrimonial", markers=True, template="plotly_dark")
            fig_evol_patrimonio.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', legend_font_color='#ECEDEE', title_font_color='#ECEDEE')
            st.plotly_chart(fig_evol_patrimonio, use_container_width=True)
    else:
        st.info("Adicione transações para visualizar os gráficos de evolução.")

    with st.expander("📜 Histórico de Transações", expanded=True):
        if not df_trans.empty:
            # Adiciona a coluna 'Excluir' para o data_editor
            df_para_editar = df_trans.copy()
            df_para_editar['Excluir'] = False
            
            colunas_config = {
                "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "Valor": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f"),
                "Subcategoria ARCA": st.column_config.TextColumn("ARCA")
            }

            # Usa o st.data_editor para permitir edições
            edited_df = st.data_editor(
                df_para_editar[['Excluir', 'Data', 'Tipo', 'Categoria', 'Subcategoria ARCA', 'Valor', 'Descrição']], 
                use_container_width=True,
                column_config=colunas_config,
                hide_index=True,
                key="editor_transacoes"
            )
            
            if st.button("Excluir Lançamentos Selecionados"):
                # Lógica de exclusão
                indices_para_excluir = edited_df[edited_df['Excluir']].index
                st.session_state.transactions = st.session_state.transactions.drop(indices_para_excluir).reset_index(drop=True)
                st.success("Lançamentos excluídos!")
                st.rerun()

            # Lógica de edição: st.data_editor já retorna o df editado.
            # Precisamos comparar com o original para salvar as mudanças.
            # O Streamlit trata isso reexecutando o script, então precisamos salvar o estado.
            # A forma mais simples é atribuir o df editado (sem a coluna Excluir) de volta ao session_state.
            # Removendo a coluna 'Excluir' antes de salvar
            edited_df_sem_excluir = edited_df.drop(columns=['Excluir'])
            st.session_state.transactions = edited_df_sem_excluir

        else:
            st.info("Nenhuma transação registrada.")


# ==============================================================================
# ABA 2: VALUATION
# ==============================================================================

def calcular_beta(ticker, ibov_data, periodo_beta):
    dados_acao = yf.download(ticker, period=periodo_beta, progress=False)
    if dados_acao.empty or 'Adj Close' not in dados_acao.columns: return 1.0
    dados_combinados = pd.concat([dados_acao['Adj Close'], ibov_data['Adj Close']], axis=1).dropna()
    dados_combinados.columns = [ticker, '^BVSP']
    retornos_mensais = dados_combinados.resample('M').ffill().pct_change().dropna()
    if len(retornos_mensais) < 2: return 1.0
    covariancia = retornos_mensais.cov().iloc[0, 1]
    variancia_mercado = retornos_mensais['^BVSP'].var()
    return covariancia / variancia_mercado if variancia_mercado != 0 else 1.0

def processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params):
    (risk_free_rate, _, premio_risco_mercado, ibov_data) = market_data
    dre, bpa, bpp, dfc = demonstrativos['dre'], demonstrativos['bpa'], demonstrativos['bpp'], demonstrativos['dfc_mi']
    empresa_dre = dre[dre['CD_CVM'] == codigo_cvm]
    empresa_bpa = bpa[bpa['CD_CVM'] == codigo_cvm]
    empresa_bpp = bpp[bpp['CD_CVM'] == codigo_cvm]
    empresa_dfc = dfc[dfc['CD_CVM'] == codigo_cvm]
    if any(df.empty for df in [empresa_dre, empresa_bpa, empresa_bpp, empresa_dfc]): return None, "Dados CVM históricos incompletos."
    try:
        info = yf.Ticker(ticker_sa).info
        market_cap = info.get('marketCap'); preco_atual = info.get('currentPrice', info.get('previousClose'))
        nome_empresa = info.get('longName', ticker_sa); n_acoes = info.get('sharesOutstanding')
        if not all([market_cap, preco_atual, n_acoes, nome_empresa]): return None, "Dados de mercado (YFinance) incompletos."
    except Exception: return None, "Falha ao buscar dados no Yahoo Finance."
    C = CONFIG['CONTAS_CVM']
    hist_ebit = obter_historico_metrica(empresa_dre, C['EBIT']); hist_impostos = obter_historico_metrica(empresa_dre, C['IMPOSTO_DE_RENDA_CSLL']); hist_lai = obter_historico_metrica(empresa_dre, C['LUCRO_ANTES_IMPOSTOS'])
    if hist_lai.sum() == 0 or hist_ebit.empty: return None, "Dados de Lucro/EBIT insuficientes."
    aliquota_efetiva = abs(hist_impostos.sum()) / abs(hist_lai.sum()); hist_nopat = hist_ebit * (1 - aliquota_efetiva)
    hist_dep_amort = obter_historico_metrica(empresa_dfc, C['DEPRECIACAO_AMORTIZACAO']); hist_fco = hist_nopat.add(hist_dep_amort, fill_value=0)
    try:
        contas_a_receber = obter_historico_metrica(empresa_bpa, C['CONTAS_A_RECEBER']).iloc[-1]; estoques = obter_historico_metrica(empresa_bpa, C['ESTOQUES']).iloc[-1]
        fornecedores = obter_historico_metrica(empresa_bpp, C['FORNECEDORES']).iloc[-1]; ativo_imobilizado = obter_historico_metrica(empresa_bpa, C['ATIVO_IMOBILIZADO']).iloc[-1]
        ativo_intangivel = obter_historico_metrica(empresa_bpa, C['ATIVO_INTANGIVEL']).iloc[-1]; divida_cp = obter_historico_metrica(empresa_bpp, C['DIVIDA_CURTO_PRAZO']).iloc[-1]
        divida_lp = obter_historico_metrica(empresa_bpp, C['DIVIDA_LONGO_PRAZO']).iloc[-1]; desp_financeira = abs(obter_historico_metrica(empresa_dre, C['DESPESAS_FINANCEIRAS']).iloc[-1])
    except IndexError: return None, "Dados de balanço patrimonial ausentes ou incompletos."
    ncg = contas_a_receber + estoques - fornecedores; capital_empregado = ncg + ativo_imobilizado + ativo_intangivel
    if capital_empregado <= 0: return None, "Capital empregado negativo ou nulo."
    nopat_medio = hist_nopat.tail(params['media_anos_calculo']).mean(); fco_medio = hist_fco.tail(params['media_anos_calculo']).mean()
    if pd.isna(nopat_medio) or pd.isna(fco_medio): return None, "Não foi possível calcular NOPAT ou FCO médio."
    roic = nopat_medio / capital_empregado; divida_total = divida_cp + divida_lp
    kd = (desp_financeira / divida_total) if divida_total > 0 else 0.0; kd_liquido = kd * (1 - aliquota_efetiva)
    beta = calcular_beta(ticker_sa, ibov_data, params['periodo_beta_ibov']); ke = risk_free_rate + beta * premio_risco_mercado
    ev_mercado = market_cap + divida_total
    wacc = ((market_cap / ev_mercado) * ke) + ((divida_total / ev_mercado) * kd_liquido) if ev_mercado > 0 else ke
    eva = (roic - wacc) * capital_empregado; riqueza_atual = (eva / wacc) if wacc > 0 else 0.0
    riqueza_futura_esperada = ev_mercado - capital_empregado; efv = riqueza_futura_esperada - riqueza_atual
    g = params['taxa_crescimento_perpetuidade']
    if wacc <= g or pd.isna(wacc): return None, "WACC inválido ou menor/igual à taxa de crescimento."
    valor_residual = (fco_medio * (1 + g)) / (wacc - g); equity_value = valor_residual - divida_total
    preco_justo = equity_value / n_acoes if n_acoes > 0 else 0; margem_seguranca = (preco_justo / preco_atual) - 1 if preco_atual > 0 else 0
    return {'Empresa': nome_empresa, 'Ticker': ticker_sa.replace('.SA', ''), 'Preço Atual (R$)': preco_atual, 'Preço Justo (R$)': preco_justo, 'Margem Segurança (%)': margem_seguranca * 100, 'Market Cap (R$)': market_cap, 'Capital Empregado (R$)': capital_empregado, 'Dívida Total (R$)': divida_total, 'NOPAT Médio (R$)': nopat_medio, 'ROIC (%)': roic * 100, 'Beta': beta, 'Custo do Capital (WACC %)': wacc * 100, 'Spread (ROIC-WACC %)': (roic - wacc) * 100, 'EVA (R$)': eva, 'EFV (R$)': efv, 'hist_nopat': hist_nopat, 'hist_fco': hist_fco, 'hist_roic': (hist_nopat / capital_empregado) * 100, 'wacc_series': pd.Series([wacc * 100] * len(hist_nopat.index), index=hist_nopat.index)}, "Análise concluída com sucesso."

def executar_analise_completa(ticker_map, demonstrativos, market_data, params, progress_bar):
    todos_os_resultados = []
    total_empresas = len(ticker_map)
    for i, (index, row) in enumerate(ticker_map.iterrows()):
        ticker = row['TICKER']; codigo_cvm = int(row['CD_CVM']); ticker_sa = f"{ticker}.SA"
        progress = (i + 1) / total_empresas
        progress_bar.progress(progress, text=f"Analisando {i+1}/{total_empresas}: {ticker}")
        try:
            resultados, _ = processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params)
            if resultados: todos_os_resultados.append(resultados)
        except Exception: continue
    progress_bar.empty()
    return todos_os_resultados

@st.cache_data
def convert_df_to_csv(df):
   return df.to_csv(index=False, decimal=',', sep=';', encoding='utf-8-sig').encode('utf-8-sig')

def exibir_rankings(df_final):
    st.subheader("🏆 Rankings de Mercado")
    if df_final.empty:
        st.warning("Nenhuma empresa pôde ser analisada com sucesso para gerar os rankings.")
        return
    rankings = {"MARGEM_SEGURANCA": ("Ranking por Margem de Segurança", 'Margem Segurança (%)', ['Ticker', 'Empresa', 'Preço Atual (R$)', 'Preço Justo (R$)', 'Margem Segurança (%)']), "ROIC": ("Ranking por ROIC", 'ROIC (%)', ['Ticker', 'Empresa', 'ROIC (%)', 'Spread (ROIC-WACC %)']), "EVA": ("Ranking por EVA", 'EVA (R$)', ['Ticker', 'Empresa', 'EVA (R$)']), "EFV": ("Ranking por EFV", 'EFV (R$)', ['Ticker', 'Empresa', 'EFV (R$)'])}
    tab_names = [config[0] for config in rankings.values()]; tabs = st.tabs(tab_names)
    for i, (nome_ranking, (titulo, coluna_sort, colunas_view)) in enumerate(rankings.items()):
        with tabs[i]:
            df_sorted = df_final.sort_values(by=coluna_sort, ascending=False).reset_index(drop=True)
            df_display = df_sorted[colunas_view].head(20).copy()
            for col in df_display.columns:
                if 'R$' in col: df_display[col] = df_display[col].apply(lambda x: f'R$ {x:,.2f}' if pd.notna(x) else 'N/A')
                if '%' in col: df_display[col] = df_display[col].apply(lambda x: f'{x:.2f}%' if pd.notna(x) else 'N/A')
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            csv = convert_df_to_csv(df_sorted[colunas_view])
            st.download_button(label=f"📥 Baixar Ranking Completo (.csv)", data=csv, file_name=f'ranking_{nome_ranking.lower()}.csv', mime='text/csv',)

def ui_valuation():
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
            p_taxa_cresc = st.slider("Taxa de Crescimento na Perpetuidade (%)", 0.0, 10.0, CONFIG["TAXA_CRESCIMENTO_PERPETUIDADE"] * 100, 0.5) / 100
            p_media_anos = st.number_input("Anos para Média de NOPAT/FCO", 1, CONFIG["HISTORICO_ANOS_CVM"], CONFIG["MEDIA_ANOS_CALCULO"])
            p_periodo_beta = st.selectbox("Período para Cálculo do Beta", options=["1y", "2y", "5y", "10y"], index=2, key="beta_individual")
        if analisar_btn:
            demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"]); market_data = obter_dados_mercado(p_periodo_beta)
            ticker_sa = f"{ticker_selecionado}.SA"; codigo_cvm_info = ticker_cvm_map_df[ticker_cvm_map_df['TICKER'] == ticker_selecionado]
            codigo_cvm = int(codigo_cvm_info.iloc[0]['CD_CVM'])
            params_analise = {'taxa_crescimento_perpetuidade': p_taxa_cresc, 'media_anos_calculo': p_media_anos, 'periodo_beta_ibov': p_periodo_beta}
            with st.spinner(f"Analisando {ticker_selecionado}..."):
                resultados, status_msg = processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params_analise)
            if resultados:
                st.success(f"Análise para **{resultados['Empresa']} ({resultados['Ticker']})** concluída!")
                col1, col2, col3 = st.columns(3)
                col1.metric("Preço Atual", f"R$ {resultados['Preço Atual (R$)']:.2f}"); col2.metric("Preço Justo (DCF)", f"R$ {resultados['Preço Justo (R$)']:.2f}")
                ms_delta = resultados['Margem Segurança (%)']; col3.metric("Margem de Segurança", f"{ms_delta:.2f}%", delta=f"{ms_delta:.2f}%" if not pd.isna(ms_delta) else None)
                st.divider()
                tab_g, tab_d = st.tabs(["📊 Gráficos", "🔢 Tabela de Dados"])
                with tab_g:
                    st.plotly_chart(px.bar(x=resultados['hist_nopat'].index, y=resultados['hist_nopat'].values, title='Histórico de NOPAT', template="plotly_dark").update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'), use_container_width=True)
                    st.plotly_chart(px.bar(x=resultados['hist_fco'].index, y=resultados['hist_fco'].values, title='Histórico de FCO', template="plotly_dark").update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'), use_container_width=True)
                with tab_d:
                    df_display = pd.DataFrame.from_dict(resultados, orient='index', columns=['Valor'])
                    st.dataframe(df_display.drop(['hist_nopat', 'hist_fco', 'hist_roic', 'wacc_series']), use_container_width=True)
            else: st.error(f"Não foi possível analisar {ticker_selecionado}. Motivo: {status_msg}")

    with tab_ranking:
        st.info("Esta análise processa todas as empresas da lista, o que pode levar vários minutos.")
        if st.button("🚀 Iniciar Análise Completa e Gerar Rankings", type="primary", use_container_width=True):
            params_ranking = {'taxa_crescimento_perpetuidade': CONFIG["TAXA_CRESCIMENTO_PERPETUIDADE"], 'media_anos_calculo': CONFIG["MEDIA_ANOS_CALCULO"], 'periodo_beta_ibov': CONFIG["PERIODO_BETA_IBOV"]}
            demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"]); market_data = obter_dados_mercado(params_ranking['periodo_beta_ibov'])
            progress_bar = st.progress(0, text="Iniciando análise em lote...")
            resultados_completos = executar_analise_completa(ticker_cvm_map_df, demonstrativos, market_data, params_ranking, progress_bar)
            if resultados_completos:
                df_final = pd.DataFrame(resultados_completos)
                st.success(f"Análise completa! {len(df_final)} de {len(ticker_cvm_map_df)} empresas foram processadas com sucesso.")
                exibir_rankings(df_final)
            else: st.error("A análise em lote não retornou nenhum resultado válido.")

# ==============================================================================
# ABA 3: MODELO FLEURIET
# ==============================================================================

def reclassificar_contas_fleuriet(df_bpa, df_bpp, contas_cvm):
    aco = obter_historico_metrica(df_bpa, contas_cvm['ESTOQUES']).add(obter_historico_metrica(df_bpa, contas_cvm['CONTAS_A_RECEBER']), fill_value=0)
    pco = obter_historico_metrica(df_bpp, contas_cvm['FORNECEDORES'])
    ap = obter_historico_metrica(df_bpa, contas_cvm['ATIVO_NAO_CIRCULANTE'])
    pl = obter_historico_metrica(df_bpp, contas_cvm['PATRIMONIO_LIQUIDO'])
    pnc = obter_historico_metrica(df_bpp, contas_cvm['PASSIVO_NAO_CIRCULANTE'])
    return aco, pco, ap, pl, pnc

def processar_analise_fleuriet(ticker_sa, codigo_cvm, demonstrativos):
    C = CONFIG['CONTAS_CVM']
    bpa = demonstrativos['bpa'][demonstrativos['bpa']['CD_CVM'] == codigo_cvm]
    bpp = demonstrativos['bpp'][demonstrativos['bpp']['CD_CVM'] == codigo_cvm]
    dre = demonstrativos['dre'][demonstrativos['dre']['CD_CVM'] == codigo_cvm]
    if any(df.empty for df in [bpa, bpp, dre]): return None
    
    aco, pco, ap, pl, pnc = reclassificar_contas_fleuriet(bpa, bpp, C)
    if any(s.empty for s in [aco, pco, ap, pl, pnc]): return None

    ncg = aco.subtract(pco, fill_value=0); cdg = pl.add(pnc, fill_value=0).subtract(ap, fill_value=0); t = cdg.subtract(ncg, fill_value=0)
    
    efeito_tesoura = False
    if len(ncg) > 1 and len(cdg) > 1:
        cresc_ncg = ncg.pct_change().iloc[-1]; cresc_cdg = cdg.pct_change().iloc[-1]
        if pd.notna(cresc_ncg) and pd.notna(cresc_cdg) and cresc_ncg > cresc_cdg and t.iloc[-1] < 0:
            efeito_tesoura = True
            
    try:
        info = yf.Ticker(ticker_sa).info; market_cap = info.get('marketCap', 0)
        ativo_total = obter_historico_metrica(bpa, C['ATIVO_TOTAL']).iloc[-1]; passivo_total = obter_historico_metrica(bpp, C['PASSIVO_TOTAL']).iloc[-1]
        lucro_retido = pl.iloc[-1] - pl.iloc[0]; ebit = obter_historico_metrica(dre, C['EBIT']).iloc[-1]; vendas = obter_historico_metrica(dre, C['RECEITA_LIQUIDA']).iloc[-1]
        X1 = cdg.iloc[-1] / ativo_total; X2 = lucro_retido / ativo_total; X3 = ebit / ativo_total
        X4 = market_cap / passivo_total if passivo_total > 0 else 0; X5 = vendas / ativo_total
        z_score = 0.038*X1 + 1.253*X2 + 2.331*X3 + 0.511*X4 + 0.824*X5
        if z_score < 1.81: classificacao = "Risco Elevado"
        elif z_score < 2.99: classificacao = "Zona Cinzenta"
        else: classificacao = "Saudável"
    except Exception: z_score, classificacao = None, "Erro no cálculo"

    return {'Ticker': ticker_sa.replace('.SA', ''), 'Empresa': info.get('longName', ticker_sa), 'Ano': t.index[-1], 'NCG': ncg.iloc[-1], 'CDG': cdg.iloc[-1], 'Tesouraria': t.iloc[-1], 'Efeito Tesoura': efeito_tesoura, 'Z-Score': z_score, 'Classificação Risco': classificacao}

def ui_modelo_fleuriet():
    st.header("Análise de Saúde Financeira (Modelo Fleuriet & Z-Score)")
    st.info("Esta análise utiliza os dados da CVM para avaliar a estrutura de capital de giro e o risco de insolvência das empresas.")
    if st.button("🚀 Iniciar Análise Fleuriet Completa", type="primary", use_container_width=True):
        ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
        demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
        resultados_fleuriet = []
        progress_bar = st.progress(0, text="Iniciando análise Fleuriet...")
        total_empresas = len(ticker_cvm_map_df)
        for i, (index, row) in enumerate(ticker_cvm_map_df.iterrows()):
            ticker = row['TICKER']
            progress_bar.progress((i + 1) / total_empresas, text=f"Analisando {i+1}/{total_empresas}: {ticker}")
            resultado = processar_analise_fleuriet(f"{ticker}.SA", int(row['CD_CVM']), demonstrativos)
            if resultado: resultados_fleuriet.append(resultado)
        progress_bar.empty()
        
        if resultados_fleuriet:
            df_fleuriet = pd.DataFrame(resultados_fleuriet)
            st.success(f"Análise Fleuriet concluída para {len(df_fleuriet)} empresas.")
            ncg_medio = df_fleuriet['NCG'].mean(); tesoura_count = df_fleuriet['Efeito Tesoura'].sum()
            risco_count = len(df_fleuriet[df_fleuriet['Classificação Risco'] == "Risco Elevado"]); zscore_medio = df_fleuriet['Z-Score'].mean()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("NCG Média", f"R$ {ncg_medio/1e6:.1f} M"); col2.metric("Efeito Tesoura", f"{tesoura_count} empresas")
            col3.metric("Alto Risco (Z-Score)", f"{risco_count} empresas"); col4.metric("Z-Score Médio", f"{zscore_medio:.2f}")
            st.dataframe(df_fleuriet, use_container_width=True)
        else: st.error("Nenhum resultado pôde ser gerado para a análise Fleuriet.")
    with st.expander("📖 Metodologia do Modelo Fleuriet"):
        st.markdown("""- **NCG (Necessidade de Capital de Giro):** `(Estoques + Contas a Receber) - Fornecedores`
- **CDG (Capital de Giro):** `(Patrimônio Líquido + Passivo Longo Prazo) - Ativo Permanente`
- **T (Saldo de Tesouraria):** `CDG - NCG`
- **Efeito Tesoura:** Ocorre quando a NCG cresce mais rapidamente que o CDG.
- **Z-Score de Prado:** Modelo estatístico que mede a probabilidade de uma empresa ir à falência.""")

# ==============================================================================
# ESTRUTURA PRINCIPAL DO APP
# ==============================================================================
def main():
    st.title("Sistema de Controle Financeiro e Análise de Investimentos")
    inicializar_session_state()
    tab1, tab2, tab3 = st.tabs(["💲 Controle Financeiro", "📈 Análise de Valuation", "🔬 Modelo Fleuriet"])
    with tab1:
        ui_controle_financeiro()
    with tab2:
        ui_valuation()
    with tab3:
        ui_modelo_fleuriet()

if __name__ == "__main__":
    main()
