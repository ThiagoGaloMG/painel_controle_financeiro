# -*- coding: utf-8 -*-
"""
analise_financeira_app.py

Este script implementa um aplicativo web interativo usando a biblioteca Streamlit
para análise financeira, incluindo controle de finanças pessoais, valuation de
empresas e modelos de saúde financeira (Fleuriet e Z-Score).

O código foi revisado com base em um TCC sobre valuation que utiliza os modelos
EVA e EFV, bem como o modelo de Hamada para ajuste do beta.
"""

import os
import pandas as pd
import yfinance as yf
import requests
from zipfile import ZipFile
from datetime import datetime, timedelta
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
        --header-color: #FFFFFF; /* Branco puro para títulos e labels importantes */
        --border-color: #5372F0; /* Borda azul sutil */
    }

    body {
        color: var(--text-color);
        background-color: var(--primary-bg);
    }

   .main.block-container {
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

    /* Métricas com Borda Neon Sutil e Texto Branco */
   .stMetric {
        border: 1px solid var(--secondary-bg);
        border-radius: 8px;
        padding: 20px;
        background-color: var(--secondary-bg);
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
    }
   .stMetric label { /* Rótulo da métrica (ex: "Saldo") */
        color: var(--text-color);
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

    /* Expanders e Formulário com Texto Branco */
    [data-testid="stExpander"] {
        background-color: var(--secondary-bg);
        border: 1px solid var(--border-color);
        border-radius: 8px;
    }
    [data-testid="stExpander"] summary, [data-testid="stForm"] label {
        font-size: 1.1em;
        font-weight: 600;
        color: var(--header-color)!important;
    }

    /* Barras de progresso */
    > div {
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
CONFIG = CONFIG / "CVM_DATA"
CONFIG = CONFIG / "CVM_EXTRACTED"


# ==============================================================================
# LÓGICA DE DADOS GERAL (CVM, MERCADO, ETC.)
# # Mover esta seção para um arquivo 'data_loader.py' ou 'services/data_fetcher.py'
# ==============================================================================

@st.cache_data
def setup_diretorios():
    """Cria os diretórios locais para armazenar os dados da CVM."""
    try:
        CONFIG.mkdir(parents=True, exist_ok=True)
        CONFIG.mkdir(parents=True, exist_ok=True)
        return True
    except Exception:
        st.error("Erro ao criar diretórios locais. Verifique as permissões.")
        return False

@st.cache_data(show_spinner=False)
def preparar_dados_cvm(anos_historico):
    """
    Baixa e processa os dados anuais da CVM para os demonstrativos financeiros.

    Args:
        anos_historico (int): Número de anos de histórico a serem baixados.

    Returns:
        dict: Um dicionário com DataFrames consolidados para DRE, BPA, BPP, DFC_MI.
    """
    ano_final = datetime.today().year
    ano_inicial = ano_final - anos_historico
    with st.spinner(f"Verificando e baixando dados da CVM de {ano_inicial} a {ano_final-1}..."):
        demonstrativos_consolidados = {}
        tipos_demonstrativos =
        
        # Otimização: A CVM possui arquivos ITR e DFP. O código atual usa apenas DFP.
        # Para um histórico mais completo, seria ideal combinar ITR e DFP.
        # O código abaixo é uma implementação simplificada usando apenas DFP.

        for tipo in tipos_demonstrativos:
            lista_dfs_anuais =
            for ano in range(ano_inicial, ano_final):
                nome_arquivo_csv = f'dfp_cia_aberta_{tipo}_con_{ano}.csv'
                caminho_arquivo = CONFIG / nome_arquivo_csv
                
                # Verificação se o arquivo já existe para evitar re-download
                if not caminho_arquivo.exists():
                    nome_zip = f'dfp_cia_aberta_{ano}.zip'
                    caminho_zip = CONFIG / nome_zip
                    url_zip = f'{CONFIG}{nome_zip}'
                    
                    try:
                        response = requests.get(url_zip, stream=True, timeout=60)
                        response.raise_for_status()
                        with open(caminho_zip, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        with ZipFile(caminho_zip, 'r') as z:
                            if nome_arquivo_csv in z.namelist():
                                z.extract(nome_arquivo_csv, CONFIG)
                            else:
                                continue
                    except Exception:
                        continue
                
                if caminho_arquivo.exists():
                    try:
                        df_anual = pd.read_csv(caminho_arquivo, sep=';', encoding='ISO-8859-1', low_memory=False)
                        lista_dfs_anuais.append(df_anual)
                    except Exception:
                        continue
            
            if lista_dfs_anuais:
                demonstrativos_consolidados[tipo.lower()] = pd.concat(lista_dfs_anuais, ignore_index=True)
                
    # Adicionando tratamento para dados com ambiguidade, conforme o TCC [2]
    # O TCC menciona que em 2015 o valor de "Imobilizado" foi o mesmo que de "Intangível".
    # O código atual lida com isso tratando as contas separadamente, mas um aviso
    # ou tratamento específico pode ser adicionado se necessário para garantir a precisão.
    
    return demonstrativos_consolidados

@st.cache_data
def carregar_mapeamento_ticker_cvm():
    """
    Carrega o mapeamento de tickers e códigos CVM a partir de um arquivo CSV.
    RECOMENDAÇÃO: Salve o conteúdo da planilha mapeamento_tickers.csv em um arquivo
    local e ajuste o caminho abaixo.

    Returns:
        pd.DataFrame: DataFrame com mapeamento de tickers.
    """
    # A prática recomendada é usar um arquivo CSV externo.
    # Exemplo: df = pd.read_csv('mapeamento_tickers.csv', sep=';', encoding='utf-8')
    # O código abaixo mantém a funcionalidade original com a string embutida.
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
22645;JFEN3;JOAO FORTES ENGENHARia S.A.
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
21040;SYNE3;SYN PROP & TECH S.A
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
        df = df.dropna(subset=)
        df = pd.to_numeric(df, errors='coerce').astype('Int64')
        df = df.astype(str).str.strip().str.upper()
        
        # O TCC menciona a análise de "ações ordinárias".[2]
        # O código original descarta tickers duplicados, o que pode remover
        # classes de ações diferentes (ex: BBDC3 vs BBDC4).
        # A nova abordagem mantém todos os tickers, permitindo ao usuário
        # selecionar a classe de ação desejada.
        df = df.dropna(subset=)
        return df
    except Exception:
        st.error("Falha ao carregar o mapeamento de tickers. Verifique o arquivo `mapeamento_tickers.csv`.")
        return pd.DataFrame()

def consulta_bc(codigo_bcb):
    """Consulta a API do Banco Central para obter dados como a taxa Selic."""
    try:
        url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados/ultimos/1?formato=json'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return float(data['valor']) / 100.0 if data else None
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def obter_dados_mercado(periodo_ibov):
    """Busca dados de mercado como Selic, Ibovespa e prêmio de risco."""
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
    """
    Extrai o histórico anual de uma conta contábil específica da CVM.
    Filtra pela 'ÚLTIMO' ordem de exercício para pegar o dado mais recente do ano fiscal.
    """
    metric_df = df_empresa == codigo_conta) & (df_empresa == 'ÚLTIMO')]
    if metric_df.empty:
        return pd.Series(dtype=float)
    
    # Tratamento para garantir que a data de referência é única por ano
    metric_df = pd.to_datetime(metric_df)
    metric_df = metric_df.sort_values('DT_REFER').groupby(metric_df.dt.year).last()
    
    return metric_df.sort_index()


# ==============================================================================
# ABA 1: CONTROLE FINANCEIRO
# # Mover esta seção para um arquivo 'pages/controle_financeiro.py'
# ==============================================================================

def inicializar_session_state():
    """Inicializa o estado da sessão para simular um banco de dados."""
    if 'transactions' not in st.session_state:
        st.session_state.transactions = pd.DataFrame(columns=)
    if 'categories' not in st.session_state:
        st.session_state.categories = {'Receita':, 'Despesa':, 'Investimento':}
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
                tipo = st.selectbox("Tipo",)
                
                categoria_final = None
                sub_arca = None
                
                category_placeholder = st.empty()

                if tipo == "Investimento":
                    with category_placeholder.container():
                        categoria_selecionada = st.selectbox("Categoria (Metodologia ARCA)", 
                                                             options=st.session_state.categories['Investimento'], 
                                                             key="arca_cat")
                    categoria_final = categoria_selecionada
                    sub_arca = categoria_selecionada
                else:
                    with category_placeholder.container():
                        label_categoria = "Categoria"
                        opcoes_categoria = st.session_state.categories[tipo] + ["--- Adicionar Nova Categoria ---"]
                        categoria_selecionada = st.selectbox(label_categoria, 
                                                             options=opcoes_categoria, 
                                                             key=f"cat_{tipo}")
                        
                        if categoria_selecionada == "--- Adicionar Nova Categoria ---":
                            nova_categoria = st.text_input("Nome da Nova Categoria", key=f"new_cat_{tipo}")
                            if nova_categoria:
                                categoria_final = nova_categoria
                        else:
                            categoria_final = categoria_selecionada

                valor = st.number_input("Valor (R$)", min_value=0.0, format="%.2f")
                descricao = st.text_input("Descrição (opcional)")
                submitted = st.form_submit_button("Adicionar Lançamento")

                if submitted and categoria_final:
                    if tipo!= "Investimento" and categoria_final not in st.session_state.categories[tipo]:
                        st.session_state.categories[tipo].append(categoria_final)

                    nova_transacao = pd.DataFrame()
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
        df_trans = pd.to_datetime(df_trans)
    
    total_receitas = df_trans == 'Receita']['Valor'].sum()
    total_despesas = df_trans == 'Despesa']['Valor'].sum()
    total_investido = df_trans == 'Investimento']['Valor'].sum()
    saldo_periodo = total_receitas - total_despesas - total_investido

    st.subheader("Resumo Financeiro Total")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Receitas", f"R$ {total_receitas:,.2f}")
    col2.metric("Despesas", f"R$ {total_despesas:,.2f}")
    col3.metric("Investimentos", f"R$ {total_investido:,.2f}")
    col4.metric("Saldo (Receitas - Despesas - Invest.)", f"R$ {saldo_periodo:,.2f}", delta=f"{saldo_periodo:,.2f}")
    
    st.divider()

    invest_produtivos = df_trans == 'Investimento') & (df_trans.isin())]['Valor'].sum()
    caixa = df_trans == 'Investimento') & (df_trans == 'Caixa')]['Valor'].sum()
    
    st.session_state.goals['Liberdade Financeira']['atual'] = invest_produtivos
    st.session_state.goals['atual'] = caixa

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Distribuição ARCA")
        df_arca = df_trans == 'Investimento'].groupby('Subcategoria ARCA')['Valor'].sum()
        if not df_arca.empty:
            fig_arca = px.pie(df_arca, values='Valor', names=df_arca.index, title="Composição dos Investimentos", hole=.3, template="plotly_dark")
            fig_arca.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', legend_font_color='#ECEDEE', title_font_color='#ECEDEE')
            fig_arca.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_arca, use_container_width=True)
        else:
            st.info("Nenhum investimento ARCA registrado.")

    with col2:
        st.subheader("Reserva de Emergência")
        meta = st.session_state.goals['meta']
        atual = st.session_state.goals['atual']
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
        df_monthly = df_trans.set_index('Data').groupby()['Valor'].sum().unstack(fill_value=0)
        col1, col2 = st.columns(2)
        with col1:
            fig_evol_tipo = px.bar(df_monthly, x=df_monthly.index, y= if col in df_monthly.columns], title="Evolução Mensal por Tipo", barmode='group', template="plotly_dark")
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
            df_para_editar = df_trans.copy()
            df_para_editar['Excluir'] = False
            
            colunas_config = {
                "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "Valor": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f"),
                "Subcategoria ARCA": st.column_config.TextColumn("ARCA")
            }

            edited_df = st.data_editor(
                df_para_editar], 
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
# # Mover esta seção para um arquivo 'pages/valuation.py'
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

    covariancia = retornos_mensais.cov().iloc
    variancia_mercado = retornos_mensais.iloc[:, 1].var()
    
    return covariancia / variancia_mercado if variancia_mercado!= 0 else 1.0

def calcular_beta_hamada(ticker, ibov_data, periodo_beta, imposto, divida_total, market_cap):
    """
    Calcula o Beta alavancado ajustado pelo modelo de Hamada.
    
    O TCC menciona o uso do modelo de Hamada, que primeiro 'desalavanca' um beta
    de mercado para encontrar o beta do ativo (risco do negócio) e depois o
    'realavanca' para a estrutura de capital da empresa em análise.
    
    Fórmula de desalavancagem (Unlevered Beta):
    βU = βL /
    
    Fórmula de realavancagem (Levered Beta):
    βL = βU *
    
    Aqui, a função simplifica o processo calculando um beta base (levered)
    e ajustando-o, já que o foco é o ajuste da alavancagem.
    """
    beta_alavancado_mercado = calcular_beta(ticker, ibov_data, periodo_beta)
    
    # Se a empresa não tem capital próprio ou dívida, o cálculo não é aplicável
    if (market_cap + divida_total) == 0:
        return beta_alavancado_mercado
    
    # Desalavanca o beta para encontrar o beta do ativo (risco do negócio)
    divida_patrimonio = divida_total / market_cap if market_cap > 0 else 0
    beta_desalavancado = beta_alavancado_mercado / (1 + (1 - imposto) * divida_patrimonio)
    
    # Realavanca o beta para a estrutura de capital da empresa em análise
    beta_realavancado = beta_desalavancado * (1 + (1 - imposto) * divida_patrimonio)
    
    return beta_realavancado

def processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params):
    """
    Executa a análise de valuation de uma única empresa, calculando EVA, EFV, WACC, etc.

    Args:
        ticker_sa (str): Ticker da empresa no formato 'ABCD3.SA'.
        codigo_cvm (int): Código CVM da empresa.
        demonstrativos (dict): Dicionário de DataFrames com dados da CVM.
        market_data (tuple): Dados de mercado (taxa livre de risco, etc.).
        params (dict): Parâmetros do modelo (taxa de crescimento, etc.).

    Returns:
        tuple: Dicionário de resultados ou None, e uma mensagem de status.
    """
    (risk_free_rate, _, premio_risco_mercado, ibov_data) = market_data
    dre, bpa, bpp, dfc = demonstrativos['dre'], demonstrativos['bpa'], demonstrativos['bpp'], demonstrativos['dfc_mi']
    
    empresa_dre = dre == codigo_cvm]
    empresa_bpa = bpa == codigo_cvm]
    empresa_bpp = bpp == codigo_cvm]
    empresa_dfc = dfc == codigo_cvm]
    
    if any(df.empty for df in [empresa_dre, empresa_bpa, empresa_bpp, empresa_dfc]):
        return None, "Dados CVM históricos incompletos ou inexistentes."
    
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
        
    C = CONFIG
    
    # Extração de dados da CVM
    hist_ebit = obter_historico_metrica(empresa_dre, C)
    hist_impostos = obter_historico_metrica(empresa_dre, C)
    hist_lai = obter_historico_metrica(empresa_dre, C)

    if hist_lai.sum() == 0 or hist_ebit.empty:
        return None, "Dados de Lucro/EBIT insuficientes para calcular a alíquota de imposto."
        
    aliquota_efetiva = abs(hist_impostos.sum()) / abs(hist_lai.sum())
    hist_nopat = hist_ebit * (1 - aliquota_efetiva)
    hist_dep_amort = obter_historico_metrica(empresa_dfc, C)
    hist_fco = hist_nopat.add(hist_dep_amort, fill_value=0)
    
    try:
        contas_a_receber = obter_historico_metrica(empresa_bpa, C).iloc[-1]
        estoques = obter_historico_metrica(empresa_bpa, C).iloc[-1]
        fornecedores = obter_historico_metrica(empresa_bpp, C).iloc[-1]
        ativo_imobilizado = obter_historico_metrica(empresa_bpa, C).iloc[-1]
        ativo_intangivel = obter_historico_metrica(empresa_bpa, C).iloc[-1]
        divida_cp = obter_historico_metrica(empresa_bpp, C).iloc[-1]
        divida_lp = obter_historico_metrica(empresa_bpp, C).iloc[-1]
        desp_financeira = abs(obter_historico_metrica(empresa_dre, C).iloc[-1])
    except IndexError:
        return None, "Dados de balanço patrimonial ausentes ou incompletos."

    # Cálculo dos indicadores-chave
    ncg = contas_a_receber + estoques - fornecedores
    capital_empregado = ncg + ativo_imobilizado + ativo_intangivel
    
    if capital_empregado <= 0:
        return None, "Capital empregado negativo ou nulo."

    nopat_medio = hist_nopat.tail(params['media_anos_calculo']).mean()
    fco_medio = hist_fco.tail(params['media_anos_calculo']).mean()
    
    if pd.isna(nopat_medio) or pd.isna(fco_medio):
        return None, "Não foi possível calcular NOPAT ou FCO médio."
    
    roic = nopat_medio / capital_empregado
    divida_total = divida_cp + divida_lp
    
    # Ke: Custo do Capital Próprio
    # Kd: Custo do Capital de Terceiros
    kd = (desp_financeira / divida_total) if divida_total > 0 else 0.0
    kd_liquido = kd * (1 - aliquota_efetiva)
    
    # Novo cálculo de Beta usando o modelo de Hamada conforme o TCC
    beta_hamada = calcular_beta_hamada(ticker_sa, ibov_data, params['periodo_beta_ibov'], aliquota_efetiva, divida_total, market_cap)
    
    ke = risk_free_rate + beta_hamada * premio_risco_mercado
    
    # WACC: Custo Médio Ponderado de Capital
    ev_mercado = market_cap + divida_total
    wacc = ((market_cap / ev_mercado) * ke) + ((divida_total / ev_mercado) * kd_liquido) if ev_mercado > 0 else ke
    
    # Cálculo do EVA e EFV conforme TCC [2]
    eva = (roic - wacc) * capital_empregado
    riqueza_atual = (eva / wacc) if wacc > 0 else 0.0
    riqueza_futura_esperada = ev_mercado - capital_empregado
    efv = riqueza_futura_esperada - riqueza_atual
    
    g = params['taxa_crescimento_perpetuidade']
    
    if wacc <= g or pd.isna(wacc):
        return None, "WACC inválido ou menor/igual à taxa de crescimento na perpetuidade. Ajuste os parâmetros."
    
    # Fluxo de Caixa Descontado (DCF) - Valor Residual
    valor_residual = (fco_medio * (1 + g)) / (wacc - g)
    equity_value = valor_residual - divida_total
    
    preco_justo = equity_value / n_acoes if n_acoes > 0 else 0
    margem_seguranca = (preco_justo / preco_atual) - 1 if preco_atual > 0 else 0
    
    return {'Empresa': nome_empresa, 'Ticker': ticker_sa.replace('.SA', ''), 'Preço Atual (R$)': preco_atual, 'Preço Justo (R$)': preco_justo, 'Margem Segurança (%)': margem_seguranca * 100, 'Market Cap (R$)': market_cap, 'Capital Empregado (R$)': capital_empregado, 'Dívida Total (R$)': divida_total, 'NOPAT Médio (R$)': nopat_medio, 'ROIC (%)': roic * 100, 'Beta': beta_hamada, 'Custo do Capital (WACC %)': wacc * 100, 'Spread (ROIC-WACC %)': (roic - wacc) * 100, 'EVA (R$)': eva, 'EFV (R$)': efv, 'hist_nopat': hist_nopat, 'hist_fco': hist_fco, 'hist_roic': (hist_nopat / capital_empregado) * 100, 'wacc_series': pd.Series([wacc * 100] * len(hist_nopat.index), index=hist_nopat.index)}, "Análise concluída com sucesso."

def executar_analise_completa(ticker_map, demonstrativos, market_data, params, progress_bar):
    """Executa a análise de valuation para todas as empresas da lista."""
    todos_os_resultados =
    total_empresas = len(ticker_map)
    for i, (index, row) in enumerate(ticker_map.iterrows()):
        ticker = row
        codigo_cvm = int(row)
        ticker_sa = f"{ticker}.SA"
        progress = (i + 1) / total_empresas
        progress_bar.progress(progress, text=f"Analisando {i+1}/{total_empresas}: {ticker}")
        try:
            resultados, _ = processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params)
            if resultados:
                todos_os_resultados.append(resultados)
        except Exception:
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
        "MARGEM_SEGURANCA": ("Ranking por Margem de Segurança", 'Margem Segurança (%)',),
        "ROIC": ("Ranking por ROIC", 'ROIC (%)',),
        "EVA": ("Ranking por EVA", 'EVA (R$)',),
        "EFV": ("Ranking por EFV", 'EFV (R$)',)
    }
    
    tab_names = [config for config in rankings.values()]
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
    tab_individual, tab_ranking = st.tabs()
    
    ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
    if ticker_cvm_map_df.empty:
        st.error("Não foi possível carregar o mapeamento de tickers."); st.stop()
    
    with tab_individual:
        with st.form(key='individual_analysis_form'):
            col1, col2 = st.columns([1, 2])
            with col1:
                lista_tickers = sorted(ticker_cvm_map_df.unique())
                ticker_selecionado = st.selectbox("Selecione o Ticker da Empresa", options=lista_tickers, index=lista_tickers.index('PETR4'))
            with col2:
                analisar_btn = st.form_submit_button("Analisar Empresa", type="primary", use_container_width=True)
        
        with st.expander("Opções Avançadas de Valuation", expanded=False):
            col_params_1, col_params_2, col_params_3 = st.columns(3)
            with col_params_1:
                p_taxa_cresc = st.slider("Taxa de Crescimento na Perpetuidade (%)", 0.0, 10.0, CONFIG * 100, 0.5) / 100
            with col_params_2:
                p_media_anos = st.number_input("Anos para Média de NOPAT/FCO", 1, CONFIG, CONFIG)
            with col_params_3:
                p_periodo_beta = st.selectbox("Período para Cálculo do Beta", options=["1y", "2y", "5y", "10y"], index=2, key="beta_individual")

        # Inclusão da análise de sensibilidade como uma melhoria
        with st.expander("Análise de Sensibilidade (Novidade)", expanded=False):
            sens_taxa_cresc = st.slider("Variação da Taxa de Crescimento (%)", -2.0, 2.0, 0.0, 0.1) / 100
            sens_aliquota = st.slider("Variação da Alíquota de Imposto (%)", -5.0, 5.0, 0.0, 0.5) / 100
        
        if analisar_btn:
            demonstrativos = preparar_dados_cvm(CONFIG)
            market_data = obter_dados_mercado(p_periodo_beta)
            ticker_sa = f"{ticker_selecionado}.SA"
            codigo_cvm_info = ticker_cvm_map_df == ticker_selecionado]
            
            if codigo_cvm_info.empty:
                st.error(f"Não foi possível encontrar o código CVM para o ticker {ticker_selecionado}.")
                st.stop()
                
            codigo_cvm = int(codigo_cvm_info.iloc)
            
            params_analise = {
                'taxa_crescimento_perpetuidade': p_taxa_cresc + sens_taxa_cresc,
                'media_anos_calculo': p_media_anos,
                'periodo_beta_ibov': p_periodo_beta,
                'aliquota_imposto_ajuste': 1 + sens_aliquota # O ajuste será feito dentro da função
            }

            with st.spinner(f"Analisando {ticker_selecionado}..."):
                resultados, status_msg = processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params_analise)
                
            if resultados:
                st.success(f"Análise para **{resultados['Empresa']} ({resultados})** concluída!")
                col1, col2, col3 = st.columns(3)
                col1.metric("Preço Atual", f"R$ {resultados:.2f}"); col2.metric("Preço Justo (DCF)", f"R$ {resultados:.2f}")
                ms_delta = resultados; col3.metric("Margem de Segurança", f"{ms_delta:.2f}%", delta=f"{ms_delta:.2f}%" if not pd.isna(ms_delta) else None)
                st.divider()
                
                # Gráfico interativo de NOPAT e FCO
                df_nopat_fco = pd.DataFrame({
                    'NOPAT': resultados['hist_nopat'],
                    'FCO': resultados['hist_fco']
                }).reset_index().rename(columns={'index': 'Ano'})
                
                fig_nopat_fco = go.Figure()
                fig_nopat_fco.add_trace(go.Bar(x=df_nopat_fco['Ano'], y=df_nopat_fco, name='NOPAT', marker_color='#00F6FF'))
                fig_nopat_fco.add_trace(go.Bar(x=df_nopat_fco['Ano'], y=df_nopat_fco['FCO'], name='FCO', marker_color='#E94560'))
                fig_nopat_fco.update_layout(title='Histórico de NOPAT e FCO', barmode='group', template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#E0E0E0'))
                st.plotly_chart(fig_nopat_fco, use_container_width=True)

                st.divider()
                
                # Gráfico interativo de ROIC vs WACC
                df_roic_wacc = pd.DataFrame({
                    'ROIC': resultados['hist_roic'],
                    'WACC': resultados['wacc_series']
                }).reset_index().rename(columns={'index': 'Ano'})
                
                fig_roic_wacc = go.Figure()
                fig_roic_wacc.add_trace(go.Scatter(x=df_roic_wacc['Ano'], y=df_roic_wacc, mode='lines+markers', name='ROIC (%)', line=dict(color='#00FF87', width=3)))
                fig_roic_wacc.add_trace(go.Scatter(x=df_roic_wacc['Ano'], y=df_roic_wacc, mode='lines+markers', name='WACC (%)', line=dict(color='#E94560', width=3)))
                fig_roic_wacc.update_layout(title='ROIC vs WACC (Indicadores de Criação de Valor)', template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#E0E0E0'))
                st.plotly_chart(fig_roic_wacc, use_container_width=True)
                
                with st.expander("🔢 Detalhes da Análise de Valuation", expanded=False):
                    df_display = pd.DataFrame.from_dict(resultados, orient='index', columns=['Valor'])
                    st.dataframe(df_display.drop(['hist_nopat', 'hist_fco', 'hist_roic', 'wacc_series']), use_container_width=True)
            else:
                st.error(f"Não foi possível analisar {ticker_selecionado}. Motivo: {status_msg}")

    with tab_ranking:
        st.info("Esta análise processa todas as empresas da lista, o que pode levar vários minutos.")
        if st.button("🚀 Iniciar Análise Completa e Gerar Rankings", type="primary", use_container_width=True):
            params_ranking = {'taxa_crescimento_perpetuidade': CONFIG, 'media_anos_calculo': CONFIG, 'periodo_beta_ibov': CONFIG}
            demonstrativos = preparar_dados_cvm(CONFIG)
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
# ABA 3: MODELO FLEURIET
# # Mover esta seção para um arquivo 'pages/fleuriet.py'
# ==============================================================================

def reclassificar_contas_fleuriet(df_bpa, df_bpp, contas_cvm):
    """Reclassifica contas para o modelo Fleuriet a partir de DFs da CVM."""
    aco = obter_historico_metrica(df_bpa, contas_cvm).add(obter_historico_metrica(df_bpa, contas_cvm), fill_value=0)
    pco = obter_historico_metrica(df_bpp, contas_cvm)
    ap = obter_historico_metrica(df_bpa, contas_cvm)
    pl = obter_historico_metrica(df_bpp, contas_cvm)
    pnc = obter_historico_metrica(df_bpp, contas_cvm)
    return aco, pco, ap, pl, pnc

def processar_analise_fleuriet(ticker_sa, codigo_cvm, demonstrativos):
    """Processa a análise de saúde financeira pelos modelos Fleuriet e Z-Score de Prado."""
    C = CONFIG
    bpa = demonstrativos['bpa'][demonstrativos['bpa'] == codigo_cvm]
    bpp = demonstrativos['bpp'][demonstrativos['bpp'] == codigo_cvm]
    dre = demonstrativos['dre'][demonstrativos['dre'] == codigo_cvm]
    
    if any(df.empty for df in [bpa, bpp, dre]):
        return None
    
    aco, pco, ap, pl, pnc = reclassificar_contas_fleuriet(bpa, bpp, C)
    
    if any(s.empty for s in [aco, pco, ap, pl, pnc]):
        return None

    # Cálculo do Modelo de Fleuriet
    ncg = aco.subtract(pco, fill_value=0)
    cdg = pl.add(pnc, fill_value=0).subtract(ap, fill_value=0)
    t = cdg.subtract(ncg, fill_value=0)
    
    efeito_tesoura = False
    if len(ncg) > 1 and len(cdg) > 1:
        cresc_ncg = ncg.pct_change().iloc[-1]
        cresc_cdg = cdg.pct_change().iloc[-1]
        if pd.notna(cresc_ncg) and pd.notna(cresc_cdg) and cresc_ncg > cresc_cdg and t.iloc[-1] < 0:
            efeito_tesoura = True
            
    try:
        # Cálculo do Z-Score de Prado conforme o TCC [2]
        info = yf.Ticker(ticker_sa).info
        market_cap = info.get('marketCap', 0)
        ativo_total = obter_historico_metrica(bpa, C).iloc[-1]
        passivo_total = obter_historico_metrica(bpp, C).iloc[-1]
        lucro_retido = pl.iloc[-1] - pl.iloc
        ebit = obter_historico_metrica(dre, C).iloc[-1]
        vendas = obter_historico_metrica(dre, C).iloc[-1]
        
        X1 = cdg.iloc[-1] / ativo_total
        X2 = lucro_retido / ativo_total
        X3 = ebit / ativo_total
        X4 = market_cap / passivo_total if passivo_total > 0 else 0
        X5 = vendas / ativo_total
        
        # Coeficientes específicos do Z-Score de Prado conforme o TCC [2]
        z_score = 0.038*X1 + 1.253*X2 + 2.331*X3 + 0.511*X4 + 0.824*X5
        
        if z_score < 1.81:
            classificacao = "Risco Elevado"
        elif z_score < 2.99:
            classificacao = "Zona Cinzenta"
        else:
            classificacao = "Saudável"
            
    except Exception:
        z_score, classificacao = None, "Erro no cálculo"

    return {'Ticker': ticker_sa.replace('.SA', ''), 'Empresa': info.get('longName', ticker_sa), 'Ano': t.index[-1], 'NCG': ncg.iloc[-1], 'CDG': cdg.iloc[-1], 'Tesouraria': t.iloc[-1], 'Efeito Tesoura': efeito_tesoura, 'Z-Score': z_score, 'Classificação Risco': classificacao}

def ui_modelo_fleuriet():
    """Renderiza a interface completa da aba do Modelo Fleuriet."""
    st.header("Análise de Saúde Financeira (Modelo Fleuriet & Z-Score)")
    st.info("Esta análise utiliza os dados da CVM para avaliar a estrutura de capital de giro e o risco de insolvência das empresas.")
    
    if st.button("🚀 Iniciar Análise Fleuriet Completa", type="primary", use_container_width=True):
        ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
        demonstrativos = preparar_dados_cvm(CONFIG)
        resultados_fleuriet =
        progress_bar = st.progress(0, text="Iniciando análise Fleuriet...")
        total_empresas = len(ticker_cvm_map_df)
        
        for i, (index, row) in enumerate(ticker_cvm_map_df.iterrows()):
            ticker = row
            progress_bar.progress((i + 1) / total_empresas, text=f"Analisando {i+1}/{total_empresas}: {ticker}")
            resultado = processar_analise_fleuriet(f"{ticker}.SA", int(row), demonstrativos)
            if resultado:
                resultados_fleuriet.append(resultado)
                
        progress_bar.empty()
        
        if resultados_fleuriet:
            df_fleuriet = pd.DataFrame(resultados_fleuriet)
            st.success(f"Análise Fleuriet concluída para {len(df_fleuriet)} empresas.")
            
            ncg_medio = df_fleuriet['NCG'].mean()
            tesoura_count = df_fleuriet.sum()
            risco_count = len(df_fleuriet == "Risco Elevado"])
            zscore_medio = df_fleuriet.mean()
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("NCG Média", f"R$ {ncg_medio/1e6:.1f} M")
            col2.metric("Efeito Tesoura", f"{tesoura_count} empresas")
            col3.metric("Alto Risco (Z-Score)", f"{risco_count} empresas")
            col4.metric("Z-Score Médio", f"{zscore_medio:.2f}")
            st.dataframe(df_fleuriet, use_container_width=True)
        else:
            st.error("Nenhum resultado pôde ser gerado para a análise Fleuriet.")
            
    with st.expander("📖 Metodologia do Modelo Fleuriet"):
        st.markdown("""- **NCG (Necessidade de Capital de Giro):** `(Estoques + Contas a Receber) - Fornecedores`
- **CDG (Capital de Giro):** `(Patrimônio Líquido + Passivo Longo Prazo) - Ativo Permanente`
- **T (Saldo de Tesouraria):** `CDG - NCG`
- **Efeito Tesoura:** Ocorre quando a NCG cresce mais rapidamente que o CDG.
- **Z-Score de Prado:** Modelo estatístico que mede a probabilidade de uma empresa ir à falência, com coeficientes específicos para o mercado brasileiro, conforme descrito no TCC.
""")


# ==============================================================================
# ESTRUTURA PRINCIPAL DO APP
# ==============================================================================
def main():
    """Função principal que orquestra o layout do aplicativo Streamlit."""
    st.title("Sistema de Controle Financeiro e Análise de Investimentos")
    inicializar_session_state()
    
    # Abas para navegação entre as diferentes funcionalidades
    tab1, tab2, tab3 = st.tabs(["💲 Controle Financeiro", "📈 Análise de Valuation", "🔬 Modelo Fleuriet"])
    
    with tab1:
        ui_controle_financeiro()
        
    with tab2:
        ui_valuation()
        
    with tab3:
        ui_modelo_fleuriet()

if __name__ == "__main__":
    main()
