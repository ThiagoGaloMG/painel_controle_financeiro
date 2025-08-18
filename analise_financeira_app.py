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

# Estilo CSS para um tema escuro e profissional, inspirado nas imagens
st.markdown("""
<style>
    .main .block-container {
        padding-top: 2rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #1E1E1E;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #282828;
    }
    .stMetric {
        border: 1px solid #444;
        border-radius: 8px;
        padding: 15px;
        background-color: #282828;
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
# ABA 1: CONTROLE FINANCEIRO - LÓGICA E UI
# ==============================================================================

def inicializar_session_state():
    """Inicializa o estado da sessão para simular um banco de dados."""
    if 'transactions' not in st.session_state:
        st.session_state.transactions = pd.DataFrame(columns=['Data', 'Tipo', 'Categoria', 'Subcategoria ARCA', 'Valor', 'Descrição'])
    if 'categories' not in st.session_state:
        st.session_state.categories = {'Receita': ['Salário', 'Freelance'], 'Despesa': ['Moradia', 'Alimentação', 'Transporte'], 'Investimento': ['Ações BR', 'FIIs', 'Ações INT', 'Caixa']}
    if 'goals' not in st.session_state:
        st.session_state.goals = {
            'Reserva de Emergência': {'meta': 10000, 'atual': 0},
            'Liberdade Financeira': {'meta': 1000000, 'atual': 0}
        }

def ui_controle_financeiro():
    """Renderiza a interface completa da aba de Controle Financeiro."""
    st.header("Dashboard de Controle Financeiro Pessoal")
    
    # --- Colunas para Lançamentos e Metas ---
    col1, col2 = st.columns(2)
    with col1:
        with st.expander("➕ Novo Lançamento", expanded=True):
            with st.form("new_transaction_form", clear_on_submit=True):
                data = st.date_input("Data", datetime.now())
                tipo = st.selectbox("Tipo", ["Receita", "Despesa", "Investimento"])
                
                # Categorias dinâmicas
                if tipo == "Investimento":
                    categoria = st.selectbox("Categoria (Metodologia ARCA)", options=st.session_state.categories['Investimento'])
                else:
                    categoria = st.selectbox(f"Categoria de {tipo}", options=st.session_state.categories[tipo])

                valor = st.number_input("Valor (R$)", min_value=0.0, format="%.2f")
                descricao = st.text_input("Descrição (opcional)")
                submitted = st.form_submit_button("Adicionar Lançamento")

                if submitted:
                    # Lógica para adicionar transação (aqui iria o INSERT no DB)
                    sub_arca = categoria if tipo == "Investimento" else None
                    nova_transacao = pd.DataFrame([{'Data': data, 'Tipo': tipo, 'Categoria': categoria, 'Subcategoria ARCA': sub_arca, 'Valor': valor, 'Descrição': descricao}])
                    st.session_state.transactions = pd.concat([st.session_state.transactions, nova_transacao], ignore_index=True)
                    st.success("Lançamento adicionado!")
    
    with col2:
        with st.expander("🎯 Metas Financeiras", expanded=True):
            meta_selecionada = st.selectbox("Selecione a meta para definir", options=list(st.session_state.goals.keys()))
            novo_valor_meta = st.number_input("Definir Valor Alvo (R$)", min_value=0.0, value=st.session_state.goals[meta_selecionada]['meta'], format="%.2f")
            if st.button("Atualizar Meta"):
                # Lógica para atualizar meta (aqui iria o UPDATE no DB)
                st.session_state.goals[meta_selecionada]['meta'] = novo_valor_meta
                st.success(f"Meta '{meta_selecionada}' atualizada!")
    
    st.divider()

    # --- Cálculos para os cards e gráficos ---
    df_trans = st.session_state.transactions
    df_trans['Data'] = pd.to_datetime(df_trans['Data'])
    
    # Mapeamento ARCA para as metas
    invest_produtivos = df_trans[(df_trans['Tipo'] == 'Investimento') & (df_trans['Subcategoria ARCA'].isin(['Ações BR', 'FIIs', 'Ações INT']))]['Valor'].sum()
    caixa = df_trans[(df_trans['Tipo'] == 'Investimento') & (df_trans['Subcategoria ARCA'] == 'Caixa')]['Valor'].sum()
    
    st.session_state.goals['Liberdade Financeira']['atual'] = invest_produtivos
    st.session_state.goals['Reserva de Emergência']['atual'] = caixa

    # --- Cards de Resumo ---
    col1, col2, col3 = st.columns(3)
    with col1: # Distribuição ARCA
        st.subheader("Distribuição ARCA")
        df_arca = df_trans[df_trans['Tipo'] == 'Investimento'].groupby('Subcategoria ARCA')['Valor'].sum()
        if not df_arca.empty:
            fig_arca = px.pie(df_arca, values='Valor', names=df_arca.index, title="Composição dos Investimentos", hole=.3)
            fig_arca.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_arca, use_container_width=True)
        else:
            st.info("Nenhum investimento ARCA registrado.")

    with col2: # Reserva de Emergência
        st.subheader("Reserva de Emergência")
        meta = st.session_state.goals['Reserva de Emergência']['meta']
        atual = st.session_state.goals['Reserva de Emergência']['atual']
        progresso = (atual / meta) if meta > 0 else 0
        st.metric("Valor em Caixa", f"R$ {atual:,.2f}")
        st.progress(progresso, text=f"{progresso:.1%} da meta de R$ {meta:,.2f}")

    with col3: # Liberdade Financeira
        st.subheader("Liberdade Financeira")
        meta = st.session_state.goals['Liberdade Financeira']['meta']
        atual = st.session_state.goals['Liberdade Financeira']['atual']
        progresso = (atual / meta) if meta > 0 else 0
        st.metric("Investimentos Produtivos", f"R$ {atual:,.2f}")
        st.progress(progresso, text=f"{progresso:.1%} da meta de R$ {meta:,.2f}")
        
    st.divider()

    # --- Gráficos de Evolução ---
    st.subheader("Análise Histórica")
    if not df_trans.empty:
        df_monthly = df_trans.set_index('Data').groupby([pd.Grouper(freq='M'), 'Tipo'])['Valor'].sum().unstack(fill_value=0)
        
        col1, col2 = st.columns(2)
        with col1:
            fig_evol_tipo = px.bar(df_monthly, x=df_monthly.index, y=['Receita', 'Despesa', 'Investimento'], title="Evolução Mensal por Tipo", barmode='group')
            st.plotly_chart(fig_evol_tipo, use_container_width=True)
        
        with col2:
            df_monthly['Patrimonio'] = (df_monthly['Receita'] - df_monthly['Despesa']).cumsum()
            fig_evol_patrimonio = px.line(df_monthly, x=df_monthly.index, y='Patrimonio', title="Evolução Patrimonial", markers=True)
            st.plotly_chart(fig_evol_patrimonio, use_container_width=True)
    else:
        st.info("Adicione transações para visualizar os gráficos de evolução.")

    st.divider()

    # --- Tabela de Histórico de Transações ---
    with st.expander("📜 Histórico de Transações", expanded=True):
        if not df_trans.empty:
            # Filtros
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                tipo_filtro = st.selectbox("Filtrar por tipo", ["Todos"] + list(df_trans['Tipo'].unique()))
            with col2:
                desc_filtro = st.text_input("Buscar na descrição...")
            with col3:
                data_inicio = st.date_input("Data início", df_trans['Data'].min())
            with col4:
                data_fim = st.date_input("Data fim", df_trans['Data'].max())

            df_filtrado = df_trans.copy()
            if tipo_filtro != "Todos":
                df_filtrado = df_filtrado[df_filtrado['Tipo'] == tipo_filtro]
            if desc_filtro:
                df_filtrado = df_filtrado[df_filtrado['Descrição'].str.contains(desc_filtro, case=False, na=False)]
            
            df_filtrado = df_filtrado[(df_filtrado['Data'].dt.date >= data_inicio) & (df_filtrado['Data'].dt.date <= data_fim)]
            
            st.dataframe(df_filtrado.sort_values(by="Data", ascending=False), use_container_width=True)
        else:
            st.info("Nenhuma transação registrada.")


# ==============================================================================
# ABA 2: VALUATION - LÓGICA E UI (CÓDIGO EXISTENTE ADAPTADO)
# ==============================================================================
# Funções de dados e valuation (preparar_dados_cvm, carregar_mapeamento, etc.)
# ... (O código das próximas seções será inserido aqui)

def ui_valuation():
    """Renderiza a interface da aba de Valuation."""
    st.header("Análise de Valuation e Scanner de Mercado")
    
    tab_individual, tab_ranking = st.tabs(["Análise de Ativo Individual", "🔍 Scanner de Mercado (Ranking)"])

    # Carregamento de dados essenciais
    ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
    if ticker_cvm_map_df.empty:
        st.error("Não foi possível carregar o mapeamento de tickers.")
        st.stop()
    
    # --- ABA DE ANÁLISE INDIVIDUAL ---
    with tab_individual:
        # ... (código da aba individual do valuation, como na versão anterior)
        pass # Placeholder para o código já existente

    # --- ABA DE RANKING ---
    with tab_ranking:
        # ... (código da aba de ranking do valuation, como na versão anterior)
        pass # Placeholder para o código já existente

# ==============================================================================
# ABA 3: MODELO FLEURIET - LÓGICA E UI
# ==============================================================================

def reclassificar_contas_fleuriet(df_bpa, df_bpp, contas_cvm):
    """Reclassifica as contas do balanço para o modelo Fleuriet."""
    aco = obter_historico_metrica(df_bpa, contas_cvm['ESTOQUES']).add(obter_historico_metrica(df_bpa, contas_cvm['CONTAS_A_RECEBER']), fill_value=0)
    pco = obter_historico_metrica(df_bpp, contas_cvm['FORNECEDORES'])
    
    acf = obter_historico_metrica(df_bpa, contas_cvm['CAIXA'])
    pcf = obter_historico_metrica(df_bpp, contas_cvm['DIVIDA_CURTO_PRAZO'])
    
    ap = obter_historico_metrica(df_bpa, contas_cvm['ATIVO_NAO_CIRCULANTE'])
    pl = obter_historico_metrica(df_bpp, contas_cvm['PATRIMONIO_LIQUIDO'])
    pnc = obter_historico_metrica(df_bpp, contas_cvm['PASSIVO_NAO_CIRCULANTE'])
    
    return aco, pco, acf, pcf, ap, pl, pnc

def processar_analise_fleuriet(ticker_sa, codigo_cvm, demonstrativos, market_data):
    """Calcula os indicadores do Modelo Fleuriet e Z-Score de Prado para uma empresa."""
    C = CONFIG['CONTAS_CVM']
    bpa = demonstrativos['bpa'][demonstrativos['bpa']['CD_CVM'] == codigo_cvm]
    bpp = demonstrativos['bpp'][demonstrativos['bpp']['CD_CVM'] == codigo_cvm]
    dre = demonstrativos['dre'][demonstrativos['dre']['CD_CVM'] == codigo_cvm]

    if any(df.empty for df in [bpa, bpp, dre]): return None
    
    # Reclassificação
    aco, pco, acf, pcf, ap, pl, pnc = reclassificar_contas_fleuriet(bpa, bpp, C)
    
    # Cálculos Fleuriet
    ncg = aco.subtract(pco, fill_value=0)
    cdg = pl.add(pnc, fill_value=0).subtract(ap, fill_value=0)
    t = cdg.subtract(ncg, fill_value=0)

    # Efeito Tesoura (simplificado: NCG cresceu mais que CDG no último ano?)
    efeito_tesoura = False
    if len(ncg) > 1 and len(cdg) > 1:
        cresc_ncg = ncg.pct_change().iloc[-1]
        cresc_cdg = cdg.pct_change().iloc[-1]
        if pd.notna(cresc_ncg) and pd.notna(cresc_cdg) and cresc_ncg > cresc_cdg:
            efeito_tesoura = True
            
    # Z-Score de Prado
    try:
        info = yf.Ticker(ticker_sa).info
        market_cap = info.get('marketCap', 0)
        
        ativo_total = obter_historico_metrica(bpa, C['ATIVO_TOTAL']).iloc[-1]
        passivo_total = obter_historico_metrica(bpp, C['PASSIVO_TOTAL']).iloc[-1]
        lucro_retido = pl.iloc[-1] - pl.iloc[0] # Simplificação
        ebit = obter_historico_metrica(dre, C['EBIT']).iloc[-1]
        vendas = obter_historico_metrica(dre, C['RECEITA_LIQUIDA']).iloc[-1]

        X1 = cdg.iloc[-1] / ativo_total
        X2 = lucro_retido / ativo_total
        X3 = ebit / ativo_total
        X4 = market_cap / passivo_total if passivo_total > 0 else 0
        X5 = vendas / ativo_total
        
        z_score = 0.038*X1 + 1.253*X2 + 2.331*X3 + 0.511*X4 + 0.824*X5
        
        if z_score < 1.81: classificacao = "Risco Elevado"
        elif z_score < 2.99: classificacao = "Zona Cinzenta"
        else: classificacao = "Saudável"
    except Exception:
        z_score, classificacao = None, "Erro no cálculo"

    return {
        'Ticker': ticker_sa.replace('.SA', ''), 'Empresa': info.get('longName', ticker_sa),
        'Ano': t.index[-1], 'NCG': ncg.iloc[-1], 'CDG': cdg.iloc[-1], 'Tesouraria': t.iloc[-1],
        'Efeito Tesoura': efeito_tesoura, 'Z-Score': z_score, 'Classificação Risco': classificacao
    }

def ui_modelo_fleuriet():
    """Renderiza a interface da aba Modelo Fleuriet."""
    st.header("Análise de Saúde Financeira (Modelo Fleuriet & Z-Score)")
    st.info("Esta análise utiliza os dados da CVM para avaliar a estrutura de capital de giro e o risco de insolvência das empresas.")

    if st.button("🚀 Iniciar Análise Fleuriet Completa", type="primary", use_container_width=True):
        # Carregar dados (eles já estarão em cache se já usados em outra aba)
        ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
        demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
        market_data = obter_dados_mercado(CONFIG["PERIODO_BETA_IBOV"])

        resultados_fleuriet = []
        progress_bar = st.progress(0, text="Iniciando análise Fleuriet...")
        total_empresas = len(ticker_cvm_map_df)

        for i, (index, row) in enumerate(ticker_cvm_map_df.iterrows()):
            ticker = row['TICKER']
            progress_bar.progress((i + 1) / total_empresas, text=f"Analisando {i+1}/{total_empresas}: {ticker}")
            
            resultado = processar_analise_fleuriet(f"{ticker}.SA", int(row['CD_CVM']), demonstrativos, market_data)
            if resultado:
                resultados_fleuriet.append(resultado)
        
        progress_bar.empty()
        
        if resultados_fleuriet:
            df_fleuriet = pd.DataFrame(resultados_fleuriet)
            st.success(f"Análise Fleuriet concluída para {len(df_fleuriet)} empresas.")
            
            # Cards de Resumo
            ncg_medio = df_fleuriet['NCG'].mean()
            tesoura_count = df_fleuriet['Efeito Tesoura'].sum()
            risco_count = len(df_fleuriet[df_fleuriet['Classificação Risco'] == "Risco Elevado"])
            zscore_medio = df_fleuriet['Z-Score'].mean()

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("NCG Média", f"R$ {ncg_medio/1e6:.1f} M")
            col2.metric("Efeito Tesoura", f"{tesoura_count} empresas")
            col3.metric("Alto Risco (Z-Score)", f"{risco_count} empresas")
            col4.metric("Z-Score Médio", f"{zscore_medio:.2f}")
            
            st.dataframe(df_fleuriet, use_container_width=True)
        else:
            st.error("Nenhum resultado pôde ser gerado para a análise Fleuriet.")

    with st.expander("📖 Metodologia do Modelo Fleuriet"):
        st.markdown("""
        - **NCG (Necessidade de Capital de Giro):** Representa o investimento líquido em capital de giro operacional. `NCG = (Estoques + Contas a Receber) - Fornecedores`.
        - **CDG (Capital de Giro):** Recursos de longo prazo disponíveis para financiar o giro. `CDG = (Patrimônio Líquido + Passivo Longo Prazo) - Ativo Permanente`.
        - **T (Saldo de Tesouraria):** Folga financeira de curto prazo da empresa. `T = CDG - NCG`.
        - **Efeito Tesoura:** Ocorre quando a NCG cresce mais rapidamente que o CDG, resultando em deterioração do saldo de tesouraria e possível insolvência.
        - **Z-Score de Prado:** Modelo estatístico que mede a probabilidade de uma empresa ir à falência.
        """)
        
# ==============================================================================
# FUNÇÕES PRINCIPAIS E ESTRUTURA DO APP
# ==============================================================================
# As funções de Valuation e dados da CVM precisam ser incluídas aqui.
# Por uma questão de brevidade, vou omitir a repetição delas, mas elas
# devem estar presentes no seu arquivo final.
# ... (cole aqui as funções: setup_diretorios, preparar_dados_cvm, carregar_mapeamento_ticker_cvm,
# consulta_bc, obter_dados_mercado, obter_historico_metrica, calcular_beta,
# processar_valuation_empresa, executar_analise_completa, convert_df_to_csv, exibir_rankings)

def main():
    """Função principal que renderiza o aplicativo com abas."""
    st.title("Sistema de Controle Financeiro e Análise de Investimentos")

    # Inicializa o 'banco de dados' em memória
    inicializar_session_state()

    # Cria as abas principais
    tab1, tab2, tab3 = st.tabs(["💲 Controle Financeiro", "📈 Análise de Valuation", "🔬 Modelo Fleuriet"])

    with tab1:
        ui_controle_financeiro()

    with tab2:
        # A função ui_valuation() agora precisa ser chamada aqui
        # ui_valuation() # Esta chamada renderizaria a aba de valuation
        st.warning("A funcionalidade de Valuation foi movida para esta aba, mas o código completo foi omitido desta resposta para brevidade. Integre o código da versão anterior aqui.")


    with tab3:
        ui_modelo_fleuriet()

if __name__ == "__main__":
    # Nota: O código completo da aba Valuation foi omitido para focar nas novidades.
    # Você deve integrar as funções da versão anterior para que a aba Valuation funcione.
    main()
