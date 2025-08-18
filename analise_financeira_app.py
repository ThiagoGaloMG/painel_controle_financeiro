# -*- coding: utf-8 -*-
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
from plotly.subplots import make_subplots
import time
import uuid
from typing import Dict, List, Optional

# Ignorar avisos para uma saída mais limpa
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURAÇÕES GERAIS E LAYOUT DA PÁGINA
# ==============================================================================
st.set_page_config(
    layout="wide", 
    page_title="Painel de Controle Financeiro Avançado", 
    page_icon="📊",
    initial_sidebar_state="collapsed"
)

# CSS aprimorado com melhor UX e responsividade
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Paleta de Cores Moderna */
    :root {
        --primary-bg: #0D1117;
        --secondary-bg: #161B22;
        --widget-bg: #21262D;
        --primary-accent: #58A6FF;
        --secondary-accent: #F85149;
        --positive-accent: #3FB950;
        --warning-accent: #D29922;
        --text-color: #F0F6FC;
        --text-secondary: #8B949E;
        --border-color: #30363D;
        --hover-bg: #262C36;
    }

    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        max-width: 95%;
    }
    
    /* Título Principal com Animação */
    h1 {
        background: linear-gradient(135deg, var(--primary-accent), var(--positive-accent));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        font-weight: 700;
        font-size: 2.5rem;
        margin-bottom: 2rem;
        animation: glow 2s ease-in-out infinite alternate;
    }
    
    @keyframes glow {
        from { filter: drop-shadow(0 0 5px rgba(88, 166, 255, 0.3)); }
        to { filter: drop-shadow(0 0 20px rgba(88, 166, 255, 0.6)); }
    }
    
    h2, h3 {
        color: var(--text-color);
        font-weight: 600;
    }

    /* Abas Modernas */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: var(--secondary-bg);
        border-radius: 12px;
        padding: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 48px;
        background: transparent;
        border-radius: 8px;
        transition: all 0.3s ease;
        color: var(--text-secondary);
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background: var(--primary-accent);
        color: white;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(88, 166, 255, 0.3);
    }

    /* Cards de Métricas Aprimorados */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, var(--widget-bg) 0%, var(--secondary-bg) 100%);
        border: 1px solid var(--border-color);
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
        backdrop-filter: blur(10px);
    }
    [data-testid="metric-container"]:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 48px rgba(0, 0, 0, 0.4);
        border-color: var(--primary-accent);
    }
    [data-testid="metric-container"] label {
        color: var(--text-secondary);
        font-weight: 500;
        font-size: 0.9rem;
    }
    [data-testid="metric-container"] [data-testid="metric-value"] {
        color: var(--text-color);
        font-weight: 700;
        font-size: 1.8rem;
    }

    /* Botões Interativos */
    .stButton > button {
        background: linear-gradient(135deg, var(--primary-accent), #4A90E2);
        border: none;
        border-radius: 12px;
        color: white;
        font-weight: 600;
        padding: 12px 24px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 16px rgba(88, 166, 255, 0.3);
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(88, 166, 255, 0.5);
        background: linear-gradient(135deg, #4A90E2, var(--primary-accent));
    }

    /* Formulários e Inputs */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div,
    .stNumberInput > div > div > input {
        background: var(--widget-bg);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        color: var(--text-color);
        transition: all 0.3s ease;
    }
    .stTextInput > div > div > input:focus,
    .stSelectbox > div > div > div:focus,
    .stNumberInput > div > div > input:focus {
        border-color: var(--primary-accent);
        box-shadow: 0 0 0 2px rgba(88, 166, 255, 0.2);
    }

    /* Expanders Melhorados */
    [data-testid="stExpander"] {
        background: var(--widget-bg);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
    }
    [data-testid="stExpander"] summary {
        font-weight: 600;
        color: var(--text-color);
        padding: 16px;
    }

    /* Tabelas Modernas */
    .stDataFrame {
        background: var(--widget-bg);
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
    }

    /* Barras de Progresso */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, var(--primary-accent), var(--positive-accent));
        border-radius: 10px;
    }

    /* Alertas e Notificações */
    .stSuccess {
        background: rgba(63, 185, 80, 0.1);
        border-left: 4px solid var(--positive-accent);
        border-radius: 8px;
    }
    .stError {
        background: rgba(248, 81, 73, 0.1);
        border-left: 4px solid var(--secondary-accent);
        border-radius: 8px;
    }
    .stInfo {
        background: rgba(88, 166, 255, 0.1);
        border-left: 4px solid var(--primary-accent);
        border-radius: 8px;
    }

    /* Sidebar Aprimorada */
    .css-1d391kg {
        background: var(--secondary-bg);
    }

    /* Responsividade */
    @media (max-width: 768px) {
        h1 { font-size: 2rem; }
        [data-testid="metric-container"] { padding: 16px; }
        .main .block-container { padding: 1rem; }
    }
</style>""", unsafe_allow_html=True)

# Configurações expandidas
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
# SISTEMA DE PERSISTÊNCIA APRIMORADO
# ==============================================================================

def inicializar_session_state():
    """Inicializa o estado da sessão com estruturas de dados mais robustas."""
    if 'transactions' not in st.session_state:
        st.session_state.transactions = pd.DataFrame(columns=[
            'id', 'Data', 'Tipo', 'Categoria', 'Subcategoria', 'Valor', 'Descrição', 'Tags', 'Recorrente'
        ])
    
    if 'categories' not in st.session_state:
        st.session_state.categories = {
            'Receita': ['Salário', 'Freelance', 'Investimentos', 'Prêmios', 'Outros'],
            'Despesa': ['Moradia', 'Alimentação', 'Transporte', 'Saúde', 'Educação', 'Lazer', 'Vestuário', 'Outros'],
            'Investimento': ['Ações BR', 'FIIs', 'Ações INT', 'Caixa']  # ARCA fixo
        }
    
    if 'goals' not in st.session_state:
        st.session_state.goals = {
            'Reserva de Emergência': {'meta': 10000.0, 'atual': 0.0, 'prazo': datetime.now() + timedelta(days=365)},
            'Liberdade Financeira': {'meta': 1000000.0, 'atual': 0.0, 'prazo': datetime.now() + timedelta(days=3650)},
            'Casa Própria': {'meta': 200000.0, 'atual': 0.0, 'prazo': datetime.now() + timedelta(days=1825)}
        }
    
    if 'budgets' not in st.session_state:
        st.session_state.budgets = {
            'Alimentação': {'limite': 800.0, 'gasto': 0.0},
            'Transporte': {'limite': 400.0, 'gasto': 0.0},
            'Lazer': {'limite': 300.0, 'gasto': 0.0}
        }
    
    if 'recurring_transactions' not in st.session_state:
        st.session_state.recurring_transactions = pd.DataFrame(columns=[
            'id', 'Tipo', 'Categoria', 'Valor', 'Descrição', 'Frequencia', 'Proximo_Vencimento', 'Ativo'
        ])

def gerar_id_unico():
    """Gera um ID único para transações."""
    return str(uuid.uuid4())[:8]

def adicionar_transacao(data, tipo, categoria, subcategoria, valor, descricao, tags="", recorrente=False):
    """Adiciona uma nova transação com validação."""
    try:
        nova_transacao = pd.DataFrame([{
            'id': gerar_id_unico(),
            'Data': pd.to_datetime(data),
            'Tipo': tipo,
            'Categoria': categoria,
            'Subcategoria': subcategoria if subcategoria else None,
            'Valor': float(valor),
            'Descrição': descricao,
            'Tags': tags,
            'Recorrente': recorrente
        }])
        
        st.session_state.transactions = pd.concat([
            st.session_state.transactions, 
            nova_transacao
        ], ignore_index=True)
        
        # Atualizar orçamentos
        if tipo == 'Despesa' and categoria in st.session_state.budgets:
            st.session_state.budgets[categoria]['gasto'] += float(valor)
        
        return True, "Transação adicionada com sucesso!"
    except Exception as e:
        return False, f"Erro ao adicionar transação: {str(e)}"

def calcular_metricas_financeiras():
    """Calcula métricas financeiras abrangentes."""
    df_trans = st.session_state.transactions.copy()
    if df_trans.empty:
        return {
            'total_receitas': 0, 'total_despesas': 0, 'total_investido': 0,
            'saldo_periodo': 0, 'taxa_poupanca': 0, 'patrimonio_liquido': 0
        }
    
    df_trans['Data'] = pd.to_datetime(df_trans['Data'])
    
    # Métricas básicas
    total_receitas = df_trans[df_trans['Tipo'] == 'Receita']['Valor'].sum()
    total_despesas = df_trans[df_trans['Tipo'] == 'Despesa']['Valor'].sum()
    total_investido = df_trans[df_trans['Tipo'] == 'Investimento']['Valor'].sum()
    
    saldo_periodo = total_receitas - total_despesas - total_investido
    taxa_poupanca = ((total_receitas - total_despesas) / total_receitas * 100) if total_receitas > 0 else 0
    patrimonio_liquido = total_investido  # Simplificado
    
    # Métricas por categoria ARCA
    invest_produtivos = df_trans[
        (df_trans['Tipo'] == 'Investimento') & 
        (df_trans['Subcategoria'].isin(['Ações BR', 'FIIs', 'Ações INT']))
    ]['Valor'].sum()
    
    caixa = df_trans[
        (df_trans['Tipo'] == 'Investimento') & 
        (df_trans['Subcategoria'] == 'Caixa')
    ]['Valor'].sum()
    
    return {
        'total_receitas': total_receitas,
        'total_despesas': total_despesas,
        'total_investido': total_investido,
        'saldo_periodo': saldo_periodo,
        'taxa_poupanca': taxa_poupanca,
        'patrimonio_liquido': patrimonio_liquido,
        'invest_produtivos': invest_produtivos,
        'caixa': caixa
    }

# ==============================================================================
# COMPONENTES DE UI APRIMORADOS
# ==============================================================================

def criar_card_metrica(titulo, valor, delta=None, formato_moeda=True, cor_delta="normal"):
    """Cria um card de métrica customizado."""
    if formato_moeda:
        valor_formatado = f"R$ {valor:,.2f}" if valor >= 0 else f"-R$ {abs(valor):,.2f}"
    else:
        valor_formatado = f"{valor:.1f}%" if isinstance(valor, float) else str(valor)
    
    delta_str = f"{delta:+.2f}" if delta and formato_moeda else f"{delta:+.1f}%" if delta else None
    
    st.metric(
        label=titulo,
        value=valor_formatado,
        delta=delta_str
    )

def exibir_dashboard_principal():
    """Exibe o dashboard principal com métricas financeiras."""
    st.subheader("📊 Dashboard Financeiro")
    
    metricas = calcular_metricas_financeiras()
    
    # Primeira linha de métricas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        criar_card_metrica("💰 Receitas Totais", metricas['total_receitas'])
    
    with col2:
        criar_card_metrica("💸 Despesas Totais", metricas['total_despesas'])
    
    with col3:
        criar_card_metrica("📈 Investimentos", metricas['total_investido'])
    
    with col4:
        saldo = metricas['saldo_periodo']
        criar_card_metrica("💵 Saldo Líquido", saldo, delta=saldo)
    
    # Segunda linha de métricas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        criar_card_metrica("💾 Taxa de Poupança", metricas['taxa_poupanca'], formato_moeda=False)
    
    with col2:
        criar_card_metrica("🏦 Patrimônio Líquido", metricas['patrimonio_liquido'])
    
    with col3:
        criar_card_metrica("🚀 Invest. Produtivos", metricas['invest_produtivos'])
    
    with col4:
        criar_card_metrica("💰 Caixa", metricas['caixa'])

def criar_formulario_transacao():
    """Cria formulário aprimorado para nova transação."""
    with st.form("new_transaction_form", clear_on_submit=True):
        st.subheader("➕ Nova Transação")
        
        col1, col2 = st.columns(2)
        
        with col1:
            data = st.date_input("📅 Data", datetime.now())
            tipo = st.selectbox("📋 Tipo", ["Receita", "Despesa", "Investimento"])
            valor = st.number_input("💵 Valor (R$)", min_value=0.0, format="%.2f", step=10.0)
        
        with col2:
            # Categoria dinâmica baseada no tipo
            if tipo == "Investimento":
                categoria = st.selectbox("📂 Categoria ARCA", st.session_state.categories['Investimento'])
                subcategoria = categoria  # Para investimentos, subcategoria = categoria
            else:
                opcoes_categoria = st.session_state.categories[tipo] + ["➕ Nova Categoria"]
                categoria_selecionada = st.selectbox("📂 Categoria", opcoes_categoria)
                
                if categoria_selecionada == "➕ Nova Categoria":
                    categoria = st.text_input("✏️ Nome da Nova Categoria")
                else:
                    categoria = categoria_selecionada
                
                subcategoria = st.text_input("🏷️ Subcategoria (opcional)")
            
            descricao = st.text_input("📝 Descrição")
            tags = st.text_input("🏷️ Tags (separadas por vírgula)")
        
        col1, col2 = st.columns(2)
        with col1:
            recorrente = st.checkbox("🔄 Transação Recorrente")
        
        with col2:
            submitted = st.form_submit_button("✅ Adicionar Transação", type="primary", use_container_width=True)
        
        if submitted and categoria and valor > 0:
            # Adicionar nova categoria se necessário
            if tipo != "Investimento" and categoria not in st.session_state.categories[tipo]:
                st.session_state.categories[tipo].append(categoria)
            
            sucesso, mensagem = adicionar_transacao(
                data, tipo, categoria, subcategoria, valor, descricao, tags, recorrente
            )
            
            if sucesso:
                st.success(mensagem)
                st.rerun()
            else:
                st.error(mensagem)

def exibir_analise_arca():
    """Exibe análise detalhada da metodologia ARCA."""
    st.subheader("🎯 Análise ARCA (Asset Allocation)")
    
    metricas = calcular_metricas_financeiras()
    total_investido = metricas['total_investido']
    
    if total_investido == 0:
        st.info("Adicione investimentos para visualizar a análise ARCA.")
        return
    
    df_trans = st.session_state.transactions[st.session_state.transactions['Tipo'] == 'Investimento']
    df_arca = df_trans.groupby('Subcategoria')['Valor'].sum()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Gráfico de Pizza Interativo
        fig_arca = px.pie(
            df_arca, 
            values='Valor', 
            names=df_arca.index,
            title="🍰 Distribuição dos Investimentos por Classe ARCA",
            hole=0.4,
            template="plotly_dark",
            color_discrete_sequence=['#58A6FF', '#3FB950', '#D29922', '#F85149']
        )
        
        fig_arca.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate="<b>%{label}</b><br>Valor: R$ %{value:,.2f}<br>Percentual: %{percent}<extra></extra>"
        )
        
        fig_arca.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#F0F6FC',
            title_font_size=16,
            title_x=0.5
        )
        
        st.plotly_chart(fig_arca, use_container_width=True)
    
    with col2:
        st.markdown("### 📋 Composição ARCA")
        
        for categoria in ['Ações BR', 'FIIs', 'Ações INT', 'Caixa']:
            valor = df_arca.get(categoria, 0)
            percentual = (valor / total_investido * 100) if total_investido > 0 else 0
            
            # Cor baseada na categoria
            cor_map = {
                'Ações BR': '#58A6FF',
                'FIIs': '#3FB950', 
                'Ações INT': '#D29922',
                'Caixa': '#F85149'
            }
            
            st.markdown(f"""
            <div style="
                background: linear-gradient(90deg, {cor_map[categoria]}20, transparent);
                border-left: 4px solid {cor_map[categoria]};
                padding: 12px;
                margin: 8px 0;
                border-radius: 8px;
            ">
                <strong>{categoria}</strong><br>
                R$ {valor:,.2f} ({percentual:.1f}%)
            </div>
            """, unsafe_allow_html=True)

def exibir_metas_financeiras():
    """Exibe e gerencia metas financeiras."""
    st.subheader("🎯 Metas Financeiras")
    
    metricas = calcular_metricas_financeiras()
    
    # Atualizar valores atuais das metas
    st.session_state.goals['Reserva de Emergência']['atual'] = metricas['caixa']
    st.session_state.goals['Liberdade Financeira']['atual'] = metricas['invest_produtivos']
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        for nome_meta, dados in st.session_state.goals.items():
            progresso = (dados['atual'] / dados['meta']) if dados['meta'] > 0 else 0
            progresso_pct = min(progresso * 100, 100)
            
            # Card da meta com progresso
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, var(--widget-bg), var(--secondary-bg));
                border: 1px solid var(--border-color);
                border-radius: 16px;
                padding: 20px;
                margin: 16px 0;
                box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
            ">
                <h4 style="color: var(--text-color); margin: 0 0 12px 0;">{nome_meta}</h4>
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                    <span style="color: var(--text-secondary);">R$ {dados['atual']:,.2f}</span>
                    <span style="color: var(--text-secondary);">R$ {dados['meta']:,.2f}</span>
                </div>
                <div style="
                    background: var(--border-color);
                    border-radius: 10px;
                    overflow: hidden;
                    height: 12px;
                ">
                    <div style="
                        background: linear-gradient(90deg, var(--primary-accent), var(--positive-accent));
                        height: 100%;
                        width: {progresso_pct}%;
                        transition: width 0.3s ease;
                    "></div>
                </div>
                <div style="text-align: center; margin-top: 8px; color: var(--text-secondary); font-weight: 600;">
                    {progresso_pct:.1f}% concluído
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        with st.expander("⚙️ Gerenciar Metas", expanded=True):
            # Seletor de meta
            meta_selecionada = st.selectbox("Selecionar Meta", list(st.session_state.goals.keys()))
            
            # Novos valores
            novo_valor = st.number_input(
                "Valor Alvo (R$)", 
                min_value=0.0, 
                value=st.session_state.goals[meta_selecionada]['meta'],
                format="%.2f"
            )
            
            nova_data = st.date_input(
                "Prazo",
                value=st.session_state.goals[meta_selecionada]['prazo']
            )
            
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("💾 Atualizar", use_container_width=True):
                    st.session_state.goals[meta_selecionada]['meta'] = novo_valor
                    st.session_state.goals[meta_selecionada]['prazo'] = nova_data
                    st.success("Meta atualizada!")
                    st.rerun()
            
            with col_btn2:
                nova_meta = st.text_input("Nova Meta", placeholder="Nome da meta")
                if st.button("➕ Adicionar", use_container_width=True) and nova_meta:
                    st.session_state.goals[nova_meta] = {
                        'meta': 1000.0, 
                        'atual': 0.0, 
                        'prazo': datetime.now() + timedelta(days=365)
                    }
                    st.success(f"Meta '{nova_meta}' criada!")
                    st.rerun()

def exibir_graficos_evolucao():
    """Exibe gráficos de evolução financeira."""
    st.subheader("📈 Evolução Patrimonial")
    
    df_trans = st.session_state.transactions.copy()
    if df_trans.empty:
        st.info("Adicione transações para visualizar a evolução.")
        return
    
    df_trans['Data'] = pd.to_datetime(df_trans['Data'])
    
    # Agrupamento mensal
    df_monthly = df_trans.set_index('Data').groupby([pd.Grouper(freq='M'), 'Tipo'])['Valor'].sum().unstack(fill_value=0)
    
    # Calcular patrimônio acumulado
    receitas_mensais = df_monthly.get('Receita', pd.Series(0, index=df_monthly.index))
    despesas_mensais = df_monthly.get('Despesa', pd.Series(0, index=df_monthly.index))
    investimentos_mensais = df_monthly.get('Investimento', pd.Series(0, index=df_monthly.index))
    
    df_monthly['Saldo_Mensal'] = receitas_mensais - despesas_mensais - investimentos_mensais
    df_monthly['Patrimonio_Acumulado'] = investimentos_mensais.cumsum()
    df_monthly['Caixa_Acumulado'] = (receitas_mensais - despesas_mensais).cumsum()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de barras - Fluxo mensal
        fig_fluxo = go.Figure()
        
        if 'Receita' in df_monthly.columns:
            fig_fluxo.add_trace(go.Bar(
                x=df_monthly.index,
                y=df_monthly['Receita'],
                name='Receitas',
                marker_color='#3FB950',
                hovertemplate="<b>Receitas</b><br>%{x}<br>R$ %{y:,.2f}<extra></extra>"
            ))
        
        if 'Despesa' in df_monthly.columns:
            fig_fluxo.add_trace(go.Bar(
                x=df_monthly.index,
                y=-df_monthly['Despesa'],  # Negativo para visualização
                name='Despesas',
                marker_color='#F85149',
                hovertemplate="<b>Despesas</b><br>%{x}<br>R$ %{y:,.2f}<extra></extra>"
            ))
        
        if 'Investimento' in df_monthly.columns:
            fig_fluxo.add_trace(go.Bar(
                x=df_monthly.index,
                y=df_monthly['Investimento'],
                name='Investimentos',
                marker_color='#58A6FF',
                hovertemplate="<b>Investimentos</b><br>%{x}<br>R$ %{y:,.2f}<extra></extra>"
            ))
        
        fig_fluxo.update_layout(
            title="💰 Fluxo de Caixa Mensal",
            xaxis_title="Período",
            yaxis_title="Valor (R$)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#F0F6FC',
            barmode='relative',
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_fluxo, use_container_width=True)
    
    with col2:
        # Gráfico de linhas - Evolução patrimonial
        fig_patrimonio = go.Figure()
        
        fig_patrimonio.add_trace(go.Scatter(
            x=df_monthly.index,
            y=df_monthly['Patrimonio_Acumulado'],
            mode='lines+markers',
            name='Patrimônio Investido',
            line=dict(color='#58A6FF', width=3),
            marker=dict(size=8),
            hovertemplate="<b>Patrimônio</b><br>%{x}<br>R$ %{y:,.2f}<extra></extra>"
        ))
        
        fig_patrimonio.add_trace(go.Scatter(
            x=df_monthly.index,
            y=df_monthly['Caixa_Acumulado'],
            mode='lines+markers',
            name='Caixa Líquido',
            line=dict(color='#3FB950', width=3),
            marker=dict(size=8),
            hovertemplate="<b>Caixa</b><br>%{x}<br>R$ %{y:,.2f}<extra></extra>"
        ))
        
        fig_patrimonio.update_layout(
            title="📈 Evolução Patrimonial",
            xaxis_title="Período",
            yaxis_title="Valor Acumulado (R$)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#F0F6FC',
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_patrimonio, use_container_width=True)

def exibir_orcamento_categorias():
    """Exibe controle de orçamento por categorias."""
    st.subheader("💰 Controle de Orçamento")
    
    # Calcular gastos por categoria no mês atual
    df_trans = st.session_state.transactions.copy()
    if not df_trans.empty:
        df_trans['Data'] = pd.to_datetime(df_trans['Data'])
        mes_atual = datetime.now().replace(day=1)
        df_mes = df_trans[
            (df_trans['Data'] >= mes_atual) & 
            (df_trans['Tipo'] == 'Despesa')
        ]
        gastos_mes = df_mes.groupby('Categoria')['Valor'].sum()
    else:
        gastos_mes = pd.Series()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📊 Status do Orçamento Mensal")
        
        for categoria, dados in st.session_state.budgets.items():
            gasto_real = gastos_mes.get(categoria, 0)
            limite = dados['limite']
            percentual_usado = (gasto_real / limite * 100) if limite > 0 else 0
            
            # Cor baseada no percentual usado
            if percentual_usado <= 50:
                cor = '#3FB950'  # Verde
            elif percentual_usado <= 80:
                cor = '#D29922'  # Amarelo
            else:
                cor = '#F85149'  # Vermelho
            
            st.markdown(f"""
            <div style="
                background: var(--widget-bg);
                border: 1px solid var(--border-color);
                border-radius: 12px;
                padding: 16px;
                margin: 12px 0;
            ">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                    <h4 style="color: var(--text-color); margin: 0;">{categoria}</h4>
                    <span style="color: {cor}; font-weight: 600;">{percentual_usado:.1f}%</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                    <span style="color: var(--text-secondary);">R$ {gasto_real:,.2f}</span>
                    <span style="color: var(--text-secondary);">R$ {limite:,.2f}</span>
                </div>
                <div style="
                    background: var(--border-color);
                    border-radius: 6px;
                    overflow: hidden;
                    height: 8px;
                ">
                    <div style="
                        background: {cor};
                        height: 100%;
                        width: {min(percentual_usado, 100)}%;
                        transition: width 0.3s ease;
                    "></div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        with st.expander("⚙️ Configurar Orçamentos", expanded=True):
            categoria_orcamento = st.selectbox(
                "Categoria",
                list(st.session_state.budgets.keys()) + ["➕ Nova Categoria"]
            )
            
            if categoria_orcamento == "➕ Nova Categoria":
                nova_categoria = st.text_input("Nome da Categoria")
                limite_orcamento = st.number_input("Limite Mensal (R$)", min_value=0.0, value=500.0)
                
                if st.button("✅ Criar Orçamento", use_container_width=True) and nova_categoria:
                    st.session_state.budgets[nova_categoria] = {
                        'limite': limite_orcamento,
                        'gasto': 0.0
                    }
                    st.success(f"Orçamento para '{nova_categoria}' criado!")
                    st.rerun()
            else:
                limite_atual = st.session_state.budgets[categoria_orcamento]['limite']
                novo_limite = st.number_input(
                    "Novo Limite (R$)",
                    min_value=0.0,
                    value=limite_atual
                )
                
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    if st.button("💾 Atualizar", use_container_width=True):
                        st.session_state.budgets[categoria_orcamento]['limite'] = novo_limite
                        st.success("Orçamento atualizado!")
                        st.rerun()
                
                with col_btn2:
                    if st.button("🗑️ Excluir", use_container_width=True):
                        del st.session_state.budgets[categoria_orcamento]
                        st.success("Orçamento removido!")
                        st.rerun()

def exibir_historico_transacoes():
    """Exibe histórico de transações com filtros avançados."""
    st.subheader("📜 Histórico de Transações")
    
    df_trans = st.session_state.transactions.copy()
    if df_trans.empty:
        st.info("Nenhuma transação registrada ainda.")
        return
    
    df_trans['Data'] = pd.to_datetime(df_trans['Data'])
    
    # Filtros avançados
    with st.expander("🔍 Filtros Avançados", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            tipo_filtro = st.multiselect(
                "Tipo", 
                options=df_trans['Tipo'].unique(),
                default=df_trans['Tipo'].unique()
            )
        
        with col2:
            categoria_filtro = st.multiselect(
                "Categoria",
                options=df_trans['Categoria'].unique(),
                default=df_trans['Categoria'].unique()
            )
        
        with col3:
            data_inicio = st.date_input(
                "Data Início",
                value=df_trans['Data'].min().date()
            )
        
        with col4:
            data_fim = st.date_input(
                "Data Fim",
                value=df_trans['Data'].max().date()
            )
        
        # Filtro de valor
        col1, col2 = st.columns(2)
        with col1:
            valor_min = st.number_input("Valor Mínimo (R$)", min_value=0.0, value=0.0)
        with col2:
            valor_max = st.number_input("Valor Máximo (R$)", min_value=0.0, value=float(df_trans['Valor'].max()))
        
        # Busca por texto
        busca_texto = st.text_input("🔎 Buscar na descrição ou tags...")
    
    # Aplicar filtros
    df_filtrado = df_trans[
        (df_trans['Tipo'].isin(tipo_filtro)) &
        (df_trans['Categoria'].isin(categoria_filtro)) &
        (df_trans['Data'].dt.date >= data_inicio) &
        (df_trans['Data'].dt.date <= data_fim) &
        (df_trans['Valor'] >= valor_min) &
        (df_trans['Valor'] <= valor_max)
    ]
    
    if busca_texto:
        mask_busca = (
            df_filtrado['Descrição'].str.contains(busca_texto, case=False, na=False) |
            df_filtrado['Tags'].str.contains(busca_texto, case=False, na=False)
        )
        df_filtrado = df_filtrado[mask_busca]
    
    # Estatísticas do filtro
    col1, col2, col3 = st.columns(3)
    col1.metric("📊 Transações", len(df_filtrado))
    col2.metric("💰 Total", f"R$ {df_filtrado['Valor'].sum():,.2f}")
    col3.metric("📈 Média", f"R$ {df_filtrado['Valor'].mean():,.2f}" if not df_filtrado.empty else "R$ 0,00")
    
    # Tabela editável
    if not df_filtrado.empty:
        df_para_editar = df_filtrado.copy()
        df_para_editar['Excluir'] = False
        df_para_editar['Data'] = df_para_editar['Data'].dt.date
        df_para_editar = df_para_editar.sort_values('Data', ascending=False)
        
        # Configuração das colunas
        config_colunas = {
            "Excluir": st.column_config.CheckboxColumn("❌"),
            "Data": st.column_config.DateColumn("📅 Data", format="DD/MM/YYYY"),
            "Tipo": st.column_config.SelectboxColumn("📋 Tipo", options=["Receita", "Despesa", "Investimento"]),
            "Categoria": st.column_config.TextColumn("📂 Categoria"),
            "Subcategoria": st.column_config.TextColumn("🏷️ Subcategoria"),
            "Valor": st.column_config.NumberColumn("💵 Valor", format="R$ %.2f"),
            "Descrição": st.column_config.TextColumn("📝 Descrição"),
            "Tags": st.column_config.TextColumn("🏷️ Tags"),
            "id": None  # Ocultar ID
        }
        
        edited_df = st.data_editor(
            df_para_editar,
            column_config=config_colunas,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic"
        )
        
        # Botões de ação
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🗑️ Excluir Selecionados", type="secondary", use_container_width=True):
                indices_excluir = edited_df[edited_df['Excluir']].index
                if not indices_excluir.empty:
                    st.session_state.transactions = st.session_state.transactions.drop(indices_excluir).reset_index(drop=True)
                    st.success(f"{len(indices_excluir)} transação(ões) excluída(s)!")
                    st.rerun()
                else:
                    st.warning("Selecione transações para excluir.")
        
        with col2:
            # Salvar alterações
            if st.button("💾 Salvar Alterações", type="primary", use_container_width=True):
                try:
                    # Atualizar transações editadas (exceto as marcadas para exclusão)
                    df_atualizado = edited_df[~edited_df['Excluir']].copy()
                    df_atualizado = df_atualizado.drop(columns=['Excluir'])
                    df_atualizado['Data'] = pd.to_datetime(df_atualizado['Data'])
                    
                    # Substituir no session state
                    for idx, row in df_atualizado.iterrows():
                        mask = st.session_state.transactions['id'] == row['id']
                        for col in ['Data', 'Tipo', 'Categoria', 'Subcategoria', 'Valor', 'Descrição', 'Tags']:
                            st.session_state.transactions.loc[mask, col] = row[col]
                    
                    st.success("Alterações salvas com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao salvar: {str(e)}")
        
        with col3:
            # Exportar dados
            csv_data = df_filtrado.to_csv(index=False, sep=';', decimal=',')
            st.download_button(
                label="📥 Exportar CSV",
                data=csv_data,
                file_name=f"transacoes_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )

# ==============================================================================
# LÓGICA DE DADOS CVM (MELHORADA)
# ==============================================================================

@st.cache_data
def setup_diretorios():
    """Configura diretórios necessários."""
    try:
        CONFIG["DIRETORIO_DADOS_CVM"].mkdir(parents=True, exist_ok=True)
        CONFIG["DIRETORIO_DADOS_EXTRAIDOS"].mkdir(parents=True, exist_ok=True)
        return True
    except Exception:
        return False

@st.cache_data(show_spinner=False)
def preparar_dados_cvm(anos_historico):
    """Prepara dados da CVM com cache otimizado."""
    ano_final = datetime.today().year
    ano_inicial = ano_final - anos_historico
    
    with st.spinner(f"🔄 Carregando dados CVM ({ano_inicial}-{ano_final-1})..."):
        demonstrativos_consolidados = {}
        tipos_demonstrativos = ['DRE', 'BPA', 'BPP', 'DFC_MI']
        
        progress_bar = st.progress(0)
        total_operacoes = len(tipos_demonstrativos) * anos_historico
        operacao_atual = 0
        
        for tipo in tipos_demonstrativos:
            lista_dfs_anuais = []
            
            for ano in range(ano_inicial, ano_final):
                operacao_atual += 1
                progress_bar.progress(operacao_atual / total_operacoes)
                
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
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        with ZipFile(caminho_zip, 'r') as z:
                            if nome_arquivo_csv in z.namelist():
                                z.extract(nome_arquivo_csv, CONFIG["DIRETORIO_DADOS_EXTRAIDOS"])
                    except Exception as e:
                        st.warning(f"Erro ao baixar {ano}: {str(e)}")
                        continue
                
                if caminho_arquivo.exists():
                    try:
                        df_anual = pd.read_csv(
                            caminho_arquivo, 
                            sep=';', 
                            encoding='ISO-8859-1', 
                            low_memory=False
                        )
                        lista_dfs_anuais.append(df_anual)
                    except Exception:
                        continue
            
            if lista_dfs_anuais:
                demonstrativos_consolidados[tipo.lower()] = pd.concat(
                    lista_dfs_anuais, 
                    ignore_index=True
                )
        
        progress_bar.empty()
    
    return demonstrativos_consolidados

@st.cache_data
def carregar_mapeamento_ticker_cvm():
    """Carrega mapeamento expandido de tickers para códigos CVM."""
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
22181;HBOR3;HELBOR EMPREENDIMENTOS S.A.
22181;HYPE3;HYPERA S.A.
21008;IFCM3;INFRACOMMERCE CXAAS S.A.
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
23280;SLED4;SARAIVA S.A. LIVREIROS EDITORES
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
    """Consulta API do Banco Central."""
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
    """Obtém dados de mercado otimizados."""
    with st.spinner("📊 Buscando dados de mercado..."):
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
    """Obtém histórico de métrica específica."""
    metric_df = df_empresa[(df_empresa['CD_CONTA'] == codigo_conta) & (df_empresa['ORDEM_EXERC'] == 'ÚLTIMO')]
    if metric_df.empty:
        return pd.Series(dtype=float)
    
    metric_df['DT_REFER'] = pd.to_datetime(metric_df['DT_REFER'])
    metric_df = metric_df.sort_values('DT_REFER').groupby(metric_df['DT_REFER'].dt.year).last()
    return metric_df['VL_CONTA'].sort_index()

# ==============================================================================
# ABA 1: CONTROLE FINANCEIRO APRIMORADO
# ==============================================================================

def ui_controle_financeiro():
    """Interface principal do controle financeiro aprimorado."""
    st.header("💰 Sistema de Controle Financeiro Pessoal")
    
    # Dashboard principal
    exibir_dashboard_principal()
    
    st.divider()
    
    # Layout em abas para melhor organização
    tab_lancamento, tab_analises, tab_metas, tab_orcamento, tab_historico = st.tabs([
        "➕ Novo Lançamento", 
        "📊 Análises", 
        "🎯 Metas", 
        "💰 Orçamento", 
        "📜 Histórico"
    ])
    
    with tab_lancamento:
        col1, col2 = st.columns([1, 1])
        with col1:
            criar_formulario_transacao()
        with col2:
            # Resumo rápido
            st.subheader("📋 Resumo Rápido")
            metricas = calcular_metricas_financeiras()
            
            st.metric("💰 Saldo Atual", f"R$ {metricas['saldo_periodo']:,.2f}")
            st.metric("📈 Taxa de Poupança", f"{metricas['taxa_poupanca']:.1f}%")
            st.metric("🏦 Patrimônio", f"R$ {metricas['patrimonio_liquido']:,.2f}")
            
            # Transações recentes
            df_trans = st.session_state.transactions.copy()
            if not df_trans.empty:
                st.subheader("🕐 Últimas Transações")
                df_trans['Data'] = pd.to_datetime(df_trans['Data'])
                ultimas = df_trans.sort_values('Data', ascending=False).head(5)
                
                for _, trans in ultimas.iterrows():
                    emoji = {"Receita": "💚", "Despesa": "💸", "Investimento": "📈"}
                    st.markdown(f"""
                    <div style="
                        background: var(--widget-bg);
                        border-radius: 8px;
                        padding: 8px;
                        margin: 4px 0;
                        border-left: 3px solid var(--primary-accent);
                    ">
                        {emoji.get(trans['Tipo'], '💰')} <strong>{trans['Categoria']}</strong><br>
                        <small>{trans['Data'].strftime('%d/%m/%Y')} - R$ {trans['Valor']:,.2f}</small>
                    </div>
                    """, unsafe_allow_html=True)
    
    with tab_analises:
        col1, col2 = st.columns(2)
        with col1:
            exibir_analise_arca()
        with col2:
            exibir_graficos_evolucao()
    
    with tab_metas:
        exibir_metas_financeiras()
    
    with tab_orcamento:
        exibir_orcamento_categorias()
    
    with tab_historico:
        exibir_historico_transacoes()

# ==============================================================================
# VALUATION E ANÁLISE DE AÇÕES (MANTIDO DO ORIGINAL)
# ==============================================================================

def calcular_beta(ticker, ibov_data, periodo_beta):
    """Calcula o Beta de uma ação em relação ao Ibovespa."""
    dados_acao = yf.download(ticker, period=periodo_beta, progress=False, auto_adjust=True)['Close']
    if dados_acao.empty:
        return 1.0

    dados_combinados = pd.merge(
        dados_acao, ibov_data['Close'], 
        left_index=True, right_index=True, 
        suffixes=('_acao', '_ibov')
    ).dropna()
    
    retornos_mensais = dados_combinados.resample('M').ffill().pct_change().dropna()

    if len(retornos_mensais) < 2:
        return 1.0

    covariancia = retornos_mensais.cov().iloc[0, 1]
    variancia_mercado = retornos_mensais.iloc[:, 1].var()
    
    return covariancia / variancia_mercado if variancia_mercado != 0 else 1.0

def processar_valuation_empresa(ticker_sa, codigo_cvm, demonstrativos, market_data, params):
    """Processa valuation de uma empresa específica."""
    # Busca dados de mercado com retry
    for i in range(3):
        try:
            info = yf.Ticker(ticker_sa).info
            market_cap = info.get('marketCap')
            preco_atual = info.get('currentPrice', info.get('previousClose'))
            nome_empresa = info.get('longName', ticker_sa)
            n_acoes = info.get('sharesOutstanding')
            
            if all([market_cap, preco_atual, n_acoes, nome_empresa]):
                break
            
            if i == 2:
                return None, "Dados de mercado (YFinance) incompletos."
            
            time.sleep(2)
        except Exception:
            if i == 2:
                return None, "Falha ao buscar dados no Yahoo Finance."
            time.sleep(2)

    (risk_free_rate, _, premio_risco_mercado, ibov_data) = market_data
    dre, bpa, bpp, dfc = demonstrativos['dre'], demonstrativos['bpa'], demonstrativos['bpp'], demonstrativos['dfc_mi']
    
    # Filtrar dados por empresa
    empresa_dre = dre[dre['CD_CVM'] == codigo_cvm]
    empresa_bpa = bpa[bpa['CD_CVM'] == codigo_cvm]
    empresa_bpp = bpp[bpp['CD_CVM'] == codigo_cvm]
    empresa_dfc = dfc[dfc['CD_CVM'] == codigo_cvm]
    
    if any(df.empty for df in [empresa_dre, empresa_bpa, empresa_bpp, empresa_dfc]):
        return None, "Dados CVM históricos incompletos."
    
    C = CONFIG['CONTAS_CVM']
    
    # Calcular métricas históricas
    hist_ebit = obter_historico_metrica(empresa_dre, C['EBIT'])
    hist_impostos = obter_historico_metrica(empresa_dre, C['IMPOSTO_DE_RENDA_CSLL'])
    hist_lai = obter_historico_metrica(empresa_dre, C['LUCRO_ANTES_IMPOSTOS'])
    
    if hist_lai.sum() == 0 or hist_ebit.empty:
        return None, "Dados de Lucro/EBIT insuficientes."
    
    aliquota_efetiva = abs(hist_impostos.sum()) / abs(hist_lai.sum())
    hist_nopat = hist_ebit * (1 - aliquota_efetiva)
    hist_dep_amort = obter_historico_metrica(empresa_dfc, C['DEPRECIACAO_AMORTIZACAO'])
    hist_fco = hist_nopat.add(hist_dep_amort, fill_value=0)
    
    try:
        contas_a_receber = obter_historico_metrica(empresa_bpa, C['CONTAS_A_RECEBER']).iloc[-1]
        estoques = obter_historico_metrica(empresa_bpa, C['ESTOQUES']).iloc[-1]
        fornecedores = obter_historico_metrica(empresa_bpp, C['FORNECEDORES']).iloc[-1]
        ativo_imobilizado = obter_historico_metrica(empresa_bpa, C['ATIVO_IMOBILIZADO']).iloc[-1]
        ativo_intangivel = obter_historico_metrica(empresa_bpa, C['ATIVO_INTANGIVEL']).iloc[-1]
        divida_cp = obter_historico_metrica(empresa_bpp, C['DIVIDA_CURTO_PRAZO']).iloc[-1]
        divida_lp = obter_historico_metrica(empresa_bpp, C['DIVIDA_LONGO_PRAZO']).iloc[-1]
        desp_financeira = abs(obter_historico_metrica(empresa_dre, C['DESPESAS_FINANCEIRAS']).iloc[-1])
    except IndexError:
        return None, "Dados de balanço patrimonial ausentes ou incompletos."
    
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
    kd = (desp_financeira / divida_total) if divida_total > 0 else 0.0
    kd_liquido = kd * (1 - aliquota_efetiva)
    
    beta = calcular_beta(ticker_sa, ibov_data, params['periodo_beta_ibov'])
    ke = risk_free_rate + beta * premio_risco_mercado
    ev_mercado = market_cap + divida_total
    wacc = ((market_cap / ev_mercado) * ke) + ((divida_total / ev_mercado) * kd_liquido) if ev_mercado > 0 else ke
    
    eva = (roic - wacc) * capital_empregado
    riqueza_atual = (eva / wacc) if wacc > 0 else 0.0
    riqueza_futura_esperada = ev_mercado - capital_empregado
    efv = riqueza_futura_esperada - riqueza_atual
    
    g = params['taxa_crescimento_perpetuidade']
    if wacc <= g or pd.isna(wacc):
        return None, "WACC inválido ou menor/igual à taxa de crescimento."
    
    valor_residual = (fco_medio * (1 + g)) / (wacc - g)
    equity_value = valor_residual - divida_total
    preco_justo = equity_value / n_acoes if n_acoes > 0 else 0
    margem_seguranca = (preco_justo / preco_atual) - 1 if preco_atual > 0 else 0
    
    return {
        'Empresa': nome_empresa,
        'Ticker': ticker_sa.replace('.SA', ''),
        'Preço Atual (R$)': preco_atual,
        'Preço Justo (R$)': preco_justo,
        'Margem Segurança (%)': margem_seguranca * 100,
        'Market Cap (R$)': market_cap,
        'Capital Empregado (R$)': capital_empregado,
        'Dívida Total (R$)': divida_total,
        'NOPAT Médio (R$)': nopat_medio,
        'ROIC (%)': roic * 100,
        'Beta': beta,
        'Custo do Capital (WACC %)': wacc * 100,
        'Spread (ROIC-WACC %)': (roic - wacc) * 100,
        'EVA (R$)': eva,
        'EFV (R$)': efv,
        'hist_nopat': hist_nopat,
        'hist_fco': hist_fco,
        'hist_roic': (hist_nopat / capital_empregado) * 100,
        'wacc_series': pd.Series([wacc * 100] * len(hist_nopat.index), index=hist_nopat.index)
    }, "Análise concluída com sucesso."

def executar_analise_completa(ticker_map, demonstrativos, market_data, params, progress_bar):
    """Executa análise completa de valuation."""
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
        except Exception:
            continue
    
    progress_bar.empty()
    return todos_os_resultados

@st.cache_data
def convert_df_to_csv(df):
    """Converte DataFrame para CSV."""
    return df.to_csv(index=False, decimal=',', sep=';', encoding='utf-8-sig').encode('utf-8-sig')

def exibir_rankings(df_final):
    """Exibe rankings de mercado."""
    st.subheader("🏆 Rankings de Mercado")
    if df_final.empty:
        st.warning("Nenhuma empresa pôde ser analisada com sucesso para gerar os rankings.")
        return
    
    rankings = {
        "MARGEM_SEGURANCA": (
            "Ranking por Margem de Segurança", 
            'Margem Segurança (%)', 
            ['Ticker', 'Empresa', 'Preço Atual (R$)', 'Preço Justo (R$)', 'Margem Segurança (%)']
        ),
        "ROIC": (
            "Ranking por ROIC", 
            'ROIC (%)', 
            ['Ticker', 'Empresa', 'ROIC (%)', 'Spread (ROIC-WACC %)']
        ),
        "EVA": (
            "Ranking por EVA", 
            'EVA (R$)', 
            ['Ticker', 'Empresa', 'EVA (R$)']
        ),
        "EFV": (
            "Ranking por EFV", 
            'EFV (R$)', 
            ['Ticker', 'Empresa', 'EFV (R$)']
        )
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
            st.download_button(
                label=f"📥 Baixar Ranking Completo (.csv)",
                data=csv,
                file_name=f'ranking_{nome_ranking.lower()}.csv',
                mime='text/csv',
            )

def ui_valuation():
    """Interface de valuation e análise de ações."""
    st.header("📈 Análise de Valuation e Scanner de Mercado")
    
    tab_individual, tab_ranking = st.tabs([
        "📊 Análise Individual", 
        "🔍 Scanner de Mercado"
    ])
    
    ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
    if ticker_cvm_map_df.empty:
        st.error("Não foi possível carregar o mapeamento de tickers.")
        st.stop()
    
    with tab_individual:
        with st.form(key='individual_analysis_form'):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                lista_tickers = sorted(ticker_cvm_map_df['TICKER'].unique())
                ticker_selecionado = st.selectbox(
                    "Selecione o Ticker da Empresa", 
                    options=lista_tickers, 
                    index=lista_tickers.index('PETR4') if 'PETR4' in lista_tickers else 0
                )
            
            with col2:
                analisar_btn = st.form_submit_button(
                    "🔍 Analisar Empresa", 
                    type="primary", 
                    use_container_width=True
                )
        
        with st.expander("⚙️ Opções Avançadas de Valuation", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                p_taxa_cresc = st.slider(
                    "Taxa de Crescimento Perpetuidade (%)", 
                    0.0, 10.0, 
                    CONFIG["TAXA_CRESCIMENTO_PERPETUIDADE"] * 100, 
                    0.5
                ) / 100
            
            with col2:
                p_media_anos = st.number_input(
                    "Anos para Média NOPAT/FCO", 
                    1, CONFIG["HISTORICO_ANOS_CVM"], 
                    CONFIG["MEDIA_ANOS_CALCULO"]
                )
            
            with col3:
                p_periodo_beta = st.selectbox(
                    "Período para Cálculo do Beta", 
                    options=["1y", "2y", "5y", "10y"], 
                    index=2
                )
        
        if analisar_btn:
            demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
            market_data = obter_dados_mercado(p_periodo_beta)
            
            ticker_sa = f"{ticker_selecionado}.SA"
            codigo_cvm_info = ticker_cvm_map_df[ticker_cvm_map_df['TICKER'] == ticker_selecionado]
            codigo_cvm = int(codigo_cvm_info.iloc[0]['CD_CVM'])
            
            params_analise = {
                'taxa_crescimento_perpetuidade': p_taxa_cresc,
                'media_anos_calculo': p_media_anos,
                'periodo_beta_ibov': p_periodo_beta
            }
            
            with st.spinner(f"🔄 Analisando {ticker_selecionado}..."):
                resultados, status_msg = processar_valuation_empresa(
                    ticker_sa, codigo_cvm, demonstrativos, market_data, params_analise
                )
            
            if resultados:
                st.success(f"✅ Análise para **{resultados['Empresa']} ({resultados['Ticker']})** concluída!")
                
                # Métricas principais
                col1, col2, col3, col4 = st.columns(4)
                
                col1.metric(
                    "💰 Preço Atual", 
                    f"R$ {resultados['Preço Atual (R$)']:.2f}"
                )
                
                col2.metric(
                    "🎯 Preço Justo (DCF)", 
                    f"R$ {resultados['Preço Justo (R$)']:.2f}"
                )
                
                ms_delta = resultados['Margem Segurança (%)']
                col3.metric(
                    "🛡️ Margem de Segurança", 
                    f"{ms_delta:.2f}%", 
                    delta=f"{ms_delta:.2f}%" if not pd.isna(ms_delta) else None
                )
                
                col4.metric(
                    "📊 ROIC", 
                    f"{resultados['ROIC (%)']:.2f}%"
                )
                
                st.divider()
                
                # Gráficos e dados detalhados
                tab_g, tab_d = st.tabs(["📊 Gráficos", "🔢 Dados Detalhados"])
                
                with tab_g:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Gráfico NOPAT
                        fig_nopat = px.bar(
                            x=resultados['hist_nopat'].index,
                            y=resultados['hist_nopat'].values,
                            title='📈 Histórico de NOPAT',
                            template="plotly_dark"
                        )
                        fig_nopat.update_layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font_color='#F0F6FC'
                        )
                        st.plotly_chart(fig_nopat, use_container_width=True)
                    
                    with col2:
                        # Gráfico FCO
                        fig_fco = px.bar(
                            x=resultados['hist_fco'].index,
                            y=resultados['hist_fco'].values,
                            title='💰 Histórico de FCO',
                            template="plotly_dark"
                        )
                        fig_fco.update_layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font_color='#F0F6FC'
                        )
                        st.plotly_chart(fig_fco, use_container_width=True)
                
                with tab_d:
                    # Tabela de dados
                    df_display = pd.DataFrame.from_dict(
                        {k: v for k, v in resultados.items() 
                         if k not in ['hist_nopat', 'hist_fco', 'hist_roic', 'wacc_series']},
                        orient='index',
                        columns=['Valor']
                    )
                    st.dataframe(df_display, use_container_width=True)
            else:
                st.error(f"❌ Não foi possível analisar {ticker_selecionado}. Motivo: {status_msg}")

    with tab_ranking:
        st.info("🔄 Esta análise processa todas as empresas da lista, o que pode levar vários minutos.")
        
        if st.button("🚀 Iniciar Análise Completa e Gerar Rankings", type="primary", use_container_width=True):
            params_ranking = {
                'taxa_crescimento_perpetuidade': CONFIG["TAXA_CRESCIMENTO_PERPETUIDADE"],
                'media_anos_calculo': CONFIG["MEDIA_ANOS_CALCULO"],
                'periodo_beta_ibov': CONFIG["PERIODO_BETA_IBOV"]
            }
            
            demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
            market_data = obter_dados_mercado(params_ranking['periodo_beta_ibov'])
            
            progress_bar = st.progress(0, text="🔄 Iniciando análise em lote...")
            
            resultados_completos = executar_analise_completa(
                ticker_cvm_map_df, demonstrativos, market_data, params_ranking, progress_bar
            )
            
            if resultados_completos:
                df_final = pd.DataFrame(resultados_completos)
                st.success(f"✅ Análise completa! {len(df_final)} de {len(ticker_cvm_map_df)} empresas foram processadas com sucesso.")
                exibir_rankings(df_final)
            else:
                st.error("❌ A análise em lote não retornou nenhum resultado válido.")

# ==============================================================================
# MODELO FLEURIET (MANTIDO DO ORIGINAL COM MELHORIAS VISUAIS)
# ==============================================================================

def reclassificar_contas_fleuriet(df_bpa, df_bpp, contas_cvm):
    """Reclassifica contas para o modelo Fleuriet."""
    aco = obter_historico_metrica(df_bpa, contas_cvm['ESTOQUES']).add(
        obter_historico_metrica(df_bpa, contas_cvm['CONTAS_A_RECEBER']), 
        fill_value=0
    )
    pco = obter_historico_metrica(df_bpp, contas_cvm['FORNECEDORES'])
    ap = obter_historico_metrica(df_bpa, contas_cvm['ATIVO_NAO_CIRCULANTE'])
    pl = obter_historico_metrica(df_bpp, contas_cvm['PATRIMONIO_LIQUIDO'])
    pnc = obter_historico_metrica(df_bpp, contas_cvm['PASSIVO_NAO_CIRCULANTE'])
    
    return aco, pco, ap, pl, pnc

def processar_analise_fleuriet(ticker_sa, codigo_cvm, demonstrativos):
    """Processa análise Fleuriet para uma empresa."""
    C = CONFIG['CONTAS_CVM']
    
    bpa = demonstrativos['bpa'][demonstrativos['bpa']['CD_CVM'] == codigo_cvm]
    bpp = demonstrativos['bpp'][demonstrativos['bpp']['CD_CVM'] == codigo_cvm]
    dre = demonstrativos['dre'][demonstrativos['dre']['CD_CVM'] == codigo_cvm]
    
    if any(df.empty for df in [bpa, bpp, dre]):
        return None
    
    aco, pco, ap, pl, pnc = reclassificar_contas_fleuriet(bpa, bpp, C)
    
    if any(s.empty for s in [aco, pco, ap, pl, pnc]):
        return None

    ncg = aco.subtract(pco, fill_value=0)
    cdg = pl.add(pnc, fill_value=0).subtract(ap, fill_value=0)
    t = cdg.subtract(ncg, fill_value=0)
    
    # Análise do efeito tesoura
    efeito_tesoura = False
    if len(ncg) > 1 and len(cdg) > 1:
        cresc_ncg = ncg.pct_change().iloc[-1]
        cresc_cdg = cdg.pct_change().iloc[-1]
        if pd.notna(cresc_ncg) and pd.notna(cresc_cdg) and cresc_ncg > cresc_cdg and t.iloc[-1] < 0:
            efeito_tesoura = True
            
    try:
        info = yf.Ticker(ticker_sa).info
        market_cap = info.get('marketCap', 0)
        
        ativo_total = obter_historico_metrica(bpa, C['ATIVO_TOTAL']).iloc[-1]
        passivo_total = obter_historico_metrica(bpp, C['PASSIVO_TOTAL']).iloc[-1]
        lucro_retido = pl.iloc[-1] - pl.iloc[0]
        ebit = obter_historico_metrica(dre, C['EBIT']).iloc[-1]
        vendas = obter_historico_metrica(dre, C['RECEITA_LIQUIDA']).iloc[-1]
        
        # Z-Score de Prado
        X1 = cdg.iloc[-1] / ativo_total
        X2 = lucro_retido / ativo_total
        X3 = ebit / ativo_total
        X4 = market_cap / passivo_total if passivo_total > 0 else 0
        X5 = vendas / ativo_total
        
        z_score = 0.038*X1 + 1.253*X2 + 2.331*X3 + 0.511*X4 + 0.824*X5
        
        if z_score < 1.81:
            classificacao = "Risco Elevado"
        elif z_score < 2.99:
            classificacao = "Zona Cinzenta"
        else:
            classificacao = "Saudável"
            
    except Exception:
        z_score, classificacao = None, "Erro no cálculo"

    return {
        'Ticker': ticker_sa.replace('.SA', ''),
        'Empresa': info.get('longName', ticker_sa) if 'info' in locals() else ticker_sa,
        'Ano': t.index[-1],
        'NCG': ncg.iloc[-1],
        'CDG': cdg.iloc[-1],
        'Tesouraria': t.iloc[-1],
        'Efeito Tesoura': efeito_tesoura,
        'Z-Score': z_score,
        'Classificação Risco': classificacao
    }

def ui_modelo_fleuriet():
    """Interface do modelo Fleuriet."""
    st.header("🔬 Análise de Saúde Financeira (Modelo Fleuriet & Z-Score)")
    
    st.info("📊 Esta análise utiliza os dados da CVM para avaliar a estrutura de capital de giro e o risco de insolvência das empresas.")
    
    if st.button("🚀 Iniciar Análise Fleuriet Completa", type="primary", use_container_width=True):
        ticker_cvm_map_df = carregar_mapeamento_ticker_cvm()
        demonstrativos = preparar_dados_cvm(CONFIG["HISTORICO_ANOS_CVM"])
        
        resultados_fleuriet = []
        progress_bar = st.progress(0, text="🔄 Iniciando análise Fleuriet...")
        
        total_empresas = len(ticker_cvm_map_df)
        
        for i, (index, row) in enumerate(ticker_cvm_map_df.iterrows()):
            ticker = row['TICKER']
            progress_bar.progress(
                (i + 1) / total_empresas, 
                text=f"Analisando {i+1}/{total_empresas}: {ticker}"
            )
            
            resultado = processar_analise_fleuriet(
                f"{ticker}.SA", 
                int(row['CD_CVM']), 
                demonstrativos
            )
            
            if resultado:
                resultados_fleuriet.append(resultado)
        
        progress_bar.empty()
        
        if resultados_fleuriet:
            df_fleuriet = pd.DataFrame(resultados_fleuriet)
            st.success(f"✅ Análise Fleuriet concluída para {len(df_fleuriet)} empresas.")
            
            # Métricas resumo
            ncg_medio = df_fleuriet['NCG'].mean()
            tesoura_count = df_fleuriet['Efeito Tesoura'].sum()
            risco_count = len(df_fleuriet[df_fleuriet['Classificação Risco'] == "Risco Elevado"])
            zscore_medio = df_fleuriet['Z-Score'].mean()
            
            col1, col2, col3, col4 = st.columns(4)
            
            col1.metric("📊 NCG Média", f"R$ {ncg_medio/1e6:.1f} M")
            col2.metric("⚠️ Efeito Tesoura", f"{tesoura_count} empresas")
            col3.metric("🚨 Alto Risco", f"{risco_count} empresas")
            col4.metric("📈 Z-Score Médio", f"{zscore_medio:.2f}")
            
            # Tabela de resultados
            st.dataframe(df_fleuriet, use_container_width=True)
            
            # Download dos resultados
            csv_fleuriet = convert_df_to_csv(df_fleuriet)
            st.download_button(
                label="📥 Baixar Análise Fleuriet (.csv)",
                data=csv_fleuriet,
                file_name=f'analise_fleuriet_{datetime.now().strftime("%Y%m%d")}.csv',
                mime='text/csv'
            )
        else:
            st.error("❌ Nenhum resultado pôde ser gerado para a análise Fleuriet.")
    
    with st.expander("📖 Metodologia do Modelo Fleuriet", expanded=False):
        st.markdown("""
        ### 🔍 Indicadores do Modelo Fleuriet
        
        - **NCG (Necessidade de Capital de Giro):** `(Estoques + Contas a Receber) - Fornecedores`
        - **CDG (Capital de Giro):** `(Patrimônio Líquido + Passivo Longo Prazo) - Ativo Permanente`
        - **T (Saldo de Tesouraria):** `CDG - NCG`
        - **Efeito Tesoura:** Ocorre quando a NCG cresce mais rapidamente que o CDG
        
        ### 📊 Z-Score de Prado
        
        Modelo estatístico que mede a probabilidade de uma empresa ir à falência:
        - **< 1.81:** Risco Elevado
        - **1.81 - 2.99:** Zona Cinzenta  
        - **> 2.99:** Empresa Saudável
        """)

# ==============================================================================
# ESTRUTURA PRINCIPAL DO APP
# ==============================================================================

def main():
    """Função principal do aplicativo."""
    # Configurar diretórios
    setup_diretorios()
    
    # Inicializar estado da sessão
    inicializar_session_state()
    
    # Título principal
    st.title("🏦 Sistema Integrado de Controle Financeiro e Análise de Investimentos")
    
    # Menu principal em abas
    tab1, tab2, tab3 = st.tabs([
        "💰 Controle Financeiro", 
        "📈 Análise de Valuation", 
        "🔬 Modelo Fleuriet"
    ])
    
    with tab1:
        ui_controle_financeiro()
    
    with tab2:
        ui_valuation()
    
    with tab3:
        ui_modelo_fleuriet()
    
    # Footer com informações
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: var(--text-secondary); padding: 20px;'>"
        "🚀 Sistema desenvolvido para controle financeiro pessoal e análise de investimentos<br>"
        "📊 Dados da CVM | 📈 Preços do Yahoo Finance | 🏦 Taxas do Banco Central"
        "</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()