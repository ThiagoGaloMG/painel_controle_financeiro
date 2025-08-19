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
import concurrent.futures
from scipy.stats import norm
from typing import Dict, List, Optional
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ignorar avisos para uma saída mais limpa
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURAÇÕES GERAIS E LAYOUT DA PÁGINA
# ==============================================================================
st.set_page_config(
    page_title="Painel de Controle Financeiro",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
    :root {
        --primary: #1f77b4;
        --background: #0e1117;
        --secondary: #2a2f3d;
        --text-primary: #f0f6fc;
        --text-secondary: #8b949e;
        --success: #2ecc71;
        --warning: #f39c12;
        --danger: #e74c3c;
        --widget-bg: rgba(255, 255, 255, 0.05);
    }
    
    body {
        background-color: var(--background);
        color: var(--text-primary);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
        background-attachment: fixed;
    }
    
    .st-bb {
        background-color: var(--widget-bg);
    }
    
    .st-at {
        background-color: var(--secondary);
    }
    
    .st-ax {
        color: var(--text-primary);
    }
    
    .stButton>button {
        background-color: var(--primary);
        color: white;
        border-radius: 8px;
        padding: 8px 16px;
        transition: all 0.3s;
    }
    
    .stButton>button:hover {
        background-color: #1669a2;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    }
    
    .stTextInput>div>div>input {
        background-color: var(--widget-bg);
        color: var(--text-primary);
        border-radius: 8px;
    }
    
    .stNumberInput>div>div>input {
        background-color: var(--widget-bg);
        color: var(--text-primary);
        border-radius: 8px;
    }
    
    .stSelectbox>div>div>div {
        background-color: var(--widget-bg);
        color: var(--text-primary);
        border-radius: 8px;
    }
    
    .stDateInput>div>div>input {
        background-color: var(--widget-bg);
        color: var(--text-primary);
        border-radius: 8px;
    }
    
    .stAlert {
        border-radius: 12px;
        padding: 16px;
    }
    
    .stProgress>div>div>div {
        background-color: var(--success);
    }
    
    .stExpander {
        background-color: var(--widget-bg);
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 16px;
    }
    
    .stTab {
        background-color: transparent !important;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: var(--widget-bg) !important;
        border-radius: 8px !important;
        padding: 10px 20px !important;
        transition: all 0.3s !important;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: var(--primary) !important;
        color: white !important;
        font-weight: bold;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    }
    
    .metric-box {
        background-color: var(--widget-bg);
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        transition: transform 0.3s;
    }
    
    .metric-box:hover {
        transform: translateY(-5px);
    }
    
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        margin: 10px 0;
    }
    
    .metric-title {
        font-size: 0.9rem;
        color: var(--text-secondary);
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# SISTEMA DE PERSISTÊNCIA APRIMORADO
# ==============================================================================
DATA_DIR = Path("data")
TRANSACTIONS_FILE = DATA_DIR / "transactions.csv"
GOALS_FILE = DATA_DIR / "goals.json"

def setup_diretorios():
    """Cria diretórios necessários se não existirem."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not TRANSACTIONS_FILE.exists():
        pd.DataFrame(columns=["ID", "Data", "Categoria", "Descrição", "Valor", "Tipo"]).to_csv(TRANSACTIONS_FILE, index=False)
    if not GOALS_FILE.exists():
        with open(GOALS_FILE, 'w') as f:
            f.write('{}')

def carregar_transacoes():
    """Carrega transações do arquivo CSV."""
    if TRANSACTIONS_FILE.exists():
        df = pd.read_csv(TRANSACTIONS_FILE, parse_dates=["Data"])
        # Garantir conversão numérica para a coluna Valor
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0)
        return df
    return pd.DataFrame(columns=["ID", "Data", "Categoria", "Descrição", "Valor", "Tipo"])

def salvar_transacoes(df):
    """Salva transações no arquivo CSV."""
    df.to_csv(TRANSACTIONS_FILE, index=False)

def carregar_metas():
    """Carrega metas do arquivo JSON."""
    if GOALS_FILE.exists():
        try:
            with open(GOALS_FILE, 'r') as f:
                return pd.read_json(f).to_dict()
        except:
            return {}
    return {}

def salvar_metas(metas):
    """Salva metas no arquivo JSON."""
    with open(GOALS_FILE, 'w') as f:
        pd.DataFrame(metas).to_json(f)

def inicializar_session_state():
    """Inicializa o estado da sessão com estruturas de dados mais robustas."""
    if 'transactions' not in st.session_state:
        st.session_state.transactions = carregar_transacoes()
    
    if 'goals' not in st.session_state:
        st.session_state.goals = carregar_metas() or {
            'Reserva de Emergência': {'meta': 20000, 'atual': 0},
            'Comprar um Carro': {'meta': 50000, 'atual': 0},
            'Viagem Internacional': {'meta': 15000, 'atual': 0}
        }
    
    if 'categorias' not in st.session_state:
        st.session_state.categorias = {
            'Receita': ['Salário', 'Freelance', 'Investimentos', 'Outros'],
            'Despesa': ['Moradia', 'Alimentação', 'Transporte', 'Lazer', 'Saúde', 'Educação', 'Outros'],
            'Investimento': ['Ações', 'Fundos Imobiliários', 'Renda Fixa', 'Criptomoedas', 'Outros']
        }
    
    # Novo: Adicionar histórico de métricas
    if 'metric_history' not in st.session_state:
        st.session_state.metric_history = pd.DataFrame(columns=[
            'Date', 'TotalReceitas', 'TotalDespesas', 'TotalInvestido', 
            'PatrimonioLiquido', 'TaxaPoupanca'
        ])
    
    # Novo: Adicionar projeções financeiras
    if 'projections' not in st.session_state:
        st.session_state.projections = {
            'scenarios': {}
        }

def adicionar_transacao(data, categoria, descricao, valor, tipo):
    """Adiciona uma nova transação ao DataFrame."""
    novo_id = str(uuid.uuid4())
    nova_transacao = pd.DataFrame([{
        "ID": novo_id,
        "Data": data,
        "Categoria": categoria,
        "Descrição": descricao,
        "Valor": valor,
        "Tipo": tipo
    }])
    
    st.session_state.transactions = pd.concat(
        [st.session_state.transactions, nova_transacao], 
        ignore_index=True
    )
    salvar_transacoes(st.session_state.transactions)
    return novo_id

def atualizar_metas():
    """Atualiza o progresso das metas com base nas transações."""
    df = st.session_state.transactions
    
    # Atualizar metas apenas para investimentos
    for meta in st.session_state.goals:
        st.session_state.goals[meta]['atual'] = 0
    
    # Calcular valor total investido
    investimentos = df[df['Tipo'] == 'Investimento']['Valor'].sum()
    
    # Distribuir o valor total investido proporcionalmente entre as metas
    total_metas = sum(st.session_state.goals[meta]['meta'] for meta in st.session_state.goals)
    for meta in st.session_state.goals:
        proporcao = st.session_state.goals[meta]['meta'] / total_metas
        st.session_state.goals[meta]['atual'] = proporcao * investimentos
    
    salvar_metas(st.session_state.goals)

def calcular_metricas_financeiras():
    """Calcula métricas financeiras básicas."""
    df = st.session_state.transactions
    metricas = {
        'total_receitas': 0,
        'total_despesas': 0,
        'total_investido': 0,
        'patrimonio_liquido': 0,
        'taxa_poupanca': 0
    }
    
    if not df.empty:
        # Garantir que Valor é numérico
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0)
        
        # Calcular totais
        metricas['total_receitas'] = df[df['Tipo'] == 'Receita']['Valor'].sum()
        metricas['total_despesas'] = df[df['Tipo'] == 'Despesa']['Valor'].sum()
        metricas['total_investido'] = df[df['Tipo'] == 'Investimento']['Valor'].sum()
        
        # Calcular patrimônio líquido
        metricas['patrimonio_liquido'] = (
            metricas['total_receitas'] - 
            metricas['total_despesas'] - 
            metricas['total_investido']
        )
        
        # Calcular taxa de poupança
        if metricas['total_receitas'] > 0:
            metricas['taxa_poupanca'] = (
                (metricas['total_investido'] / metricas['total_receitas']) * 100
            )
    
    return metricas

# NOVO: Cálculo de métricas com cache
@st.cache_data(ttl=300)
def calcular_metricas_financeiras_cached(transactions):
    """Calcula métricas financeiras com cache para melhor performance."""
    return calcular_metricas_financeiras()

# ==============================================================================
# NOVO: SISTEMA DE PROJEÇÕES FINANCEIRAS
# ==============================================================================

def simular_cenarios_financeiros():
    """Simula diferentes cenários financeiros usando Monte Carlo."""
    df_trans = st.session_state.transactions.copy()
    if df_trans.empty:
        return {}
    
    # Garantir conversão numérica
    df_trans['Valor'] = pd.to_numeric(df_trans['Valor'], errors='coerce').fillna(0)
    
    # Agrupar dados mensais
    df_trans['Data'] = pd.to_datetime(df_trans['Data'])
    df_monthly = df_trans.set_index('Data').resample('M').agg({
        'Valor': lambda x: x[df_trans['Tipo'] == 'Receita'].sum() - 
                          x[df_trans['Tipo'] == 'Despesa'].sum() - 
                          x[df_trans['Tipo'] == 'Investimento'].sum()
    })
    df_monthly['Saldo'] = df_monthly['Valor'].cumsum()
    
    # Calcular estatísticas
    mean_return = df_monthly['Valor'].mean()
    std_dev = df_monthly['Valor'].std()
    
    if pd.isna(mean_return) or pd.isna(std_dev) or std_dev == 0:
        return {}
    
    # Configurar simulação
    num_simulations = 100
    num_months = 60  # 5 anos
    results = {}
    
    # Executar simulações em paralelo
    def run_simulation(i):
        np.random.seed(i)
        simulated = [df_monthly['Saldo'].iloc[-1]]
        for _ in range(num_months):
            ret = np.random.normal(mean_return, std_dev)
            simulated.append(simulated[-1] + ret)
        return simulated
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(run_simulation, i) for i in range(num_simulations)]
        results = [f.result() for f in futures]
    
    # Calcular estatísticas das simulações
    df_sim = pd.DataFrame(results).T
    df_sim['Média'] = df_sim.mean(axis=1)
    df_sim['Percentil_5'] = df_sim.apply(lambda x: np.percentile(x, 5), axis=1)
    df_sim['Percentil_95'] = df_sim.apply(lambda x: np.percentile(x, 95), axis=1)
    
    return df_sim

def analisar_saude_financeira():
    """Analisa a saúde financeira com base em múltiplos indicadores."""
    metricas = calcular_metricas_financeiras()
    
    # Calcular scores
    score = 0
    
    # 1. Taxa de poupança (20%)
    taxa_poupanca = metricas['taxa_poupanca']
    if taxa_poupanca >= 20:
        score += 20
    elif taxa_poupanca >= 10:
        score += 15
    elif taxa_poupanca >= 5:
        score += 10
    else:
        score += 5
    
    # 2. Reserva de emergência (30%)
    reserva_meta = st.session_state.goals['Reserva de Emergência']['meta']
    reserva_atual = st.session_state.goals['Reserva de Emergência']['atual']
    if reserva_atual >= reserva_meta:
        score += 30
    elif reserva_atual >= reserva_meta * 0.7:
        score += 20
    elif reserva_atual >= reserva_meta * 0.5:
        score += 15
    else:
        score += 5
    
    # 3. Endividamento (20%)
    total_despesas = metricas['total_despesas']
    total_receitas = metricas['total_receitas']
    if total_receitas > 0:
        razao_endividamento = total_despesas / total_receitas
        if razao_endividamento <= 0.3:
            score += 20
        elif razao_endividamento <= 0.5:
            score += 15
        elif razao_endividamento <= 0.7:
            score += 10
        else:
            score += 5
    
    # 4. Diversificação (15%)
    total_investido = metricas['total_investido']
    if total_investido > 0:
        # Suposição: investimentos produtivos são ações e FIIs
        invest_produtivos = st.session_state.transactions[
            (st.session_state.transactions['Tipo'] == 'Investimento') &
            (st.session_state.transactions['Categoria'].isin(['Ações', 'Fundos Imobiliários']))
        ]['Valor'].sum()
        
        razao_produtivos = invest_produtivos / total_investido
        if razao_produtivos >= 0.7:
            score += 15
        elif razao_produtivos >= 0.5:
            score += 10
        elif razao_produtivos >= 0.3:
            score += 7
        else:
            score += 3
    
    # 5. Progresso de metas (15%)
    progresso_total = 0
    for meta, dados in st.session_state.goals.items():
        if dados['meta'] > 0:
            progresso = min(dados['atual'] / dados['meta'], 1.0)
            progresso_total += progresso
    
    progresso_medio = progresso_total / len(st.session_state.goals)
    score += progresso_medio * 15
    
    # Classificação
    if score >= 90:
        status = "Excelente"
        cor = "#2ecc71"
    elif score >= 75:
        status = "Boa"
        cor = "#27ae60"
    elif score >= 60:
        status = "Regular"
        cor = "#f39c12"
    else:
        status = "Preocupante"
        cor = "#e74c3c"
    
    return score, status, cor

# ==============================================================================
# COMPONENTES DE UI APRIMORADOS
# ==============================================================================

def criar_dashboard_saude_financeira():
    """Cria dashboard interativo de saúde financeira."""
    score, status, cor = analisar_saude_financeira()
    
    st.subheader("❤️ Saúde Financeira")
    
    # Gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Score de Saúde Financeira"},
        gauge={
            'axis': {'range': [None, 100]},
            'bar': {'color': cor},
            'steps': [
                {'range': [0, 60], 'color': "#e74c3c"},
                {'range': [60, 75], 'color': "#f39c12"},
                {'range': [75, 90], 'color': "#27ae60"},
                {'range': [90, 100], 'color': "#2ecc71"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 4},
                'thickness': 0.75,
                'value': score
            }
        }
    ))
    
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='#F0F6FC'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Detalhamento
    st.markdown(f"""
    <div style="
        background: var(--widget-bg);
        border-radius: 12px;
        padding: 16px;
        margin-top: 20px;
        border-left: 4px solid {cor};
    ">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h3 style="color: {cor}; margin: 0;">Status: {status}</h3>
            <span style="font-size: 1.5rem; font-weight: 700; color: {cor};">{score:.1f}/100</span>
        </div>
        <div style="margin-top: 15px;">
            <p>Recomendações para melhorar sua saúde financeira:</p>
            <ul>
                <li>Aumente sua taxa de poupança para pelo menos 20%</li>
                <li>Complete sua reserva de emergência</li>
                <li>Diversifique seus investimentos</li>
                <li>Monitore seus orçamentos mensais</li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)

def criar_projecao_patrimonial():
    """Cria projeção patrimonial com simulação de Monte Carlo."""
    st.subheader("🔮 Projeção Patrimonial")
    
    with st.expander("Configurar Projeção", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            anos_projecao = st.slider("Anos de Projeção", 1, 30, 10)
        with col2:
            num_simulacoes = st.slider("Número de Simulações", 10, 500, 100)
    
    # Botão para gerar projeções
    if st.button("🔄 Gerar Projeções", type="primary"):
        with st.spinner("Simulando cenários futuros..."):
            df_sim = simular_cenarios_financeiros()
            
            if not df_sim.empty:
                st.session_state.projections['monte_carlo'] = df_sim
                st.success("Projeções geradas com sucesso!")
            else:
                st.error("Dados insuficientes para gerar projeções")
    
    # Exibir resultados se disponíveis
    if 'monte_carlo' in st.session_state.projections:
        df_sim = st.session_state.projections['monte_carlo']
        
        # Gráfico de projeções
        fig = go.Figure()
        
        # Adicionar percentis
        fig.add_trace(go.Scatter(
            x=df_sim.index,
            y=df_sim['Percentil_5'],
            fill=None,
            mode='lines',
            line_color='rgba(46, 204, 113, 0.2)',
            name='Cenário Conservador (5º percentil)'
        ))
        
        fig.add_trace(go.Scatter(
            x=df_sim.index,
            y=df_sim['Percentil_95'],
            fill='tonexty',
            mode='lines',
            line_color='rgba(46, 204, 113, 0.2)',
            name='Cenário Otimista (95º percentil)'
        ))
        
        fig.add_trace(go.Scatter(
            x=df_sim.index,
            y=df_sim['Média'],
            mode='lines',
            line=dict(color='#2ecc71', width=3),
            name='Cenário Esperado'
        ))
        
        # Configurar layout
        meses = df_sim.index
        dates = [datetime.now() + timedelta(days=30*i) for i in range(len(meses))]
        
        fig.update_layout(
            title='Projeção Patrimonial com Simulação de Monte Carlo',
            xaxis_title='Tempo',
            yaxis_title='Patrimônio Líquido (R$)',
            template='plotly_dark',
            hovermode='x',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#F0F6FC',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Estatísticas resumidas
        st.subheader("📊 Resumo das Projeções")
        
        col1, col2, col3 = st.columns(3)
        col1.metric(
            "Patrimônio em 1 ano", 
            f"R$ {df_sim['Média'].iloc[12]:,.0f}",
            delta=f"R$ {df_sim['Média'].iloc[12] - df_sim['Média'].iloc[0]:,.0f}"
        )
        
        col2.metric(
            "Patrimônio em 5 anos", 
            f"R$ {df_sim['Média'].iloc[60]:,.0f}",
            delta=f"R$ {df_sim['Média'].iloc[60] - df_sim['Média'].iloc[0]:,.0f}"
        )
        
        col3.metric(
            "Crescimento Anual Esperado", 
            f"{((df_sim['Média'].iloc[60]/df_sim['Média'].iloc[0])**(1/5)-1)*100:.1f}%"
        )
    else:
        st.info("Clique em 'Gerar Projeções' para simular cenários futuros")

# ==============================================================================
# ABA DE CONTROLE FINANCEIRO (CORRIGIDA)
# ==============================================================================

def ui_controle_financeiro():
    """Interface do usuário para controle financeiro."""
    st.header("💰 Controle Financeiro Pessoal")
    
    # Atualizar métricas
    metricas = calcular_metricas_financeiras()
    
    # Dashboard de métricas
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("""
        <div class="metric-box">
            <div class="metric-title">Receitas Totais</div>
            <div class="metric-value">R$ {total_receitas:,.2f}</div>
            <div class="metric-title">Último mês</div>
        </div>
        """.format(total_receitas=metricas['total_receitas']), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-box">
            <div class="metric-title">Despesas Totais</div>
            <div class="metric-value">R$ {total_despesas:,.2f}</div>
            <div class="metric-title">Último mês</div>
        </div>
        """.format(total_despesas=metricas['total_despesas']), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-box">
            <div class="metric-title">Patrimônio Líquido</div>
            <div class="metric-value">R$ {patrimonio:,.2f}</div>
            <div class="metric-title">Atual</div>
        </div>
        """.format(patrimonio=metricas['patrimonio_liquido']), unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="metric-box">
            <div class="metric-title">Taxa de Poupança</div>
            <div class="metric-value">{taxa_poupanca:.1f}%</div>
            <div class="metric-title">Média mensal</div>
        </div>
        """.format(taxa_poupanca=metricas['taxa_poupanca']), unsafe_allow_html=True)
    
    # Divisão em colunas
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Formulário para adicionar nova transação
        with st.expander("➕ Adicionar Nova Transação", expanded=True):
            with st.form("nova_transacao"):
                data = st.date_input("Data", value=datetime.today())
                tipo = st.selectbox("Tipo", ["Receita", "Despesa", "Investimento"])
                
                # Categorias dinâmicas baseadas no tipo
                categoria = st.selectbox(
                    "Categoria", 
                    st.session_state.categorias[tipo]
                )
                
                descricao = st.text_input("Descrição")
                valor = st.number_input("Valor (R$)", value=0.0, min_value=0.0, step=100.0)
                
                if st.form_submit_button("Adicionar Transação"):
                    adicionar_transacao(data, categoria, descricao, valor, tipo)
                    st.success("Transação adicionada com sucesso!")
                    atualizar_metas()
    
    with col2:
        # Visualização de metas
        with st.expander("🎯 Metas Financeiras", expanded=True):
            meta_selecionada = st.selectbox(
                "Selecione a meta para definir/alteração", 
                list(st.session_state.goals.keys())
            )
            
            # Usar 0.0 como valor padrão (correção aplicada)
            novo_valor_meta = st.number_input(
                "Definir Valor Alvo (R$)", 
                value=st.session_state.goals[meta_selecionada]['meta'] * 1.0
            )
            
            if st.button("Atualizar Meta"):
                st.session_state.goals[meta_selecionada]['meta'] = novo_valor_meta
                salvar_metas(st.session_state.goals)
                st.success("Meta atualizada com sucesso!")
            
            st.divider()
            
            # Exibir progresso das metas
            for meta, dados in st.session_state.goals.items():
                progresso = min(dados['atual'] / dados['meta'], 1.0) if dados['meta'] > 0 else 0
                st.write(f"**{meta}**")
                st.progress(progresso)
                st.caption(f"R$ {dados['atual']:,.2f} de R$ {dados['meta']:,.2f} ({progresso*100:.1f}%)")
    
    # Visualização de transações
    st.subheader("📜 Histórico de Transações")
    
    # Filtros
    col1, col2, col3 = st.columns(3)
    with col1:
        filtro_tipo = st.selectbox("Filtrar por Tipo", ["Todos", "Receita", "Despesa", "Investimento"])
    with col2:
        filtro_categoria = st.selectbox("Filtrar por Categoria", ["Todas"] + list(set(
            [cat for sublist in st.session_state.categorias.values() for cat in sublist]
        )))
    with col3:
        filtro_periodo = st.selectbox("Filtrar por Período", ["Últimos 30 dias", "Últimos 90 dias", "Último ano", "Todos"])
    
    # Aplicar filtros
    df_filtrado = st.session_state.transactions.copy()
    
    if filtro_tipo != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Tipo'] == filtro_tipo]
    
    if filtro_categoria != "Todas":
        df_filtrado = df_filtrado[df_filtrado['Categoria'] == filtro_categoria]
    
    if filtro_periodo != "Todos":
        hoje = datetime.today()
        if filtro_periodo == "Últimos 30 dias":
            df_filtrado = df_filtrado[df_filtrado['Data'] >= (hoje - timedelta(days=30))]
        elif filtro_periodo == "Últimos 90 dias":
            df_filtrado = df_filtrado[df_filtrado['Data'] >= (hoje - timedelta(days=90))]
        elif filtro_periodo == "Último ano":
            df_filtrado = df_filtrado[df_filtrado['Data'] >= (hoje - timedelta(days=365))]
    
    # Exibir tabela
    st.dataframe(
        df_filtrado.sort_values('Data', ascending=False).reset_index(drop=True),
        height=400,
        use_container_width=True
    )
    
    # Opção para exportar dados
    if st.button("📤 Exportar Dados para CSV"):
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Baixar CSV",
            data=csv,
            file_name="transacoes_financeiras.csv",
            mime="text/csv"
        )

# ==============================================================================
# ABA DE VALUATION (SIMPLIFICADA)
# ==============================================================================

def ui_valuation():
    """Interface para análise de valuation de ativos."""
    st.header("📈 Análise Fundamentalista de Ativos")
    st.info("Esta funcionalidade está em desenvolvimento e será implementada em breve!")
    
# ==============================================================================
# ABA DE MODELO FLEURIET (SIMPLIFICADA)
# ==============================================================================

def ui_modelo_fleuriet():
    """Interface para aplicação do Modelo Fleuriet."""
    st.header("🔬 Análise de Liquidez - Modelo Fleuriet")
    st.info("Esta funcionalidade está em desenvolvimento e será implementada em breve!")

# ==============================================================================
# ABA DE SAÚDE FINANCEIRA
# ==============================================================================

def ui_saude_financeira():
    """Interface completa de saúde financeira."""
    st.header("❤️ Saúde e Projeção Financeira")
    
    tab_saude, tab_projecao, tab_cenarios = st.tabs([
        "📊 Saúde Financeira", 
        "🔮 Projeções", 
        "🔄 Cenários"
    ])
    
    with tab_saude:
        criar_dashboard_saude_financeira()
        
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Evolução do Score")
            # Implementar histórico de scores
            
        with col2:
            st.subheader("🎯 Recomendações Personalizadas")
            # Implementar recomendações baseadas em análise
            
    with tab_projecao:
        criar_projecao_patrimonial()
        
    with tab_cenarios:
        st.subheader("🔄 Análise de Cenários")
        st.info("Esta funcionalidade está em desenvolvimento e será implementada em breve!")

# ==============================================================================
# ESTRUTURA PRINCIPAL DO APP (MODIFICADA PARA ADICIONAR NOVA ABA)
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
    tab1, tab2, tab3, tab4 = st.tabs([
        "💰 Controle Financeiro", 
        "📈 Análise de Valuation", 
        "🔬 Modelo Fleuriet",
        "❤️ Saúde Financeira"
    ])
    
    with tab1:
        ui_controle_financeiro()
    
    with tab2:
        ui_valuation()
    
    with tab3:
        ui_modelo_fleuriet()
    
    with tab4:
        ui_saude_financeira()
    
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