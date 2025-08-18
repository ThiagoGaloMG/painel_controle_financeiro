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

# Ignorar avisos para uma saída mais limpa
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURAÇÕES GERAIS E LAYOUT DA PÁGINA (MANTIDO)
# ==============================================================================
# ... (existing CSS and config code remains unchanged) ...

# ==============================================================================
# SISTEMA DE PERSISTÊNCIA APRIMORADO (MELHORIAS ADICIONADAS)
# ==============================================================================

def inicializar_session_state():
    """Inicializa o estado da sessão com estruturas de dados mais robustas."""
    # ... (existing initialization code) ...
    
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

# ... (existing functions remain) ...

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
        razao_produtivos = metricas['invest_produtivos'] / total_investido
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
# COMPONENTES DE UI APRIMORADOS (NOVOS COMPONENTES ADICIONADOS)
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
# ABA DE SAÚDE FINANCEIRA (NOVA ABA)
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
        # Implementar análise de cenários

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
    
    # Menu principal em abas (ADICIONADA NOVA ABA)
    tab1, tab2, tab3, tab4 = st.tabs([
        "💰 Controle Financeiro", 
        "📈 Análise de Valuation", 
        "🔬 Modelo Fleuriet",
        "❤️ Saúde Financeira"  # NOVA ABA
    ])
    
    with tab1:
        ui_controle_financeiro()
    
    with tab2:
        ui_valuation()
    
    with tab3:
        ui_modelo_fleuriet()
    
    with tab4:  # CONTEÚDO DA NOVA ABA
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