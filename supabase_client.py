# supabase_client.py

import streamlit as st
import pandas as pd
from supabase import create_client, Client

# Etapa 1: Inicializar a conexão com o Supabase
# Usamos st.cache_resource para que a conexão seja criada apenas uma vez por sessão.
@st.cache_resource
def init_connection() -> Client:
    """
    Inicializa e retorna o cliente Supabase.
    As credenciais (URL e chave) são lidas dos segredos do Streamlit.
    """
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception as e:
        st.error(f"Erro ao conectar com o Supabase: {e}")
        return None

# Instancia o cliente para ser usado pelas outras funções no arquivo
supabase_client = init_connection()

# --- Etapa 2: Funções CRUD para a tabela 'transactions' ---

def fetch_transactions() -> pd.DataFrame:
    """
    Busca todas as transações do banco de dados e as retorna como um DataFrame do Pandas.
    Retorna um DataFrame vazio se não houver dados ou em caso de erro.
    """
    if supabase_client is None:
        return pd.DataFrame() # Retorna DF vazio se a conexão falhou
        
    try:
        # Seleciona todas as colunas ('*') e ordena pela coluna "Data" em ordem decrescente
        response = supabase_client.table("transactions").select("*").order("Data", desc=True).execute()
        
        # A API retorna os dados em uma lista de dicionários dentro de 'response.data'
        if response.data:
            df = pd.DataFrame(response.data)
            # Converte a coluna de data para o tipo datetime do pandas para manipulação correta
            df['Data'] = pd.to_datetime(df['Data'])
            return df
        else:
            # Retorna um DataFrame vazio com as colunas esperadas se a tabela estiver vazia
            return pd.DataFrame(columns=['id', 'created_at', 'Data', 'Tipo', 'Categoria', 'Subcategoria ARCA', 'Valor', 'Descrição'])
            
    except Exception as e:
        st.error(f"Erro ao buscar transações: {e}")
        return pd.DataFrame()

def add_transaction(data: dict):
    """
    Adiciona uma nova transação (um dicionário) ao banco de dados.
    """
    if supabase_client is None:
        return None

    try:
        # A data vem do st.date_input como um objeto 'date', convertemos para string no formato ISO
        data['Data'] = data['Data'].isoformat()
        
        # Insere o dicionário na tabela 'transactions'
        response = supabase_client.table("transactions").insert(data).execute()
        
        # Limpa os caches de dados e recursos para forçar o Streamlit a buscar os novos dados
        st.cache_data.clear()
        st.cache_resource.clear()
        return response
    except Exception as e:
        st.error(f"Erro ao adicionar transação: {e}")
        return None

def delete_transaction(transaction_id: int):
    """
    Exclui uma transação do banco de dados com base em seu ID único.
    """
    if supabase_client is None:
        return None
        
    try:
        # Deleta a linha onde a coluna 'id' é igual ao 'transaction_id' fornecido
        response = supabase_client.table("transactions").delete().eq("id", transaction_id).execute()
        
        # Limpa os caches para garantir que a lista de transações seja atualizada
        st.cache_data.clear()
        st.cache_resource.clear()
        return response
    except Exception as e:
        st.error(f"Erro ao excluir transação: {e}")
        return None
