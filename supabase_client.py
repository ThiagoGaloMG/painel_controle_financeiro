# supabase_client.py

import streamlit as st
import pandas as pd
from supabase import create_client, Client

# Etapa 1: Inicializar a conexão com o Supabase (esta parte não muda)
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

# --- Etapa 2: Funções CRUD atualizadas para a tabela 'transactions' ---

def fetch_transactions(user_id: str) -> pd.DataFrame:
    """
    Busca as transações de um usuário específico do banco de dados.
    """
    if supabase_client is None: return pd.DataFrame()
    try:
        # A query agora filtra pela coluna 'user_id' para buscar apenas os dados do usuário logado
        response = supabase_client.table("transactions").select("*").eq("user_id", user_id).order("Data", desc=True).execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            df['Data'] = pd.to_datetime(df['Data'])
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Erro ao buscar transações: {e}")
        return pd.DataFrame()

def add_transaction(data: dict, user_id: str):
    """
    Adiciona uma nova transação, associando-a a um user_id.
    """
    if supabase_client is None: return None
    try:
        # Garante que a data está no formato de texto correto (ISO)
        data['Data'] = pd.to_datetime(data['Data']).isoformat()
        # Adiciona o ID do usuário ao dicionário de dados antes de inserir no banco
        data['user_id'] = user_id
        
        response = supabase_client.table("transactions").insert(data).execute()
        # Limpa o cache para forçar a busca de novos dados
        st.cache_data.clear()
        return response
    except Exception as e:
        st.error(f"Erro ao adicionar transação: {e}")
        return None

def update_transaction(transaction_id: int, data: dict, user_id: str):
    """
    Atualiza uma transação existente, verificando a posse do usuário.
    """
    if supabase_client is None: return None
    try:
        # Garante que o formato da data está correto, se ela for alterada
        if 'Data' in data and hasattr(data['Data'], 'isoformat'):
            data['Data'] = pd.to_datetime(data['Data']).isoformat()
        
        # A query de update agora tem duas condições:
        # 1. O 'id' da transação deve corresponder.
        # 2. O 'user_id' da transação deve corresponder ao do usuário logado.
        response = supabase_client.table("transactions").update(data).eq("id", transaction_id).eq("user_id", user_id).execute()
        
        st.cache_data.clear()
        return response
    except Exception as e:
        st.error(f"Erro ao atualizar transação: {e}")
        return None

def delete_transaction(transaction_id: int, user_id: str):
    """
    Exclui uma transação, verificando a posse do usuário.
    """
    if supabase_client is None: return None
    try:
        # A query de delete também verifica o ID da transação e a posse do usuário
        response = supabase_client.table("transactions").delete().eq("id", transaction_id).eq("user_id", user_id).execute()
        
        st.cache_data.clear()
        return response
    except Exception as e:
        st.error(f"Erro ao excluir transação: {e}")
        return None
