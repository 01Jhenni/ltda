import streamlit as st
from supabase import create_client, Client
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
from io import BytesIO
from PIL import Image
import hashlib

# Inicialização do cliente Supabase
supabase: Client = create_client(
    st.secrets["supabase_url"],
    st.secrets["supabase_key"]
)

# Função para verificar se uma tabela existe no Supabase
def check_table_exists(table_name):
    try:
        response = supabase.table(table_name).select("*").limit(1).execute()
        return True
    except Exception as e:
        if "relation" in str(e) and "does not exist" in str(e):
            return False
        raise e

# Função para criar tabelas se não existirem
def create_tables_if_not_exist():
    try:
        # Verificar e criar tabela de registros se não existir
        if not check_table_exists("registros"):
            # Criar tabela registros
            response = supabase.table("registros").select("*").limit(1).execute()
            
            if response.error and "relation" in str(response.error) and "does not exist" in str(response.error):
                # Criar a tabela usando SQL direto
                create_table_query = """
                CREATE TABLE IF NOT EXISTS registros (
                    id BIGSERIAL PRIMARY KEY,
                    data TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()),
                    empresa TEXT NOT NULL,
                    tipo_nota TEXT NOT NULL,
                    erro TEXT,
                    arquivo_erro TEXT,
                    status TEXT DEFAULT 'Pendente',
                    arquivo TEXT,
                    tipo_arquivo TEXT,
                    usuario TEXT
                );
                """
                
                response = supabase.postgrest.rpc("exec_sql", {"query": create_table_query}).execute()
                
                if response.error:
                    st.error(f"❌ Erro ao criar tabela registros: {response.error.message}")
                    return False
                
                # Criar políticas de segurança
                policies = [
                    """
                    CREATE POLICY "Usuários podem ver registros de suas empresas"
                    ON registros FOR SELECT
                    USING (empresa = ANY(string_to_array((SELECT empresas FROM users WHERE username = current_user), ',')));
                    """,
                    """
                    CREATE POLICY "Usuários podem inserir registros para suas empresas"
                    ON registros FOR INSERT
                    WITH CHECK (empresa = ANY(string_to_array((SELECT empresas FROM users WHERE username = current_user), ',')));
                    """,
                    """
                    CREATE POLICY "Usuários podem atualizar registros de suas empresas"
                    ON registros FOR UPDATE
                    USING (empresa = ANY(string_to_array((SELECT empresas FROM users WHERE username = current_user), ',')))
                    WITH CHECK (empresa = ANY(string_to_array((SELECT empresas FROM users WHERE username = current_user), ',')));
                    """
                ]
                
                for policy in policies:
                    response = supabase.postgrest.rpc("exec_sql", {"query": policy}).execute()
                    if response.error:
                        st.error(f"❌ Erro ao criar política de segurança: {response.error.message}")
                        return False
        
        return True
    except Exception as e:
        st.error(f"❌ Erro ao criar tabelas: {str(e)}")
        return False

# Função para processar upload de arquivos
def process_upload(arquivo, empresa):
    try:
        if not arquivo:
            return True
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{timestamp}_{arquivo.name}"
        arquivo_path = f"arquivos/{empresa}/{nome_arquivo}"
        
        # Criar diretório temporário
        temp_dir = os.path.join(os.getcwd(), "temp_uploads")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Salvar arquivo temporariamente
        temp_path = os.path.join(temp_dir, nome_arquivo)
        with open(temp_path, "wb") as f:
            f.write(arquivo.getvalue())
        
        # Verificar se o bucket existe
        try:
            supabase.storage.from_("arquivos").list()
        except Exception:
            # Criar o bucket se não existir
            supabase.storage.create_bucket("arquivos")
        
        # Upload do arquivo para o Supabase Storage
        response = supabase.storage.from_("arquivos").upload(
            arquivo_path,
            arquivo.getvalue(),
            {"content-type": arquivo.type}
        )
        
        if response.error:
            st.error(f"❌ Erro ao salvar arquivo: {response.error.message}")
            return False
            
        return True
    except Exception as e:
        st.error(f"❌ Erro ao processar upload: {str(e)}")
        return False
    finally:
        # Limpar arquivo temporário
        if os.path.exists(temp_path):
            os.remove(temp_path)

# Função para salvar registro
def save_record(empresa, tipo_nota, erro, arquivo):
    try:
        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        arquivo_path = f"arquivos/{empresa}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{arquivo.name}" if arquivo else None
        
        registro_data = {
            "data": data_atual,
            "empresa": empresa,
            "tipo_nota": tipo_nota,
            "erro": erro,
            "arquivo_erro": arquivo_path if erro else None,
            "status": "OK" if not erro else "Pendente",
            "arquivo": arquivo_path if arquivo else None,
            "tipo_arquivo": arquivo.type if arquivo else None,
            "usuario": st.session_state.username
        }
        
        response = supabase.table("registros").insert(registro_data).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao salvar registro: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return False
            
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar registro: {str(e)}")
        return False

# Função para buscar registros
def get_registros(empresa, data_inicio=None, data_fim=None):
    try:
        query = supabase.table("registros").select("*").eq("empresa", empresa)
        
        if data_inicio and data_fim:
            query = query.gte("data", data_inicio.strftime("%Y-%m-%d")).lte("data", data_fim.strftime("%Y-%m-%d"))
        
        response = query.order("data", desc=True).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao buscar registros: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return pd.DataFrame()
            
        return pd.DataFrame(response.data)
    except Exception as e:
        st.error(f"❌ Erro ao buscar registros: {str(e)}")
        return pd.DataFrame()

# Função para buscar registros por período
def get_registros_periodo(data_inicio, data_fim):
    try:
        query = supabase.table("registros").select("*")
        
        if data_inicio and data_fim:
            query = query.gte("data", data_inicio.strftime("%Y-%m-%d")).lte("data", data_fim.strftime("%Y-%m-%d"))
        
        response = query.order("data", desc=True).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao buscar registros: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return pd.DataFrame()
            
        return pd.DataFrame(response.data)
    except Exception as e:
        st.error(f"❌ Erro ao buscar registros: {str(e)}")
        return pd.DataFrame()

# Função para exibir registros
def display_registros(registros):
    for index, row in registros.iterrows():
        with st.expander(f"📌 {row['empresa']} - {row['tipo_nota']} - {row['data']}"):
            st.write(f"**Status:** {row['status']}")
            st.write(f"**Erro:** {row['erro']}" if row['erro'] else "**Sem erro registrado.**")
            st.write(f"**Usuário:** {row['usuario']}")

            # Exibir arquivos
            if row['arquivo_erro']:
                display_arquivo(row['arquivo_erro'], "Arquivo de Erro")
            if row['arquivo']:
                display_arquivo(row['arquivo'], "Arquivo Principal")

            # Ações
            if row['status'] == "Pendente":
                if st.button("✔ OK", key=f"resolver_{row['id']}"):
                    update_status(row['id'], "Resolvido")

# Função para exibir arquivo
def display_arquivo(arquivo_path, titulo):
    try:
        response = supabase.storage.from_("arquivos").download(arquivo_path)
        if response:
            # Verificar extensão do arquivo
            extensao = os.path.splitext(arquivo_path)[1].lower()
            
            # Imagens
            if extensao in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp']:
                image = Image.open(BytesIO(response))
                st.image(image, caption=titulo, use_container_width=True)
            
            # PDFs
            elif extensao == '.pdf':
                st.download_button(
                    f"📄 Baixar PDF - {titulo}",
                    response,
                    os.path.basename(arquivo_path),
                    "application/pdf"
                )
            
            # Planilhas
            elif extensao in ['.xlsx', '.xls', '.csv']:
                st.download_button(
                    f"📊 Baixar Planilha - {titulo}",
                    response,
                    os.path.basename(arquivo_path),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # XML
            elif extensao == '.xml':
                st.download_button(
                    f"📋 Baixar XML - {titulo}",
                    response,
                    os.path.basename(arquivo_path),
                    "application/xml"
                )
            
            # Outros tipos de arquivo
            else:
                st.download_button(
                    f"📎 Baixar {titulo}",
                    response,
                    os.path.basename(arquivo_path),
                    "application/octet-stream"
                )
        else:
            st.warning(f"{titulo} não encontrado no armazenamento.")
    except Exception as e:
        st.error(f"❌ Erro ao carregar arquivo: {str(e)}")

# Função para atualizar status
def update_status(registro_id, novo_status):
    try:
        response = supabase.table("registros").update({"status": novo_status}).eq("id", registro_id).execute()
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao atualizar status: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
        else:
            st.success("✅ Status atualizado com sucesso!")
            st.rerun()
    except Exception as e:
        st.error(f"❌ Erro ao atualizar status: {str(e)}")

# Função para exibir métricas
def display_metricas(registros):
    if registros.empty:
        st.warning("Não há dados suficientes para exibir métricas.")
        return
        
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_registros = len(registros)
        st.metric(
            "Total de Registros",
            total_registros,
            help="Número total de registros no período selecionado"
        )
    
    with col2:
        registros_erro = len(registros[registros['erro'].notna()])
        st.metric(
            "Registros com Erro",
            registros_erro,
            help="Número de registros que apresentaram erros"
        )
    
    with col3:
        taxa_sucesso = ((total_registros - registros_erro) / total_registros * 100) if total_registros > 0 else 0
        st.metric(
            "Taxa de Sucesso",
            f"{taxa_sucesso:.1f}%",
            help="Percentual de registros sem erros"
        )
    
    with col4:
        empresas_unicas = registros['empresa'].nunique()
        st.metric(
            "Empresas Ativas",
            empresas_unicas,
            help="Número de empresas com registros no período"
        )
    
    # Adicionar informações detalhadas
    st.subheader("📊 Detalhamento")
    col5, col6 = st.columns(2)
    
    with col5:
        tipos_nota = registros['tipo_nota'].value_counts()
        st.write("**Tipos de Nota:**")
        for tipo, quantidade in tipos_nota.items():
            st.write(f"- {tipo}: {quantidade}")
    
    with col6:
        status_count = registros['status'].value_counts()
        st.write("**Status dos Registros:**")
        for status, quantidade in status_count.items():
            st.write(f"- {status}: {quantidade}")

# Função para exibir gráficos
def display_graficos(registros):
    if registros.empty:
        st.warning("Não há dados suficientes para exibir gráficos.")
        return
        
    col1, col2 = st.columns(2)
    
    # Gráfico de registros por empresa
    with col1:
        empresa_count = registros["empresa"].value_counts().reset_index()
        empresa_count.columns = ["Empresa", "Total de Registros"]
        fig1 = px.pie(
            empresa_count,
            names="Empresa",
            values="Total de Registros",
            title="📌 Registros por Empresa",
            hole=0.4
        )
        fig1.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig1, use_container_width=True)
    
    # Gráfico de tipos de nota
    with col2:
        tipo_nota_count = registros["tipo_nota"].value_counts().reset_index()
        tipo_nota_count.columns = ["Tipo de Nota", "Quantidade"]
        fig2 = px.pie(
            tipo_nota_count,
            names="Tipo de Nota",
            values="Quantidade",
            title="📌 Tipos de Nota Registradas",
            hole=0.4
        )
        fig2.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    
    # Gráfico de linha temporal
    st.subheader("📈 Registros por Dia")
    registros_por_dia = registros.groupby(pd.to_datetime(registros['data']).dt.date).size().reset_index()
    registros_por_dia.columns = ['Data', 'Quantidade']
    fig3 = px.line(
        registros_por_dia,
        x='Data',
        y='Quantidade',
        title='Evolução Diária de Registros',
        markers=True
    )
    fig3.update_layout(
        xaxis_title="Data",
        yaxis_title="Quantidade de Registros",
        hovermode='x unified'
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    # Gráfico de erros
    if registros["erro"].notna().sum() > 0:
        st.subheader("🔴 Erros Mais Comuns")
        erro_count = registros["erro"].value_counts().reset_index().head(5)
        erro_count.columns = ["Erro", "Frequência"]
        fig4 = px.pie(
            erro_count,
            names="Erro",
            values="Frequência",
            title="Distribuição dos Erros Mais Frequentes",
            hole=0.4
        )
        fig4.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig4, use_container_width=True)
        
        # Tabela de erros
        st.subheader("📊 Detalhamento dos Erros")
        st.dataframe(erro_count, use_container_width=True)

# Função para exibir tabela
def display_tabela(registros):
    st.subheader("📊 Dados Detalhados")
    st.dataframe(registros)

# Função para exportar dados
def export_data(registros):
    st.subheader("📥 Download de Dados")
    csv = registros.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Baixar Planilha Completa",
        data=csv,
        file_name=f"registros_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

# Função para hashear senha
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest() 


     
