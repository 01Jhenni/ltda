import urllib.parse
import webbrowser
import streamlit as st

def open_outlook(email, assunto, mensagem, arquivo_anexo=None):
    try:
        mailto_link = f"mailto:{email}?subject={urllib.parse.quote(assunto)}&body={urllib.parse.quote(mensagem)}"
        webbrowser.open(mailto_link)
        return True
    except Exception as e:
        st.error(f"❌ Erro ao abrir o Outlook: {str(e)}")
        return False 