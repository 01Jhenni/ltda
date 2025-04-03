import streamlit as st
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import pandas as pd
import plotly.express as px
import shutil
import zipfile
import tempfile
from io import BytesIO
import hashlib
import webbrowser
import urllib.parse
from datetime import date, datetime
from PIL import Image

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(url, key)

# Função para verificar se uma tabela existe no Supabase
def check_table_exists(table_name):
    try:
        response = supabase.table(table_name).select("*").limit(1).execute()
        return True
    except Exception as e:
        if "relation" in str(e) and "does not exist" in str(e):
            return False
        raise e

# Função para carregar as mensagens do Supabase
def load_messages():
    try:
        # Verificar se a tabela messages existe
        if not check_table_exists("messages"):
            st.warning("A tabela 'messages' ainda não foi criada no Supabase. Por favor, crie a tabela com as seguintes colunas: id, username, message, created_at")
            return []

        response = supabase.table("messages").select("*").order("created_at", desc=True).limit(50).execute()
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao carregar mensagens: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return []
        return response.data
    except Exception as e:
        st.error(f"❌ Erro ao carregar mensagens: {str(e)}")
        return []

# Função para salvar as mensagens no Supabase
def save_message(username, message):
    try:
        # Verificar se a tabela messages existe
        if not check_table_exists("messages"):
            st.warning("A tabela 'messages' ainda não foi criada no Supabase. Por favor, crie a tabela com as seguintes colunas: id, username, message, created_at")
            return False

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        response = supabase.table("messages").insert({
            "username": username,
            "message": message,
            "created_at": timestamp
        }).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao salvar mensagem: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return False
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar mensagem: {str(e)}")
        return False

# Lista de empresas disponíveisDA", 
lista_empresas = ["2B COMBUSTIVEL LTDA", "A REDE GESTAO PATRIMONIAL LTDA", "A.M CHEQUER IMOVEIS LTDA", "A.R. PARTICIPACOES LTDA", "ABEL CONSTRUTORA LTDA", "ABEL SEMINOVOS LTDA", "ACOLOG LOGISTICA LTDA", "ACOS SERVICOS DE PROMOCAO LTDA", "ACR GESTAO PATRIMONIAL LTDA", "ADCAR SERVICO DE ESCRITORIO E APOIO ADMINISTRATIVO LTDA", "ADR MOBILIDADE E SERVICOS LTDA", "ADS COMERCIO E IMPORTACAO E EXPORTACAO EIRELI", "AESA PARTICIPAÇÕES LTDA", "AGM ESQUADRIAS LTDA", "AGP03 EMPREENDIMENTOS IMOBILIARIOS SPE LTDA", "AGP03 EMPREENDIMENTOS IMOBILIARIOS SPE LTDA FILIAL 02-82", "AGP03 EMPREENDIMENTOS IMOBILIARIOS SPE LTDA SCP RESIDENCIAL CAPARAO", "AGP05 ARGON EMPREENDIMENTOS IMOBILIARIOS SPE LTDA", "AGROPECUARIA BONANZA LTDA", "AGT01 MORADA NOVA DE MINAS SPE LTDA", "AGT01 MORADA NOVA DE MINAS SPE LTDA FILIAL 02-52", "AGUIA 8 COMERCIO DE COMBUSTIVEIS LTDA", "AGUIA IV COMERCIO DE COMBUSTIVEIS LTDA", "AGUIA IX COMERCIO DE COMBUSTIVEIS LTDA", "AGUIA V COMERCIO DE COMBUSTIVEIS LTDA", "ALLOTECH CONSULTORIA EM PRODUCAO INDUSTRIAL LTDA", "ALVES E SANTOS PARTICIPACOES LTDA", "AMH COMERCIO E SERVICOS LTDA", "AML HOLDING S/A", "AMMC PARTICIPACOES LTDA", "AMPLUS PARTICIPACOES SA", "AMX GESTÃO PATRIMONIAL LTDA", "ANF EMPREENDIMENTOS E PARTICIPACOES LTDA", "ANITA CHEQUER PARTICIPACOES LTDA", "ANITA CHEQUER PATRIMONIAL LTDA", "APL ADMINISTRACAO E PARTICIPACOES LTDA", "APMG PARTICIPACOES S/A", "ARCI PARTICIPACOES LTDA", "ARCI PATRIMONIAL LTDA", "ARGON ENGENHARIA LTDA", "ARNDT PATRIMONIAL LTDA", "ARNDT REFORMAS E MANUTENCOES LTDA", "ARNDT, TRAVASSOS E MORRISON SPE LTDA", "ARTMIX HOLDING LTDA", "AUMAR PRESTACAO DE SERVICOS ADMINISTRATIVOS LTDA", "AUTO POSTO ALELUIA LTDA", "AUTO POSTO ALELUIA LTDA FILIAL 02-46", "AUTO POSTO CENTENARIO LTDA", "AUTO POSTO DAS LAJES LTDA", "AUTO POSTO DOM BOSCO LTDA", "AUTO POSTO MAQUINE LTDA", "AUTO POSTO MARIO CAMPOS COMERCIO DE COMBUSTIVEIS LTDA", "AUTO POSTO PORTAL DO NORTE LTDA", "AUTO POSTO VERONA LTDA", "AUTOREDE LOCADORA DE VEICULOS LTDA", "AUTOREDE PARTICIPACOES LTDA", "AXJ PARTICIPACOES EIRELI", "AXP GESTAO PATRIMONIAL LTDA", "AZEVEDO & CIA", "BARAO VPP CONVENIENCIAS LTDA", "BARTELS DERMATOLOGIA ESTETICA E LASER LTDA", "BEL DISTRIBUIDOR DE LUBRIFICANTES LTDA", "BEL DISTRIBUIDOR DE LUBRIFICANTES LTDA FILIAL 02-79", "BEL LUBRIFICANTES ESPECIAIS LTDA", "BELTMORE PARTICIPACOES LTDA", "BEMX - PARTICIPACOES E EMPREENDIMENTOS LTDA", "BIOCLINTECH CIENTIFICA LTDA", "BIOCLINTECH LTDA", "BIOCLINTECH MANUTENCAO LTDA", "BLUE SKY PARTICIPACOES LTDA", "BMC EDITORA LTDA", "BMGL PARTICIPACOES E EMPREENDIMENTOS IMOBILIARIOS LTDA", "BOA VISTA ASSESSORIA LTDA", "BOA VISTA BOCAIUVA HOTEL LTDA", "BORA EMBALAGENS LTDA", "BRANT EMPREENDIMENTOS LTDA", "BRASIL CONCRETO LTDA", "BRAZIL MANIA LTDA", "BRB TRANSPORTES LTDA", "BRM COMERCIO DE VEICULOS LTDA", "BRM COMERCIO DE VEICULOS LTDA FILIAL 02-24", "BROMELIAS GESTAO PATRIMONIAL LTDA", "BURITIS CONVENIENCIA LTDA", "BV DISTRIBUIDORA LTDA", "CAPITAO COMERCIO DE COMBUSTIVEIS LTDA", "CASA NOVA SPE LTDA", "CASA SEMPRE VIVA COMERCIO DE MATERIAIS DE CONSTRUCAO LTDA", "CASCALHO PARTICIPACOES LTDA", "CATIRA INTERMEDIACOES DE NEGOCIOS LTDA" , "CAD COMERCIAL DE MAQUINAS LTDA", "CAMPO ALEGRE PARTICIPACOES LTIACOES DE NEGOCIOS LTDA", "CCA COMERCIAL DE COMBUSTIVEIS AUTOMOTIVOS LTDA", "CDI NUCLEAR LTDA", "CDVM LTDA", "CELT -COMERCIO DE COMBUSTIVEIS E LUBRIFICANTES LTDA", "CENTER POSTO LTDA", "CENTER POSTO LTDA FILIAL 02-12", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 02-49", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 04-00", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 05-91", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 07-53", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 09-15", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 10-59", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 11-30", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 14-82", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 17-25", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 19-97", "CENTRO DE DIAGNOSTICO POR IMAGEM LTDA FILIAL 20-20", "CGA SERVICOS MEDICOS LTDA", "CGI - EMPREENDIMENTOS COMERCIAL LTDA", "CHAVE DE OURO EMPREENDIMENTOS IMOBILIARIOS EIRELI", "CHEL LTDA", "CHEQUER & COELHO LTDA", "CIA ITABIRITO INDUSTRIAL FIACAO E TECELAGEM DE ALGODAO", "CIAZA CONSTRUTORA LTDA", "CLAM CONSULTORIA LTDA", "CLAM ENGENHARIA LTDA", "CLAM ENGENHARIA LTDA FILIAL 03-00", "CLAM ESG LTDA", "CLAM MEIO AMBIENTE LTDA", "CLAM MEIO AMBIENTE LTDA FILIAL 02-49", "CLAM MONITORAMENTO AMBIENTAL LTDA", "CLAM MONITORAMENTO AMBIENTAL LTDA", "CLAM PARTICIPACOES E INVESTIMENTOS S/A", "CLINICA LEV SAVASSI LTDA", "CLINICA RADIOLOGICA ELDORADO LTDA", "CLINICA UNIAO SERVICOS MEDICOS LTDA", "COELHO CONVENIENCIA LTDA", "COELHO E PEREIRA EIRELI", "COLISEU SERVICOS ADMINISTRATIVOS LTDA", "COMERCIAL AVIAMENTOS LTDA", "COMERCIAL FOCCUS LTDA", "COMERCIAL GIULIANO LIMITADA", "COMERCIAL OLIVEIRA & BRANT LTDA", "COMERCIAL OLIVEIRA & BRANT LTDA 02", "COMERCIO LANCHE KARRAO LTDA", "CONSORCIO ENGEBRAS ILCON SES 0620240094", "CONSORCIO GERASUN SOLAR", "CONSTANTINO MATIAS NOGUEIRA - IMOVEIS", "CONSTANTINO MATIAS NOGUEIRA - PATRIMONIAL LTDA", "CONSTRUTORA AGMAR LTDA", "CONSTRUTORA AGMAR LTDA", "CONSTRUTORA AGMAR LTDA FILIAL 02-10", "CONSTRUTORA E INCORPORADORA SPLIT LTDA", "CONTABILIDADE LTDA", "CONVENIENCIA DOIS IRMAOS - EIRELI", "CONVENIENCIA DOIS IRMAOS LTDA", "CORRETORA DE SEGUROS BELO HORIZONTE LTDA", "CRISTAL VALLE ADMINISTRACAO LTDA", "CRISTAL VALLE INDUSTRIA E COMERCIO DE VIDROS LTDA", "CSV GESTAO PATRIMONIAL LTDA", "CVQ I SPE LTDA", "CVQ II SPE LTDA", "D.L.A. SERVICOS ADMINISTRATIVOS LTDA", "DEBURR COMERCIO DE COSMETICOS LTDA", "DEL PAPEIS LTDA", "DEL PAPEIS LTDA FILIAL 03-84", "DELMA - COMERCIO DE COMBUSTIVEIS LTDA", "DH ORIGINAL IMPORTACAO E EXPORTACAO LTDA", "DISTRIBUIDORA BIOCLIN DIAGNOSTICA LTDA", "DJB PARTICIPACOES LTDA", "DOBRAFLEX CORTE E DOBRA DE METAIS LTDA", "DVS PARTICIPACOES LTDA", "E.D. TECNOLOGIA DIGITAL BH LTDA", "EAGLE ADMINISTRACAO LTDA", "EDIFICIO REDE OFFICE I", "ELETROFERRAGENS RM EIRELI", "EMAG CONSTRUTORA LTDA", "EMAG CONSTRUTORA LTDA 02", "EMIS MINAS DISTRIBUIDORA DE PRODUTOS FARMACEUTICOS LTDA", "EMP - AVALIACAO EM RECURSOS HUMANOS LTDA", "EMPIRE DJB PATRIMONIAL LTDA", "ENERGY TRANSPORTES LTDA", "ENGEBRAS CONSTRUTORA LTDA", "ESMIG INDUSTRIA DE ESCADAS LTDA", "ESMIG INDUSTRIA DE ESCADAS LTDA FILIAL 03-89", "ESTACIONAMENTO AGMAR LTDA", "ESTACIONAMENTO AGMAR LTDA FILIAL 02-06", "ESTACIONAMENTO AGMAR LTDA FILIAL 03-89", "ESTACIONAMENTO AGMAR LTDA FILIAL 04-60", "ESTIVA PARTICIPACOES LTDA","EAT FRUTZ ALIMENTOS LTDA" , "EVELINE DE PAULA BARTELS", "EVERYBODY - CENTRO DE PERFORMANCE E FISIOTERAPIA LTDA", "EVOLUTION CONSULTORIA E GESTAO EMPRESARIAL S/A", "EXPRESSO FERRENSE LTDA", "FAST GESTAO DE RECURSOS LTDA", "FAST TEAM SERVICOS DE ESTETICA AUTOMOTIVA LTDA", "FASTPLOT SERVICOS DE ESTETICA AUTOMOTIVA LTDA", "FATIMA ADMINISTRACAO LTDA", "FCF CONSULTORIA LTDA", "FCK PREMOLDADOS LTDA FILIAL 02", "FCK PREMOLDADOS LTDA FILIAL 03", "FCK PREMOLDADOS LTDA", "FCK TRANSPORTES LTDA", "FEAG - FERRAGENS AGMAR PARA FACHADA EIRELI", "FERREIRA ADMINISTRACAO LTDA", "FERRO E ACO TAKONO LTDA", "FERRO E ACO TAKONO LTDA FILIAL 0028-03", "FERRO E ACO TAKONO LTDA FILIAL 06-06", "FERRO E ACO TAKONO LTDA FILIAL 07-89", "FERRO E ACO TAKONO LTDA FILIAL 08-60", "FERRO E ACO TAKONO LTDA FILIAL 10-84", "FERRO E ACO TAKONO LTDA FILIAL 11-65", "FERRO E ACO TAKONO LTDA FILIAL 12-46", "FERRO E ACO TAKONO LTDA FILIAL 13-27", "FERRO E ACO TAKONO LTDA FILIAL 14-08", "FERRO E ACO TAKONO LTDA FILIAL 15-99", "FERRO E ACO TAKONO LTDA FILIAL 16-70", "FERRO E ACO TAKONO LTDA FILIAL 18-31", "FERRO E ACO TAKONO LTDA FILIAL 19-12", "FERRO E ACO TAKONO LTDA FILIAL 20-56", "FERRO E ACO TAKONO LTDA FILIAL 21-37", "FERRO E ACO TAKONO LTDA FILIAL 22-18", "FERRO E ACO TAKONO LTDA FILIAL 23-07", "FERRO E ACO TAKONO LTDA FILIAL 24-80", "FERRO E ACO TAKONO LTDA FILIAL 25-60", "FERRO E ACO TAKONO LTDA FILIAL 26-41", "FERRO E ACO TAKONO LTDA FILIAL 27-22", "FIX MANUTENCAO PREDIAL LTDA", "FIX MANUTENCOES LTDA", "FLARA GESTAO PATRIMONIAL LTDA", "FMPL PARTICIPACOES LTDA", "FORTRESS GESTAO PARTICIPACOES LTDA", "FRADE PARTICIPACOES LTDA", "FS PROCESSAMENTO DE DADOS LTDA", "GAMA SERV LTDA", "GECORP GESTAO DE BENEFICIOS E CORRETORA DE SEGUROS LTDA", "GENETICENTER - CENTRO DE GENETICA LTDA", "GERTH CONSULTORIA E PROMOCOES DE VENDAS LTDA", "GFF ENGENHARIA LTDA", "GIBRALTAR HOLDING LTDA", "GIOVANNI CARLECH GUIMARAES MARQUEZANI", "GMAC ESTACIONAMENTOS LTDA", "GMB ASSESSORIA LTDA", "GNV SETE BELO LTDA", "GPK PARTICIPACOES LTDA", "GR COMBUSTIVEIS LTDA", "GRB INDUSTRIA E COMERCIO DE EQUIPAMENTOS LTDA", "GRB INDUSTRIA E COMERCIO DE EQUIPAMENTOS LTDA FILIAL 03-50", "GRB INDUSTRIA E COMERCIO DE EQUIPAMENTOS LTDA FILIAL 04-31", "GRB INDUSTRIA E COMERCIO DE EQUIPAMENTOS LTDA FILIAL 05-12", "GROUND GESTAO PATRIMONIAL LTDA", "GSC GESTAO PATRIMONIAL LTDA", "GUIMARAES & VIEIRA DE MELLO SOCIEDADE DE ADVOGADOS", "GUIMARAES E VIEIRA DE MELLO ADVOGADOS", "GVM ADMINISTRACAO E CONSULTORIA LTDA", "GVM CONSORCIO", "GVM CORRETORA DE SEGUROS LTDA", "GWS ENGENHARIA LTDA", "GWS TECH LTDA", "H.C.-COMERCIO DE ALIMENTOS LTDA", "H.C.-COMERCIO DE ALIMENTOS LTDA 04", "HAND SHOP SUPRIMENTOS MEDICOS E TERAPEUTICOS LTDA", "HC COMERCIO DE ALIMENTOS LTDA 05", "HC COMERCIO DE ALIMENTOS LTDA FILIAL 03-63", "HOLDING MAIS MABC LTDA", "HOTEL SAO BENTO LTDA", "HPX PARTICIPACOES LTDA", "HSP GESTAO PATRIMONIAL LTDA", "HVAR INCORPORACOES LTDA", "I9 GESTAO E PARTICIPACOES LTDA", "IB COMBUSTIVEL LTDA", "IB TRANSPORTES & EMPREENDIMENTOS LTDA", "INCONFIDENTES PARTICIPACOES LTDA", "INCORPORADORA MONTE VERDE SPE LTDA", "INDUSTRIA DE TRANSFORMADORES KING LIMITADA", "INFLUXO SOCIEDADE DE PROFISSIONAIS", "INSTITUTO PERSONA INTELIGENCIA EMOCIONAL LTDA", "INTERWEG ADM E CORRETORA DE SEGUROS LTDA", "INTERWEG CORRETORA DE SEGUROS E BENEFICIOS LTDA", "J.V.V. GESTAO PATRIMONIAL LTDA", "JACL PARTICIPACOES LTDA", "JARDINO MALL LTDA", "JCA SERVICOS DE RADIOLOGIA LTDA", "JCM PARTICIPACOES LTDA", "JHCL GESTAO PATRIMONIAL LTDA", "JJA LOCACOES LTDA", "JKV GESTAO PATRIMONIAL LTDA", "JPAMACEDO E PARTICIPACOES LTDA", "JR LAVA JATO EIRELI", "JVC PARTICIPACOES LTDA", "JVP GESTAO PATRIMONIAL LTDA", "K10 PARTICIPACOES LTDA", "KALAB LOPES GESTAO PATRIMONIAL LTDA", "KALAB NEGOCIOS IMOBILIARIOS LTDA", "L.O. IMPORT EXPORT LTDA", "LABORATORIO DE PATOLOGIA CIRURGICA E CITOPATOLOGIA LTDA", "LABORATORIO DE PATOLOGIA CIRURGICA E CITOPATOLOGIA LTDA FILIAL 03-44", "LETOM EMPREENDIMENTOS LTDA", "LGX - PARTICIPACOES E ADMINISTRACAO LTDA", "LINK INDUSTRIA E COMERCIO DE MAQUINAS PARA MINERACAO LTDA", "LINK INDUSTRIA E COMERCIO DE MAQUINAS PARA MINERACAO LTDA FILIAL 03-05", "LOCS LOCADORA DE VEICULOS LTDA", "LS ENTERPRISE SOLLUTIONS LTDA", "M A B COSTA LTDA", "M L SILVEIRA SERVICOS CORPORATIVOS EIRELI", "MADA CLINICA ODONTOLOGICA LTDA", "MAIS CONSTRUCOES LTDA", "MAIS NEGOCIOS E REPRESENTACOES LTDA", "MAQUINAS RABELLO ITABAYANA LIMITADA", "MAR UP CONSULTORIA GESTAO E REPRESENTACAO COMERCIAL LTDA", "MAR9 TRATAMENTO DE DADOS LTDA", "MARCHALENTA AUTO SERVICOS LTDA", "MARCHALIVRE SERVICOS E PECAS LTDA", "MARCO GRILLI COMERCIO DE OBJETOS DE ARTE LTDA", "MARIA CHEQUER PARTICIPACOES LTDA", "MARIA CHEQUER PATRIMONIAL LTDA", "MARIANA CARLECH GUIMARAES MARQUEZANI", "MARICABI GESTAO PATRIMONIAL LTDA", "MASSIME DISTRIBUIDORA DE MEDICAMENTOS LTDA", "MASTER AUTO POSTO LTDA", "MASTER EMPREENDIMENTOS E PARTICIPACOES LTDA", "MASTER PISOS MATERIAL DE CONSTRUCAO EIRELI", "MATTA NUNES REPRESENTACOES COMERCIAIS E GESTAO DE NEGOCIOS LTDA", "MAX GESTAO PATRIMONIAL LTDA", "MD EMPREENDIMENTOS S.A", "MEDWAY SOLUCOES PARA A SAUDE LTDA", "MENDONCA E FERREIRA PARTICIPACOES LTDA", "MENDONCA E FERREIRA PATRIMONIAL LTDA", "MENDONCA E FILHOS GESTAO PATRIMONIAL LTDA", "MENDONCA PARTICIPACOES LTDA", "MEROS GESTAO PATRIMONIAL LTDA", "MG CONVENIENCIA LTDA", "MG CONVENIENCIA LTDA FILIAL 02-57", "MICRONIC COMERCIO E INDUSTRIA LTDA", "MILENE GUIMARAES MARQUEZANI", "MINAS GERAIS ADMINISTRADORA DE IMOVEIS LTDA", "MINEIRAO POSTO DE SERVICOS LTDA", "MLM HOLDING EIRELI", "MM COMERCIO DE DERIVADOS DE PETROLEO LTDA", "MMORAES PARTICIPACOES LTDA", "MONTE VERDE EDIFICACOES I SPE LTDA", "MONTE VERDE URBANIZACOES SPE LTDA", "MP INCORPORACOES LTDA", "MRI MOVIMENTACAO E RECUPERACAO INDUSTRIAL LTDA", "MRLIZ CONSULTORIA LTDA", "MTL PARTICIPACOES LTDA", "MULT SERVICOS ADMINISTRATIVOS LTDA", "MWA PARTICIPACOES LTDA", "MWA PATRIMONIAL LTDA", "NACIONAL RENOVAVEIS LTDA", "NATUREZA X COMERCIO LTDA", "NOSSA OBRA VAREJO DIGITAL LTDA", "NRSM REFORMAS LTDA", "NVB SERVICOS LTDA", "OLE PARTICIPACOES LTDA", "OLIVEIRA SANTOS ADVOGADOS", "OPEN-5 LTDA", "OPEN-5 LTDA 02", "OPX PARTICIPACOES LTDA", "ORGANIZACAO COMERCIAL MARINHO LTDA", "ORGANIZACOES SOUKI EIRELI", "ORGANIZACOES SOUKI EIRELI FILIAL 03-32", "ORIENT AUTOMOVEIS PECAS E SERVICOS LTDA", "ORIENT AUTOMOVEIS PECAS E SERVICOS LTDA FILIAL 03-43", "ORIENT AUTOMOVEIS PECAS E SERVICOS LTDA FILIAL 04-24", "ORIENTE FARMACEUTICA COMERCIO IMPORTACAO E EXPORTACAO LTDA", "PADUA COMERCIO E INDUSTRIA LTDA", "PAIVA BRANT LTDA", "PAIVA EMPREENDIMENTOS E GESTAO DE IMOVEIS PROPRIOS LTDA", "PCFORYOU LTDA", "PEMAX INTERMEDIACAO E NEGOCIOS LTDA", "PERFORMANCE GESTAO EMPRESARIAL LTDA", "PETRODATA PROCESSAMENTO DE DADOS LTDA", "PLATAFORMA AM3 LTDA", "PNEUS JUA COMERCIO DE PNEUS LTDA", "PONTUAUTO CENTRO AUTOMOTIVO LTDA", "POP EMPREENDIMENTOS E PARTICIPACOES S/A", "POSTO AEROPORTO LTDA", "POSTO AGUIA COMERCIO DE COMBUSTIVEIS LTDA", "POSTO ALAMO LTDA", "POSTO ALLGAS LTDA", "POSTO AVENIDA BRASIL COMERCIO DE COMBUSTIVEIS LTDA", "POSTO BALNEARIO AGUA LIMPA LTDA", "POSTO BARAO VPP LTDA", "POSTO BERIMBAU LTDA", "POSTO BURITIS LTDA", "POSTO CATEDRAL LTDA", "POSTO CENTER NORTE LTDA", "POSTO COELHO LTDA", "POSTO DANUBIO LTDA", "POSTO DE COMBUSTIVEIS CENTER SUL LTDA", "POSTO DE COMBUSTIVEIS SANTO AGOSTINHO LTDA", "POSTO DE COMBUSTIVEL PETROLANDIA LTDA", "POSTO DE COMBUSTIVEL VILA CRUZEIRO LIMITADA", "POSTO ESTORIL LTDA", "POSTO FORMULA BR LTDA", "POSTO HUGO WERNECK LTDA", "POSTO IPE COMERCIO DE COMBUSTIVEIS LTDA", "POSTO IRMAOS AULER LTDA", "POSTO JUPITER LTDA", "POSTO LESTE LTDA", "POSTO MARIO WERNECK LIMITADA", "POSTO MAURITANIA LTDA", "POSTO MINAS SHOPPING LTDA", "POSTO MINASLANDIA LTDA", "POSTO MONTE VERDE LTDA", "POSTO MUSTANG LTDA", "POSTO NOGUEIRINHA LTDA", "POSTO OCEANO AZUL LTDA", "POSTO OCEANO LTDA", "POSTO PANAMERA LTDA", "POSTO PARQUE BURITIS LTDA", "POSTO PARQUE JARDIM LTDA", "POSTO PICA PAU LTDA", "POSTO POETA LTDA", "POSTO PORTAL DE BETIM LTDA", "POSTO PORTAL DE CONTAGEM LTDA", "POSTO PORTAL DOS CAICARAS LTDA", "POSTO SIGMA LTDA", "POSTO SOBERANO AUTORAMA LTDA", "POSTO SOBERANO KARRAO LTDA", "POSTO SOBERANO SETE DE SETEMBRO LTDA", "POSTO TATIANA LTDA", "POSTO TROVAO LTDA", "POSTO VIA FERNAO DIAS LTDA", "POSTO VILA CHALE LTDA", "POSTO VILA DA SERRA LTDA", "POSTO VILA PICA PAU LTDA", "POSTO ZEPPE GRAND PRIX LTDA", "POSTO ZEPPE MG LTDA", "POSTO ZEPPE OASIS LTDA", "POSTO ZEPPE SAO JOSE LTDA", "POSTO ZEPPELIN LTDA", "PPML INDUSTRIA E COMERCIO DE ROUPAS EIRELI", "PRESERVAR PARTICIPACOES LTDA", "PRIMA LINEA AUTOMOVEIS LTDA", "PRIMOLA FRAGRANCIAS LTDA FILIAL 04-30", "PROFIT FOODS LTDA", "PROJETOUM COMERCIO E REPRESENTACOES LTDA", "PROJETOUM COMERCIO E REPRESENTACOES LTDA FILIAL 0002-27", "PROPELLER LTDA", "PROSERVICE LTDA", "PROSPECTIVA SOCIEDADE DE PROFISSIONAIS", "PURA SAUDE ALIMENTOS LTDA", "PURA SAUDE ALIMENTOS LTDA FILIAL 02-88", "QUADRIJET ALPHAVILLE COMERCIO LTDA", "QUEOPZ GESTAO PATRIMONIAL LTDA", "QUIBASA QUIMICA BASICA LTDA", "QUIBASA QUIMICA BASICA LTDA FILIAL 02-98", "QUIBASA QUIMICA BASICA LTDA FILIAL 03-79", "QUICK CONVENIENCIAS LTDA", "QUICK LUBE COMERCIO DE PRODUTOS E FRANQUIAS LTDA", "RACCO EQUIPAMENTOS E SERVICOS EIRELI", "RACCO SERVICOS DE PUBLICIDADE E COMUNICACAO LTDA", "RC INVEST PARTICIPACOES LTDA", "RECICLAGEM PASSARELA LTDA", "REDE A PUBLICIDADE E PROPAGANDA LTDA", "REDE OFFICE INCORPORACOES LTDA", "RESIDENCIAL ANDORINHAS SPE LTDA", "RESIDENCIAL PACIFICO RIBEIRAO DAS NEVES SPE LTDA", "RESIDENCIAL PACIFICO RIBEIRAO DAS NEVES SPE LTDA FILIAL 02-72", "RESIDENCIAL VILA AMAZONAS SPE LTDA","RESIDENCIAL VILA AMAZONAS SPE LTDA FILIAL 02-91", "RESIDENCIAL VILA ATLANTICO SABARA SPE LTDA", "RESIDENCIAL VILA ATLANTICO SABARA SPE LTDA 02", "RESIDENCIAL VILA CONCEICAO SPE LTDA", "RESIDENCIAL VILA CONCEICAO SPE LTDA FILIAL 02-01", "RESIDENCIAL VILA MORGANTI I SCP", "RESIDENCIAL VILA MORGANTI I SPE LTDA", "RESIDENCIAL VILA MORGANTI I SPE LTDA FILIAL 02-05", "RESIDENCIAL VILA SAO JOSE I SPE LTDA", "RESIDENCIAL VILA SAO JOSE I SPE LTDA FILIAL 02-64", "RESIDENCIAL VILA SAO JOSE II SPE LTDA", "RESIDENCIAL VILA SAO JOSE II SPE LTDA FILIAL 0002-01", "RESIDENCIAL VILA SAO JOSE SCP", "RETES IMAGENS SERVICOS E CONSULTORIA LTDA", "RFX ADMINISTRACAO DE RECURSOS LTDA", "RFX CONSULTORIA E GESTAO DE NEGOCIOS LTDA", "RFX DISTRIBUIDORA DE PRODUTOS AUTOMOTIVOS LTDA", "RFX GESTAO PATRIMONIAL LTDA", "RFX LOGISTICA E TRANSPORTES DE COMBUSTIVEIS LTDA", "RFX TREINAMENTO PROFISSIONAL LTDA", "RGGC EMPREENDIMENTOS LTDA", "RH CENTRO DE SAUDE LTDA", "RICARDO SANTOS BRANT", "ROCKET GESTAO PATRIMONIAL LTDA", "ROL COMERCIO DE DERIVADOS DE PETROLEO LTDA", "RPB COMERCIO DE COMBUSTIVEL LTDA", "RSM COMERCIO E GERENCIAMENTO DE RESIDUOS GUAXUPE EIRELI", "SAINT EMILION AUTOMOVEIS PECAS E SERVICOS LTDA", "SAINT EMILION AUTOMOVEIS PECAS E SERVICOS LTDA 02", "SAINT EMILION AUTOMOVEIS PECAS E SERVICOS LTDA 03", "SAINT EMILION AUTOMOVEIS PECAS E SERVICOS LTDA 04", "SANTA CLARA AGROPECUARIA LTDA", "SANTA MARIA ECOLOGIC EQUIPAMENTOS LTDA", "SANTA MARIA ECOLOGIC LTDA", "SANTA MARIA ECOLOGIC 02", "SANTA MARIA ECOLOGIC 04", "SANTA MARIA ECOLOGIC 05", "SANTA MARIA ECOLOGIC 06", "SANTA MARIA ECOLOGIC 08", "SANTA MARIA ECOLOGIC 09", "SANTA MARIA ECOLOGIC 10", "SANTA MARIA ECOLOGIC 11", "SANTA MARIA ECOLOGIC 13", "SANTA MARIA ECOLOGIC 14", "SANTA MARIA ECOLOGIC 15", "SANTA MARIA ECOLOGIC RESIDUOS LTDA", "SANTORINI POSTO DE SERVICOS LTDA", "SARAMENHA ENGENHARIA LTDA", "SCP AUDITORIA DE IMPOSTOS E CONTRIBUICOES", "SCP CLAM ENGENHARIA LTDA", "SCP CLINICA RADIOLOGICA ELDORADO LTDA", "SCP DIAGNOSTICO BETIMBARREIRO LTDA", "SCP RESIDENCIAL VILA CONCEICAO", "SCP VILA PACIFICO SANCRUZA", "SE LOTEAMENTOS LTDA", "SETEC- CONSULTORIA EMPRESARIAL LTDA", "SICAL INDUSTRIAL LTDA", "SIMEX ENTREGAS E MOVIMENTACAO DE CARGAS LTDA", "SIX TRACKS LTDA", "SOBERANO LOJAS DE CONVENIENCIA LTDA", "SOBERANO LUBRIFICANTES LTDA", "SOBERANO SERVICOS LTDA", "SOBERANO TRANSPORTES LTDA", "SOLAR VOLT SOLUCOES COMERCIO E INSTALACAO PARA ENERGIA LTDA", "SOLUCAO CORTE E DOBRA DE METAIS LTDA", "SOLVE OPERACAO, MANUTENCAO E COMISSIONAMENTO DE SISTEMAS FOTOVOLTAICOS LTDA", "SOLVIA SOLUCOES VIARIAS LTDA", "SP IMPORTS LTDA", "SP IMPORTS LTDA FILIAL 02-70", "SP IMPORTS LTDA FILIAL 03-50", "SPE JARDINS DOS BURITIS LTDA", "SPE JARDINS DOS BURITIS LTDA FILIAL 02-60", "SPE LA BRESSE LTDA", "SPE MARIA FAUSTINA LTDA", "SPE MIRANTE DO LAGO SETE LAGOAS LTDA", "SSME EMPREENDIMENTOS IMOBILIARIOS LTDA FILIAL 02-76", "SSME EMPREENDIMENTOS IMOBILIARIOS LTDA", "SSME FLORESTAL LTDA", "SSME FLORESTAL LTDA FILIAL 0002-43", "SSME FLORESTAL LTDA FILIAL 0003-24", "SSME FLORESTAL LTDA FILIAL 0005-96", "SSME FLORESTAL LTDA FILIAL 0006-77", "SSME FLORESTAL LTDA FILIAL 0007-58", "SSME FLORESTAL LTDA FILIAL 0008-39", "SSME FLORESTAL LTDA FILIAL 0009-10", "SSME FLORESTAL LTDA FILIAL 0010-53", "SSME FLORESTAL LTDA FILIAL 0011-34", "SSME FLORESTAL LTDA FILIAL 0012-15", "SSME FLORESTAL LTDA FILIAL 0013-04", "SSME FLORESTAL LTDA FILIAL 0014-87", "SUDESTE ADMINISTRADORA DE SERVICOS LTDA", "SUDESTE ENGENHARIA E COMERCIO LTDA", "SUDESTE ENGENHARIA E COMERCIO LTDA FILIAL 02-39", "SUDESTE ENGENHARIA E COMERCIO LTDA FILIAL 03-10", "SUDESTE PARTICIPACOES LTDA", "SV LOGISTICA LTDA", "SV RANCHO VELHO GERACAO DE ENERGIA SPE LTDA", "SWA PARTICIPACOES LTDA", "SWA PATRIMONIAL LTDA", "TAKONO DISTRIBUICAO LTDA", "TASK SOFTWARE LTDA", "TAX CLOUD SOLUCOES LTDA", "TCX COMERCIO E INDUSTRIA DE EQUIPAMENTOS PECAS E SERVICOS LTDA", "TEBAS ADMINISTRACAO LTDA", "TECHNEACO ENGENHARIA LTDA", "TECHNEACO ENGENHARIA LTDA FILIAL 02-58", "TECNOCAP RECAPAGEM E PNEUS LTDA", "THAISSA CALAB CURSOS LTDA", "THAISSA CALAB ODONTOLOGIA LTDA", "TIMBIRAS PARTICIPACOES LTDA", "TK LOCACAO DE EQUIPAMENTOS LTDA", "TK PATRIMONIAL LTDA", "TLUANER PARTICIPACOES S/A", "TMJ MARCA E PATENTE LTDA", "TOP RAJA CAR LOCADORA DE VEICULOS LTDA", "TOPAZIO IMPERIAL MINERACAO COMERCIO E INDUSTRIA LTDA", "TRANSPORTADORA DONIZETE LTDA", "TRANSPORTES BOA VISTA LOGISTICA LTDA", "TRIACO ESTRUTURAS METALICAS LTDA", "TURMALINA INCORPORACOES SPE LTDA", "USA DIAGNOSTICA LTDA", "VALADAO E SANTOS PARTICIPACOES LTDA", "VALUMA COBRANCA E NEGOCIOS LTDA", "VCA COMERCIO LTDA", "VCS COMERCIO LTDA", "VEIGA ESTRUTURAS METALICAS LTDA", "VENETO EMPREENDIMENTO COMERCIAL LTDA", "VEREDAS DA SERRA COMBUSTIVEL LTDA", "VERO LATTE COMERCIO DE ALIMENTOS LTDA", "VERO LATTE COMERCIO DE ALIMENTOS LTDA 03", "VERO LATTE COMERCIO DE ALIMENTOS LTDA 04", "VIA MONDO APS LTDA", "VIA MONDO APS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA", "VIA MONDO AUTOMOVEIS E PECAS LTDA - FILIAL 08-80", "VIA MONDO AUTOMOVEIS E PECAS LTDA - FILIAL 09-61", "VIA MONDO DISTRIBUIDORA DE PECAS E ACESSORIOS AUTOMOTIVOS LTDA",  "VIA MONDO DISTRIBUIDORA DE PECAS E ACESSORIOS AUTOMOTIVOS LTDA", "VIA MONDO FANDI LTDA", "VIA MONDO LOCADORA LTDA", "VIA MONDO LOCADORA LTDA FILIAL 02-94", "VIA MONDO LOCADORA LTDA FILIAL 03-75", "VIA MONDO MULTIMARCAS LTDA", "VIA MONDO TRANSPORTES LTDA", "VIEIRA ADMINISTRACAO LTDA", "VILA CLARA VITORIA LTDA", "VJ PARTICIPACOES LTDA", "VJ PATRIMONIAL LTDA", "VN EMPREENDIMENTOS LTDA", "VSX VALVULAS E EQUIPAMENTOS LTDA", "WOLF PARTICIPACOES S/A", "WRN PARTICIPACOES LTDA", "ZOX GESTAO PATRIMONIAL LTDA"]

# Lista de funcionalidades disponíveis
lista_funcionalidades = ["Página Inicial" , "Chat" , "Organizar Arquivos Fiscais", "Controle Importação", "Registros Importação", "Indicadores", "Configurações"]

# Função para hashear senha
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Dados do usuário padrão
username = "JHENNIFER"
nova_senha = hash_password("Refinnehj262")
novas_empresas = ",".join(lista_empresas)
novas_permissoes = ",".join(lista_funcionalidades)

# Verificar se o usuário existe e criar/atualizar se necessário
response = supabase.table("users").select("*").eq("username", username).execute()

if not response or (hasattr(response, 'error') and response.error):
    st.error(f"❌ Erro ao verificar usuário: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
else:
    if not response.data:
        # Usuário não encontrado, criar um novo
        insert_response = supabase.table("users").insert({
            "username": username,
            "password": nova_senha,
            "empresas": novas_empresas,
            "permissoes": novas_permissoes
        }).execute()
        
        if not insert_response or (hasattr(insert_response, 'error') and insert_response.error):
            st.error(f"❌ Erro ao criar usuário: {insert_response.error.message if hasattr(insert_response, 'error') else 'Erro desconhecido'}")
        else:
            print("Novo usuário criado com sucesso!")
    else:
        # Usuário encontrado, atualizar dados
        update_response = supabase.table("users").update({
            "empresas": novas_empresas,
            "permissoes": novas_permissoes
        }).eq("username", username).execute()
        
        if not update_response or (hasattr(update_response, 'error') and update_response.error):
            st.error(f"❌ Erro ao atualizar usuário: {update_response.error.message if hasattr(update_response, 'error') else 'Erro desconhecido'}")
        else:
            print("Usuário atualizado com sucesso!")

def check_and_add_columns():
    response = supabase.rpc("get_columns", {"p_table_name": "users"}).execute()
    columns = [col["column_name"] for col in response.data]

    # Adicionar colunas se não existirem
    if "empresas" not in columns:
        supabase.postgrest.rpc("alter_p_table_name_users", {"query": "ALTER p_table_name users ADD COLUMN empresas TEXT DEFAULT ''"}).execute()
    if "permissoes" not in columns:
        supabase.postgrest.rpc("alter_p_table_name_users", {"query": "ALTER p_table_name users ADD COLUMN permissoes TEXT DEFAULT ''"}).execute()

# Executar verificação e alteração
check_and_add_columns()
print("Verificação concluída!")

# Função para adicionar ou atualizar usuário no Supabase
def save_user(username, password, empresas, permissoes):
    try:
        hashed_password = hash_password(password) if password else None

        # Verifica se o usuário já existe no banco de dados
        response = supabase.table("users").select("username").eq("username", username).execute()

        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao buscar usuário: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return False

        user_exists = bool(response.data)  # Se houver dados, o usuário já existe

        if user_exists:
            update_data = {"empresas": empresas, "permissoes": permissoes}
            if password:
                update_data["password"] = hashed_password

            update_response = supabase.table("users").update(update_data).eq("username", username).execute()

            if not update_response or (hasattr(update_response, 'error') and update_response.error):
                st.error(f"❌ Erro ao atualizar usuário: {update_response.error.message if hasattr(update_response, 'error') else 'Erro desconhecido'}")
                return False

        else:
            insert_response = supabase.table("users").insert({
                "username": username,
                "password": hashed_password,
                "empresas": empresas,
                "permissoes": permissoes
            }).execute()

            if not insert_response or (hasattr(insert_response, 'error') and insert_response.error):
                st.error(f"❌ Erro ao inserir usuário: {insert_response.error.message if hasattr(insert_response, 'error') else 'Erro desconhecido'}")
                return False

        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar usuário: {str(e)}")
        return False

# Função para excluir usuário do Supabase
def delete_user(username):
    try:
        delete_response = supabase.table("users").delete().eq("username", username).execute()

        if not delete_response or (hasattr(delete_response, 'error') and delete_response.error):
            st.error(f"❌ Erro ao excluir usuário: {delete_response.error.message if hasattr(delete_response, 'error') else 'Erro desconhecido'}")
            return False

        return True
    except Exception as e:
        st.error(f"❌ Erro ao excluir usuário: {str(e)}")
        return False

# Função para validar login no Supabase
def validate_login(username, password):
    try:
        response = supabase.table("users").select("password, empresas, permissoes").eq("username", username).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao validar login: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return False

        user_data = response.data[0] if response.data else None

        if user_data and user_data["password"] == hash_password(password):
            st.session_state.empresas = user_data["empresas"].split(",") if user_data["empresas"] else []
            st.session_state.permissoes = user_data["permissoes"].split(",") if user_data["permissoes"] else []
            return True

        return False
    except Exception as e:
        st.error(f"❌ Erro ao validar login: {str(e)}")
        return False

# Tela de Login
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("Login")
    username = st.text_input("Nome de usuário")
    password = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if validate_login(username, password):
            st.session_state.logged_in = True
            st.session_state.username = username
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos!")
    st.stop()

# Menu restrito conforme permissões do usuário
menu_options = [option for option in lista_funcionalidades if option in st.session_state.permissoes]
menu = st.sidebar.selectbox("Escolha a funcionalidade", menu_options)

if menu == "Configurações":
    st.title("Configurações - Gerenciamento de Usuários")
    
    # Verificar se o usuário atual é o administrador
    if st.session_state.username != "JHENNIFER":
        st.error("Apenas o administrador pode acessar esta página.")
        st.stop()
    
    with st.form("add_user_form"):
        new_username = st.text_input("Usuário")
        new_password = st.text_input("Senha (deixe em branco para não alterar)", type="password")
        
        # Adicionar checkbox para selecionar todas as empresas
        todas_empresas = st.checkbox("Selecionar todas as empresas")
        if todas_empresas:
            empresas_selecionadas = lista_empresas
        else:
            empresas_selecionadas = st.multiselect("Empresas associadas", lista_empresas)
            
        permissoes_selecionadas = st.multiselect("Funcionalidades permitidas", lista_funcionalidades)
        empresas_str = ",".join(empresas_selecionadas)
        permissoes_str = ",".join(permissoes_selecionadas)
        save_button = st.form_submit_button("Salvar Usuário")
        
        if save_button and new_username:
            if save_user(new_username, new_password, empresas_str, permissoes_str):
                st.success("Usuário salvo com sucesso!")
                st.rerun()
            else:
                st.error("Erro ao salvar usuário!")
    
    # Exibir usuários cadastrados
    st.subheader("Usuários Cadastrados")

    # Buscar usuários no Supabase
    response = supabase.table("users").select("username, empresas, permissoes").execute()
    users = response.data

    # Exibir usuários em um formato mais compacto
    for user in users:
        with st.expander(f"👤 {user['username']}", expanded=False):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write("**Empresas:**")
                empresas_list = user['empresas'].split(',') if user['empresas'] else []
                st.write(f"{len(empresas_list)} empresas associadas")
                
                st.write("**Permissões:**")
                permissoes_list = user['permissoes'].split(',') if user['permissoes'] else []
                st.write(f"{len(permissoes_list)} permissões")
            
            with col2:
                if st.button("✏️ Editar", key=f"edit_{user['username']}"):
                    st.session_state.editing_user = user
                    st.rerun()
                
                if st.button("🗑️ Remover", key=f"del_{user['username']}"):
                    if delete_user(user['username']):
                        st.success(f"Usuário {user['username']} removido com sucesso!")
                        st.rerun()

    # Formulário de edição
    if "editing_user" in st.session_state:
        st.subheader("✏️ Editar Usuário")
        user = st.session_state.editing_user
        
        with st.form("edit_user_form"):
            edit_username = st.text_input("Usuário", value=user['username'], disabled=True)
            edit_password = st.text_input("Nova Senha (deixe em branco para não alterar)", type="password")
            
            # Adicionar checkbox para selecionar todas as empresas
            todas_empresas = st.checkbox("Selecionar todas as empresas")
            if todas_empresas:
                empresas_selecionadas = lista_empresas
            else:
                empresas_selecionadas = st.multiselect("Empresas associadas", lista_empresas, default=user['empresas'].split(',') if user['empresas'] else [])
                
            permissoes_selecionadas = st.multiselect("Funcionalidades permitidas", lista_funcionalidades, default=user['permissoes'].split(',') if user['permissoes'] else [])
            
            col1, col2 = st.columns(2)
            with col1:
                if st.form_submit_button("💾 Salvar Alterações"):
                    empresas_str = ",".join(empresas_selecionadas)
                    permissoes_str = ",".join(permissoes_selecionadas)
                    if save_user(edit_username, edit_password, empresas_str, permissoes_str):
                        st.success("Usuário atualizado com sucesso!")
                        del st.session_state.editing_user
                        st.rerun()
                    else:
                        st.error("Erro ao atualizar usuário!")
            
            with col2:
                if st.form_submit_button("❌ Cancelar"):
                    del st.session_state.editing_user
                    st.rerun()

#Organizador de Arquivos Fiscais
def create_bucket_if_not_exists():
    try:
        # Try to list buckets to check if 'arquivos' exists
        buckets = supabase.storage.list_buckets()
        bucket_exists = False
        
        for bucket in buckets:
            if isinstance(bucket, dict) and bucket.get('name') == 'arquivos':
                bucket_exists = True
                break
        
        if not bucket_exists:
            st.error("❌ O bucket 'arquivos' não existe. Por favor, crie o bucket manualmente no Supabase.")
            return False
            
        return True
            
    except Exception as e:
        st.error(f"❌ Erro ao verificar bucket: {str(e)}")
        return False

def salvar_arquivo(arquivo, pasta_destino):
    try:
        # Criar um nome único para o arquivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{timestamp}_{arquivo.name}"
        
        # Criar diretório temporário se não existir
        temp_dir = os.path.join(os.getcwd(), "temp_uploads")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Salvar arquivo localmente
        temp_path = os.path.join(temp_dir, nome_arquivo)
        with open(temp_path, "wb") as f:
            f.write(arquivo.getvalue())
        
        return temp_path
            
    except Exception as e:
        st.error(f"❌ Erro ao processar arquivo: {str(e)}")
        return None

def extrair_zip(arquivo_zip, pasta_destino):
    try:
        # Criar diretório temporário
        temp_dir = os.path.join(os.getcwd(), "temp_uploads")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Salvar o arquivo ZIP temporariamente
        zip_path = os.path.join(temp_dir, arquivo_zip.name)
        with open(zip_path, "wb") as f:
            f.write(arquivo_zip.getvalue())
        
        # Extrair o ZIP
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Limpar arquivo ZIP temporário
        os.remove(zip_path)
        
        return extract_dir
        
    except Exception as e:
        st.error(f"❌ Erro ao extrair arquivo ZIP: {str(e)}")
        return None

def verificar_arquivo(arquivo):
    try:
        if arquivo.name.endswith(".xml") or arquivo.name.endswith(".txt"):
            conteudo = arquivo.read()
            if not conteudo.strip():
                return False
        return True
    except Exception as e:
        return False

def verificar_conteudo_arquivo(conteudo):
    try:
        # Converter para string se for bytes
        if isinstance(conteudo, bytes):
            conteudo = conteudo.decode('utf-8', errors='ignore')
        
        # Verificar tipo de documento
        if "NFe" in conteudo:
            return "NFE"
        elif "CTe" in conteudo:
            return "CTE"
        elif "NFCe" in conteudo:
            return "NFCE"
        elif "SPED" in conteudo or "|C100|" in conteudo or "|C170|" in conteudo:
            return "SPED"
        elif "NFSe" in conteudo or "nfse" in conteudo.lower():
            return "NFS"
        elif any(ext in conteudo.lower() for ext in [".xls", ".xlsx", "csv"]):
            return "PLANILHA"
        else:
            return "OUTROS"
    except Exception:
        return "OUTROS"

def processar_arquivos(uploaded_files, nome_empresa):
    if not nome_empresa:
        st.error("Por favor, digite o nome da empresa antes de processar os arquivos.")
        return
    
    try:
        # Criar diretório temporário
        temp_dir = os.path.join(os.getcwd(), "temp_uploads")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Criar pasta da empresa
        pasta_empresa = os.path.join(temp_dir, nome_empresa)
        os.makedirs(pasta_empresa, exist_ok=True)
        
        for arquivo in uploaded_files:
            try:
                # Ler o conteúdo do arquivo
                conteudo = arquivo.read()
                
                if arquivo.name.endswith(".zip"):
                    # Criar arquivo ZIP temporário
                    zip_path = os.path.join(temp_dir, arquivo.name)
                    with open(zip_path, "wb") as f:
                        f.write(conteudo)
                    
                    # Extrair ZIP
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        # Processar cada arquivo dentro do ZIP
                        for zip_info in zip_ref.filelist:
                            if not zip_info.filename.endswith('/'):  # Ignorar diretórios
                                try:
                                    # Ler o conteúdo do arquivo do ZIP
                                    conteudo_zip = zip_ref.read(zip_info.filename)
                                    categoria = verificar_conteudo_arquivo(conteudo_zip)
                                    
                                    # Criar pasta da categoria
                                    pasta_categoria = os.path.join(pasta_empresa, categoria)
                                    os.makedirs(pasta_categoria, exist_ok=True)
                                    
                                    # Salvar arquivo
                                    nome_arquivo = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(zip_info.filename)}"
                                    caminho_arquivo = os.path.join(pasta_categoria, nome_arquivo)
                                    
                                    with open(caminho_arquivo, "wb") as f:
                                        f.write(conteudo_zip)
                                except Exception as e:
                                    continue
                    
                    # Remover ZIP temporário
                    os.remove(zip_path)
                else:
                    # Salvar arquivo individual
                    categoria = verificar_conteudo_arquivo(conteudo)
                    pasta_categoria = os.path.join(pasta_empresa, categoria)
                    os.makedirs(pasta_categoria, exist_ok=True)
                    
                    nome_arquivo = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{arquivo.name}"
                    caminho_arquivo = os.path.join(pasta_categoria, nome_arquivo)
                    
                    with open(caminho_arquivo, "wb") as f:
                        f.write(conteudo)
                
            except Exception as e:
                continue
        
        # Criar ZIP da pasta da empresa
        zip_path = os.path.join(temp_dir, f"{nome_empresa}.zip")
        
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(pasta_empresa):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, pasta_empresa)
                    zipf.write(file_path, arcname)
        
        # Fornecer o arquivo ZIP para download
        with open(zip_path, "rb") as f:
            st.download_button(
                "Baixar Arquivos Organizados",
                f.read(),
                f"{nome_empresa}.zip",
                "application/zip"
            )
        
        # Limpar arquivos temporários
        shutil.rmtree(pasta_empresa)
        os.remove(zip_path)
        
    except Exception as e:
        st.error("❌ Erro ao processar arquivos")

if menu == "Organizar Arquivos Fiscais":
    st.title("Organizador de Arquivos Fiscais")
    
    # Selecionar empresa
    nome_empresa = st.selectbox("Nome da Empresa", st.session_state.empresas)
    
    # Upload de arquivos
    uploaded_files = st.file_uploader(
        "Envie seus arquivos XML, TXT, ZIP ou Excel", 
        accept_multiple_files=True,
        key=f'uploaded_files_{nome_empresa}'
    )

    if uploaded_files and st.button("Processar Arquivos"):
        processar_arquivos(uploaded_files, nome_empresa)

elif menu == "Controle Importação":
    st.title("📑 Importação")
    
    # Verificar se o usuário está autenticado
    if not st.session_state.get("logged_in"):
        st.error("❌ Você precisa estar logado para registrar importações.")
        st.stop()
    
    with st.form("registro_form"):
        empresa_filtro = st.selectbox("Nome da empresa", st.session_state.empresas)
        tipo_nota = st.selectbox("Tipo de Nota", ["NFE entrada", "NFE saída", "CTE entrada", "CTE saída", "CTE cancelado", "SPED", "NFS tomado", "NFS prestado", "Planilha", "NFCE saída"])
        erro = st.text_area("Erro (se houver)")
        arquivo = st.file_uploader("Anexar arquivo", type=["png", "jpeg", "jpg", "pdf", "xml", "txt", "xlsx", "xls", "csv", "zip"])
        submit = st.form_submit_button("Registrar")
        
        if submit:
            try:
                data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                arquivo_path = ""
                upload_success = True
                
                if arquivo:
                    # Criar um nome único para o arquivo
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    nome_arquivo = f"{timestamp}_{arquivo.name}"
                    arquivo_path = f"arquivos/{empresa_filtro}/{nome_arquivo}"
                    
                    # Criar diretório temporário
                    temp_dir = os.path.join(os.getcwd(), "temp_uploads")
                    os.makedirs(temp_dir, exist_ok=True)
                    
                    # Salvar arquivo temporariamente
                    temp_path = os.path.join(temp_dir, nome_arquivo)
                    with open(temp_path, "wb") as f:
                        f.write(arquivo.getvalue())
                    
                    # Upload do arquivo para o Supabase Storage
                    try:
                        response = supabase.storage.from_("arquivos").upload(
                            arquivo_path,
                            arquivo.getvalue(),
                            {"content-type": arquivo.type}
                        )
                        
                        if response.error:
                            st.error(f"❌ Erro ao salvar arquivo: {response.error.message}")
                            upload_success = False
                    except Exception as e:
                        st.error(f"❌ Erro ao fazer upload do arquivo: {str(e)}")
                        upload_success = False
                    finally:
                        # Limpar arquivo temporário
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                
                if upload_success:
                    # Definir status baseado no erro
                    status = "OK" if not erro else "Pendente"
                    
                    # Inserir registro no Supabase
                    try:
                        registro_data = {
                            "data": data_atual,
                            "empresa": empresa_filtro,
                            "tipo_nota": tipo_nota,
                            "erro": erro,
                            "arquivo_erro": arquivo_path if erro else None,
                            "status": status,
                            "arquivo": arquivo_path if arquivo else None,
                            "tipo_arquivo": arquivo.type if arquivo else None
                        }
                        
                        # Tentar inserir o registro diretamente
                        response = supabase.table("registros").insert(registro_data).execute()
                        
                        if not response or (hasattr(response, 'error') and response.error):
                            st.error(f"❌ Erro ao salvar registro: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
                        else:
                            st.success("✅ Registro salvo com sucesso!")
                            st.rerun()
                            
                    except Exception as e:
                        st.error(f"❌ Erro ao salvar registro: {str(e)}")
                    
            except Exception as e:
                st.error(f"❌ Erro ao processar registro: {str(e)}")

elif menu == "Registros Importação":
    st.title("🔍 Buscar Registros")

    if not st.session_state.empresas:
        st.warning("⚠ Você não tem permissão para acessar registros de nenhuma empresa.")
        st.stop()

    empresa_filtro = st.selectbox("Nome da empresa", st.session_state.empresas)
    
    try:
        # Buscar registros diretamente do Supabase
        response = supabase.table("registros").select("*").eq("empresa", empresa_filtro).order("data", desc=True).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao buscar registros: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            st.stop()
        
        registros = pd.DataFrame(response.data)
        
        if not registros.empty:
            # Adicionar filtros
            col1, col2 = st.columns(2)
            with col1:
                status_filtro = st.selectbox("Status", ["Todos"] + list(registros["status"].unique()))
            with col2:
                tipo_nota_filtro = st.selectbox("Tipo de Nota", ["Todos"] + list(registros["tipo_nota"].unique()))

            # Aplicar filtros
            if status_filtro != "Todos":
                registros = registros[registros["status"] == status_filtro]
            if tipo_nota_filtro != "Todos":
                registros = registros[registros["tipo_nota"] == tipo_nota_filtro]

            # Exibir registros
            for index, row in registros.iterrows():
                with st.expander(f"📌 {row['empresa']} - {row['tipo_nota']} - {row['data']}"):
                    st.write(f"**Status:** {row['status']}")
                    st.write(f"**Erro:** {row['erro']}" if row['erro'] else "**Sem erro registrado.**")

                    # Exibir arquivo de erro se existir
                    if row['arquivo_erro']:
                        try:
                            response = supabase.storage.from_("arquivos").download(row['arquivo_erro'])
                            if response:
                                if row['arquivo_erro'].lower().endswith(('.png', '.jpg', '.jpeg')):
                                    image = Image.open(BytesIO(response))
                                    st.image(image, caption="Imagem do erro", use_container_width=True)
                                else:
                                    st.download_button(
                                        "Baixar arquivo de erro",
                                        response,
                                        os.path.basename(row['arquivo_erro']),
                                        "application/octet-stream"
                                    )
                            else:
                                st.warning("Arquivo não encontrado no armazenamento.")
                        except Exception as e:
                            st.error(f"❌ Erro ao carregar arquivo: {str(e)}")

                    # Exibir arquivo principal se existir
                    if row['arquivo']:
                        try:
                            response = supabase.storage.from_("arquivos").download(row['arquivo'])
                            if response:
                                st.download_button(
                                    "Baixar arquivo principal",
                                    response,
                                    os.path.basename(row['arquivo']),
                                    "application/octet-stream"
                                )
                            else:
                                st.warning("Arquivo principal não encontrado no armazenamento.")
                        except Exception as e:
                            st.error(f"❌ Erro ao carregar arquivo principal: {str(e)}")

                    if row['erro']:
                        email_cliente = st.text_input(f"Digite o e-mail do cliente para {row['empresa']}", key=f"email_{row['id']}")
                        erro_mensagem = f"Ocorreu um erro na importação do arquivo. Detalhes: {row['erro']}"
                        assunto = f"Erro na Importação de Arquivo - {row['empresa']}"

                        if email_cliente and st.button(f"🔔 Abrir e-mail para {email_cliente}", key=f"abrir_email_{row['id']}"):
                            open_outlook(email_cliente, assunto, erro_mensagem, row['arquivo_erro'])
                            st.success(f"📧 E-mail pronto para envio no Outlook! O anexo precisa ser adicionado manualmente.")

                    if row['status'] == "Pendente":
                        if st.button("✔ OK", key=f"resolver_{row['id']}"):
                            # Atualizar status diretamente
                            response = supabase.table("registros").update({"status": "Resolvido"}).eq("id", row['id']).execute()
                            if not response or (hasattr(response, 'error') and response.error):
                                st.error(f"❌ Erro ao atualizar status: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
                            else:
                                st.success("✅ Status atualizado com sucesso!")
                                st.rerun()
        else:
            st.info("ℹ️ Nenhum registro encontrado para a empresa selecionada.")
            
    except Exception as e:
        st.error(f"❌ Erro ao buscar registros: {str(e)}")

elif menu == "Indicadores":
    st.title("📈 Indicadores de Importação")

    # Verifica se há empresas disponíveis no estado da sessão
    if not st.session_state.empresas:
        st.warning("⚠ Você não tem permissão para acessar indicadores de nenhuma empresa.")
        st.stop()

    try:
        if not check_table_exists("registros"):
            st.warning("A tabela 'registros' ainda não foi criada no Supabase. Por favor, crie a tabela com as seguintes colunas: id, data, empresa, tipo_nota, erro, arquivo_erro, status")
            st.stop()

        # Filtra os registros apenas das empresas que o usuário tem acesso
        response = supabase.table("registros").select("*").in_("empresa", st.session_state.empresas).execute()

        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao buscar indicadores: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            st.stop()

        registros = pd.DataFrame(response.data)

        if not registros.empty:
            st.dataframe(registros)  # Exibe os dados como uma tabela interativa no Streamlit

            col1, col2 = st.columns(2)
            empresa_count = registros["empresa"].value_counts().reset_index()
            empresa_count.columns = ["Empresa", "Total de Registros"]
            fig1 = px.pie(empresa_count, names="Empresa", values="Total de Registros", title="📌 Registros por Empresa")
            col1.plotly_chart(fig1)
            
            tipo_nota_count = registros["tipo_nota"].value_counts().reset_index()
            tipo_nota_count.columns = ["Tipo de Nota", "Quantidade"]
            fig2 = px.pie(tipo_nota_count, names="Tipo de Nota", values="Quantidade", title="📌 Tipos de Nota Registradas")
            col2.plotly_chart(fig2)
            
            importadas = registros["empresa"].nunique()
            total_registros = len(registros)
            df_empresas = pd.DataFrame({"Status": ["Importadas", "Não Importadas"], "Quantidade": [importadas, total_registros - importadas]})
            fig3 = px.pie(df_empresas, names="Status", values="Quantidade", title="📌 Empresas Importadas vs. Não Importadas")
            st.plotly_chart(fig3)
            
            if registros["erro"].notna().sum() > 0:
                erro_count = registros["erro"].value_counts().reset_index().head(5)
                erro_count.columns = ["Erro", "Frequência"]
                st.subheader("🔴 Erros Mais Comuns")
                fig4 = px.pie(erro_count, names="Erro", values="Frequência", title="📌 Erros Mais Frequentes")
                st.plotly_chart(fig4)
        
            st.subheader("📥 Download de Registros")
            data_hoje = date.today().strftime("%d-%m-%Y")
            df_hoje = registros[registros['data'] == data_hoje]
            if not df_hoje.empty:
                csv = df_hoje.to_csv(index=False).encode("utf-8")
                st.download_button("📥 Baixar Planilha do Dia", data=csv, file_name=f"registros_{data_hoje}.csv", mime="text/csv")
            else:
                st.info("Nenhum registro encontrado para hoje.")
        else:
            st.info("ℹ️ Nenhum registro encontrado para as empresas selecionadas.")
    except Exception as e:
        st.error(f"❌ Erro ao carregar indicadores: {str(e)}")



# Função para carregar as mensagens do Supabase
def load_messages():
    try:
        # Verificar se a tabela messages existe
        if not check_table_exists("messages"):
            st.warning("A tabela 'messages' ainda não foi criada no Supabase. Por favor, crie a tabela com as seguintes colunas: id, username, message, created_at")
            return []

        response = supabase.table("messages").select("*").order("created_at", desc=True).limit(50).execute()
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao carregar mensagens: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return []
        return response.data
    except Exception as e:
        st.error(f"❌ Erro ao carregar mensagens: {str(e)}")
        return []

# Função para salvar as mensagens no Supabase
def save_message(username, message):
    try:
        # Verificar se a tabela messages existe
        if not check_table_exists("messages"):
            st.warning("A tabela 'messages' ainda não foi criada no Supabase. Por favor, crie a tabela com as seguintes colunas: id, username, message, created_at")
            return False

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        response = supabase.table("messages").insert({
            "username": username,
            "message": message,
            "created_at": timestamp
        }).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao salvar mensagem: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return False
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar mensagem: {str(e)}")
        return False

# Função para pegar o nome do usuário a partir do banco de dados (Supabase)
def get_user_name():
    try:
        response = supabase.table("users").select("username").eq("username", st.session_state.username).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao buscar usuário: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return "Usuário desconhecido"

        if response.data:
            return response.data[0]["username"]
        
        return "Usuário desconhecido"
    except Exception as e:
        st.error(f"❌ Erro ao buscar usuário: {str(e)}")
        return "Usuário desconhecido"

# Função para exibir o chat
if menu == "Chat":
    st.title("Chat Entre Usuários")

    # Carregar mensagens antigas
    messages = load_messages()

    # Área para exibir as mensagens
    chat_area = st.container()
    with chat_area:
        st.write("### Mensagens:")
        for msg in messages:
            try:
                # Formatar a data para exibição, removendo a parte do timezone
                msg_date = msg['created_at'].split('T')[0] + ' ' + msg['created_at'].split('T')[1].split('+')[0]
                msg_date = datetime.strptime(msg_date, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')
                # Exibir mensagem com o nome do usuário
                st.write(f"**{msg['username']}** ({msg_date}):")
                st.write(f"{msg['message']}")
                st.markdown("---")
            except Exception as e:
                st.error(f"❌ Erro ao exibir mensagem: {str(e)}")
                continue

    # Área fixa para digitar a mensagem
    input_area = st.container()
    with input_area:
        user_message = st.text_input("Digite sua mensagem:")

    # Enviar a mensagem
    if st.button("Enviar"):
        if user_message:
            if save_message(st.session_state.username, user_message):
                st.success("Mensagem enviada com sucesso!")
                st.rerun()
            else:
                st.error("Erro ao enviar mensagem!")

# Função para exibir a página inicial
if menu == "Página Inicial":
    st.title("Bem Vindo")
    st.write("Assista o tutorial de como utilizar nosso sistema.")
    # URL do vídeo que será exibido na página inicial
    video_url = "https://youtu.be/aP0sPUVjs40?si=n_lO5UnNNKR1mBwe"  # Substitua pelo link do seu vídeo

    # Exibe o vídeo diretamente na página inicial
    st.video(video_url)

def check_table_exists(table_name):
    try:
        response = supabase.table(table_name).select("*").limit(1).execute()
        return True
    except Exception as e:
        if "relation" in str(e) and "does not exist" in str(e):
            return False
        raise e

# Função para buscar registros
def buscar_registros(empresa_filtro=None):
    try:
        if not check_table_exists("registros"):
            st.warning("A tabela 'registros' ainda não foi criada no Supabase.")
            return pd.DataFrame()

        query = supabase.table("registros").select("*")
        
        if empresa_filtro:
            query = query.eq("empresa", empresa_filtro)
        
        response = query.order("data", desc=True).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao buscar registros: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return pd.DataFrame()
        
        return pd.DataFrame(response.data)
    except Exception as e:
        st.error(f"❌ Erro ao buscar registros: {str(e)}")
        return pd.DataFrame()

# Função para atualizar status no Supabase
def atualizar_status_resolvido(registro_id):
    try:
        if not check_table_exists("registros"):
            st.warning("A tabela 'registros' ainda não foi criada no Supabase.")
            return False

        response = supabase.table("registros").update({"status": "Resolvido"}).eq("id", registro_id).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            st.error(f"❌ Erro ao atualizar status: {response.error.message if hasattr(response, 'error') else 'Erro desconhecido'}")
            return False
        
        st.success(f"✅ Status do registro {registro_id} atualizado para 'Resolvido'!")
        return True
    except Exception as e:
        st.error(f"❌ Erro ao atualizar status: {str(e)}")
        return False

# Função para salvar registro no Supabase
def salvar_registro(data, empresa, tipo_nota, erro, arquivo_erro, status, arquivo=None, tipo_arquivo=None):
    try:
        # Verificar se o usuário está autenticado
        if not st.session_state.get("logged_in"):
            st.error("❌ Você precisa estar logado para salvar registros.")
            return False

        if not check_table_exists("registros"):
            st.warning("A tabela 'registros' ainda não foi criada no Supabase.")
            return False

        registro_data = {
            "data": data,
            "empresa": empresa,
            "tipo_nota": tipo_nota,
            "erro": erro,
            "arquivo_erro": arquivo_erro,
            "status": status,
            "arquivo": arquivo,
            "tipo_arquivo": tipo_arquivo
        }

        # Tentar inserir o registro
        response = supabase.table("registros").insert(registro_data).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            error_message = response.error.message if hasattr(response, 'error') else 'Erro desconhecido'
            st.error(f"❌ Erro ao salvar registro: {error_message}")
            
            # Se o erro for relacionado à política de segurança
            if hasattr(response, 'error') and response.error.code == '42501':
                st.warning("⚠️ Erro de permissão. Verifique se as políticas de segurança (RLS) estão configuradas corretamente.")
            
            return False
        
        st.success("✅ Registro salvo com sucesso!")
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar registro: {str(e)}")
        return False

# Função para inserir registro com verificação de autenticação
def inserir_registro_seguro(registro_data):
    try:
        # Verificar se o usuário está autenticado
        if not st.session_state.get("logged_in"):
            st.error("❌ Você precisa estar logado para registrar importações.")
            return False
            
        # Verificar se a empresa está nas permissões do usuário
        if registro_data["empresa"] not in st.session_state.empresas:
            st.error("❌ Você não tem permissão para registrar importações para esta empresa.")
            return False
            
        # Tentar inserir o registro
        response = supabase.table("registros").insert(registro_data).execute()
        
        if not response or (hasattr(response, 'error') and response.error):
            error_message = response.error.message if hasattr(response, 'error') else 'Erro desconhecido'
            st.error(f"❌ Erro ao salvar registro: {error_message}")
            return False
            
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar registro: {str(e)}")
        return False
