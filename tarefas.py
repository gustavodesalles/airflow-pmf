import json
from datetime import datetime, timedelta
import psycopg2

import requests


def get_dados_api():
    # hoje = datetime.today()
    hoje = datetime(2025, 9, 1)
    # primeiro_dia_mes_atual = hoje.replace(day=1)
    # ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
    # primeiro_dia_mes_passado = ultimo_dia_mes_passado.replace(day=1)
    # url_licitacoes = f'https://transparencia.e-publica.net:443/epublica-portal/rest/florianopolis/api/v1/licitacao?periodo_inicial={primeiro_dia_mes_passado.strftime("%d/%m/%Y")}&periodo_final={ultimo_dia_mes_passado.strftime("%d/%m/%Y")}'
    url_licitacoes = f'https://transparencia.e-publica.net:443/epublica-portal/rest/florianopolis/api/v1/licitacao?periodo_inicial={hoje.strftime("%d/%m/%Y")}&periodo_final={hoje.strftime("%d/%m/%Y")}'
    
    response = requests.get(url_licitacoes)
    dados = response.json()
    return dados['registros']

def get_dados_internos():
    # hoje = datetime.today()
    hoje = datetime(2025, 9, 1)
    # primeiro_dia_mes_atual = hoje.replace(day=1)
    # ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
    # primeiro_dia_mes_passado = ultimo_dia_mes_passado.replace(day=1)
    url_get_licitacoes = 'https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/compras/licitacao/listAll?ano=2025&entidade=2002'
    url_licitacao_individual = 'https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/compras/licitacao/form?ano=2025&entidade=2002'
    registros = []
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
    }

    payload_data = {}
    with open('payload.json', 'r') as f:
        payload_data = json.load(f)
        # payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['valueCompare'] = ultimo_dia_mes_passado.strftime("%Y-%m-%d")
        # payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['value'] = primeiro_dia_mes_passado.strftime("%Y-%m-%d")
        payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['valueCompare'] = hoje.strftime("%Y-%m-%d")
        payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['value'] = hoje.strftime("%Y-%m-%d")
        payload_data['pagination']['count'] = 1000

    dados2 = requests.post(url_get_licitacoes, json=payload_data)
    dados = dados2.json()

    ids_licitacao = []
    for item in dados['rows']:
        id_licitacao = item['id']
        ids_licitacao.append(id_licitacao)

    for id_licitacao in ids_licitacao:
        payload = json.dumps({
            "id": id_licitacao,
            "mode": "INFO"
        })

        retry = True
        while retry:
            response = requests.post(url_licitacao_individual, headers=headers, data=payload)
            if response.status_code == 200:
                retry = False
                registro = response.json()['pojo']
                registros.append(registro)
                # print(id_licitacao)
    return registros

def juntar_dados_licitacao():
    dados_api = get_dados_api()
    dados_internos = get_dados_internos()

    # try:
    #     conn = psycopg2.connect(
    #         host="localhost",
    #         database="test",
    #         user="airflow",
    #         password="airflow",
    #         port="5432"
    #     )
    #     print("Conexão bem-sucedida ao banco de dados PostgreSQL")
    # except Exception as e:
    #     print(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")
    
    # cursor = conn.cursor()
    for registro in dados_api:
        licitacao = registro['registro']['licitacao']
        licitacao_interno = next((item for item in dados_internos if item['numero'] == licitacao['numero']), None)

        numero = licitacao.get('numero', None)
        modalidade = licitacao.get('modalidade', None)
        valor_estimado = licitacao.get('valorEstimado', None)
        data_abertura = licitacao.get('aberturaData', None)
        data_emissao = licitacao.get('dataEmissao', None)
        objeto_resumido = licitacao.get('objetoResumido', None)
        finalidade = licitacao.get('finalidade', None)
        forma_julgamento = licitacao.get('formaJulgamento', None)

        nome_advogado = registro['registro']['advogado'].get('nome', None)
        # inserir advogado
        # inserir licitação

        for ug in registro['registro']['listUnidadesGestoras']:
            codigo_ug = ug.get('codigo', None)
            #inserir relação licitacao_unidade_gestora
        
        for item in registro['registro']['listItens']:
            numero_item = item.get('numero', None)
            denominacao_item = item.get('denominacao', None)
            quantidade_item = item.get('quantidade', None)
            unidade_medida_item = item.get('unidadeMedida', None)
            valor_unitario_item = item.get('valorUnitarioEstimado', None)
            situacao_item = item.get('situacao', None)
            # inserir itens
            for vencedor in item['listVencedores']:
                # obter fornecedor e verificar se existe na tabela 'fornecedor'
                quantidade_vencedor = vencedor.get('quantidade', None)
                valor_unitario_vencedor = vencedor.get('valorUnitario', None)
                situacao_vencedor = vencedor.get('situacao', None)
                # inserir vencedores
            
        for texto in registro['registro']['listTextos']:
            denominacao_texto = texto.get('denominacao', None)
            if licitacao_interno and 'textos' in licitacao_interno:
                texto_interno = next((t for t in licitacao_interno['textos'] if t['nome'] == denominacao_texto), None)
                url_texto = "https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/licitacao/texto/download/public?ano=2025&entidade=2002"
                if texto_interno:
                    requisicao = requests.post(url_texto, json=texto_interno)
                    if requisicao.status_code == 200:
                        link_texto = requisicao.json()['id']
                    else:
                        link_texto = None
            # inserir textos
        
        for contrato in licitacao_interno['contratos']:
            # update contrato; colocar id da licitação
        
        for empenho in licitacao_interno['empenhos']:
            # inserir empenho
            
    # cursor.execute("DROP TABLE IF EXISTS public.dag_runs;")

    # conn.commit()
    # conn.close()
    # Aqui você pode implementar a lógica para juntar os dois conjuntos de dados conforme necessário
    # inserir advogado
    # inserir licitação
    # inserir contrato
    # inserir empenho
    # inserir itens
    # inserir vencedores
    # inserir textos
    # inserir registros licitacao_unidade_gestora
    # return dados_api, dados_internos

# get_dados_api()

# get_dados_internos()
juntar_dados_licitacao()