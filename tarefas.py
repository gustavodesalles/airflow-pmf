import json
from datetime import datetime, timedelta

import requests


def get_dados_api():
    hoje = datetime.today()
    primeiro_dia_mes_atual = hoje.replace(day=1)
    ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
    primeiro_dia_mes_passado = ultimo_dia_mes_passado.replace(day=1)
    url_licitacoes = f'https://transparencia.e-publica.net:443/epublica-portal/rest/florianopolis/api/v1/licitacao?periodo_inicial={primeiro_dia_mes_passado.strftime("%d/%m/%Y")}&periodo_final={ultimo_dia_mes_passado.strftime("%d/%m/%Y")}'

    response = requests.get(url_licitacoes)
    dados = response.json()
    return dados['registros']

def get_dados_internos():
    # hoje = datetime.today()
    hoje = datetime(2025, 3, 13)
    primeiro_dia_mes_atual = hoje.replace(day=1)
    ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
    primeiro_dia_mes_passado = ultimo_dia_mes_passado.replace(day=1)
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
        payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['valueCompare'] = ultimo_dia_mes_passado.strftime("%Y-%m-%d")
        payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['value'] = primeiro_dia_mes_passado.strftime("%Y-%m-%d")
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
                print(id_licitacao)
    return registros

# get_dados_api()

get_dados_internos()