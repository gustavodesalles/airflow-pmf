from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import requests, json, os

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

@task
def get_dados_api(is_licitacao=True):
    # hoje = datetime.today()
    hoje = datetime(2025, 9, 1)
    # primeiro_dia_mes_atual = hoje.replace(day=1)
    # ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
    # primeiro_dia_mes_passado = ultimo_dia_mes_passado.replace(day=1)
    # url_licitacoes = f'https://transparencia.e-publica.net:443/epublica-portal/rest/florianopolis/api/v1/licitacao?periodo_inicial={primeiro_dia_mes_passado.strftime("%d/%m/%Y")}&periodo_final={ultimo_dia_mes_passado.strftime("%d/%m/%Y")}'
    url_licitacoes = f'https://transparencia.e-publica.net:443/epublica-portal/rest/florianopolis/api/v1/licitacao?periodo_inicial={hoje.strftime("%d/%m/%Y")}&periodo_final={hoje.strftime("%d/%m/%Y")}'
    url_contratos = f'https://transparencia.e-publica.net:443/epublica-portal/rest/florianopolis/api/v1/contrato?periodo_inicial={hoje.strftime("%d/%m/%Y")}&periodo_final={hoje.strftime("%d/%m/%Y")}'
    response = requests.get(url_licitacoes) if is_licitacao else requests.get(url_contratos)
    dados = response.json()
    return dados['registros']

@task
def get_dados_internos(is_licitacao=True):
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

    base_dir = os.path.dirname(os.path.abspath(__file__))
    payload_path = os.path.join(base_dir, 'sql', 'payload.json')

    payload_data = {}
    with open(payload_path, 'r') as f:
        payload_data = json.load(f)
        # payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['valueCompare'] = ultimo_dia_mes_passado.strftime("%Y-%m-%d")
        # payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['value'] = primeiro_dia_mes_passado.strftime("%Y-%m-%d")
        if is_licitacao:
            payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['valueCompare'] = hoje.strftime("%Y-%m-%d")
            payload_data['searchBean']['searchProperties']['Filtrar porlicitacao.dataEmissao']['value'] = hoje.strftime("%Y-%m-%d")
        else:
            payload_data['searchBean']['searchProperties']['Filtrar porcontrato.assinatura']['valueCompare'] = hoje.strftime("%Y-%m-%d")
            payload_data['searchBean']['searchProperties']['Filtrar porcontrato.assinatura']['value'] = hoje.strftime("%Y-%m-%d")
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
    return registros

with DAG(
    dag_id='dag_compras_publicas_v2',
    default_args=default_args,
    start_date=datetime(2025, 3, 13),
    schedule="@daily",
    catchup=False
) as dag:
    task_criar_tabelas = SQLExecuteQueryOperator(
        task_id='criar_tabelas',
        conn_id='postgres_localhost',
        sql='/sql/criar_tabelas.sql'
    )
    task_inserir_unidades_gestoras = SQLExecuteQueryOperator(
        task_id='inserir_unidades_gestoras',
        conn_id='postgres_localhost',
        sql='/sql/inserir_unidades_gestoras.sql'
    )
    dados_api_licitacoes = get_dados_api()
    dados_internos_licitacoes = get_dados_internos()
    print(dados_api_licitacoes)
    print(dados_internos_licitacoes)

    task_criar_tabelas >> task_inserir_unidades_gestoras >> [dados_api_licitacoes, dados_internos_licitacoes]