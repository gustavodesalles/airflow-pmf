import json
from datetime import datetime, timedelta
import psycopg2

import requests


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

def get_dados_internos(is_licitacao=True):
    # hoje = datetime.today()
    hoje = datetime(2025, 9, 1)
    # hoje = datetime(2025, 7, 17)
    # primeiro_dia_mes_atual = hoje.replace(day=1)
    # ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
    # primeiro_dia_mes_passado = ultimo_dia_mes_passado.replace(day=1)
    url_get_licitacoes = 'https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/compras/licitacao/listAll?ano=2025&entidade=2002'
    url_licitacao_individual = 'https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/compras/licitacao/form?ano=2025&entidade=2002'
    
    url_get_contratos = 'https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/compras/contrato/listAll?ano=2025&entidade=2002'
    url_contrato_individual = 'https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/compras/contrato/form?ano=2025&entidade=2002'

    registros = []
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
    }

    payload_data = {}
    with open('payload.json' if is_licitacao else 'payload_contrato.json', 'r') as f:
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

    if is_licitacao:
        dados2 = requests.post(url_get_licitacoes, json=payload_data)
    else:
        dados2 = requests.post(url_get_contratos, json=payload_data)
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
            if is_licitacao:
                response = requests.post(url_licitacao_individual, headers=headers, data=payload)
            else:
                response = requests.post(url_contrato_individual, headers=headers, data=payload)
            if response.status_code == 200:
                retry = False
                registro = response.json()['pojo']
                registros.append(registro)
    return registros

def juntar_dados_contrato():
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host="localhost",
            database="test",
            user="airflow",
            password="airflow",
            port="5432"
        )
        print("Conexão bem-sucedida ao banco de dados PostgreSQL")
        
        cursor = conn.cursor()

        # Fetch data from APIs
        dados_api = get_dados_api(False)
        dados_internos = get_dados_internos(False)

        for registro in dados_api:
            contrato = registro['registro']['contrato']
            contrato_interno = next((item for item in dados_internos if item['numero'] == contrato['numero']), None)

            numero = contrato.get('numero', None)
            assinatura = contrato.get('assinatura', None)
            inicio_vigencia = contrato.get('inicioVigencia', None)
            vencimento = contrato.get('vencimento', None)
            valor_total = contrato.get('valorTotal', None)
            objeto_resumido = contrato.get('objetoResumido', None)

            if contrato_interno:
                nome_fornecedor = contrato_interno.get('fornecedorNome', None)
                # Insert or update fornecedor
                cursor.execute(
                    """
                    INSERT INTO Fornecedor (nome)
                    VALUES (%s)
                    ON CONFLICT (nome) DO UPDATE
                    SET nome = EXCLUDED.nome
                    RETURNING id_fornecedor;
                    """,
                    (nome_fornecedor,)
                )
                id_fornecedor = cursor.fetchone()[0]

                numero_licitacao = contrato_interno.get('licitacao', None)
                id_licitacao = None
                cursor.execute(
                    "SELECT id_licitacao FROM Licitacao WHERE numero_licitacao = %s;",
                    (numero_licitacao,)
                )
                result = cursor.fetchone()
                if result:
                    id_licitacao = result[0]

                codigo_unidade_gestora = None
                nome_unidade_gestora = contrato_interno.get('unidadeGestora', None)
                if nome_unidade_gestora:
                    nome_unidade_gestora = nome_unidade_gestora.rstrip()
                    cursor.execute(
                        "SELECT codigo_unidade_gestora FROM Unidade_Gestora WHERE denominacao ILIKE %s LIMIT 1;",
                        (nome_unidade_gestora,)
                    )
                    codigo_unidade_gestora = cursor.fetchone()[0]

                # Insert or update contrato
                cursor.execute(
                    """
                    INSERT INTO Contrato (
                        numero_contrato, assinatura, inicio_vigencia, vencimento,
                        valor_total, objeto_resumido, codigo_unidade_gestora, id_fornecedor, id_licitacao
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (numero_contrato, codigo_unidade_gestora) DO UPDATE
                    SET 
                        assinatura = EXCLUDED.assinatura,
                        inicio_vigencia = EXCLUDED.inicio_vigencia,
                        vencimento = EXCLUDED.vencimento,
                        valor_total = EXCLUDED.valor_total,
                        objeto_resumido = EXCLUDED.objeto_resumido,
                        codigo_unidade_gestora = EXCLUDED.codigo_unidade_gestora,
                        id_fornecedor = EXCLUDED.id_fornecedor,
                        id_licitacao = EXCLUDED.id_licitacao
                    RETURNING id_contrato;
                    """,
                    (numero, assinatura, inicio_vigencia, vencimento, valor_total, objeto_resumido, codigo_unidade_gestora, id_fornecedor, id_licitacao)
                )
                id_contrato = cursor.fetchone()[0]

                # Process items
                for item in contrato_interno['itens']:
                    numero_item = item.get('numero', None)
                    denominacao_item = item.get('denominacao', None)
                    quantidade_item = item.get('quantidade', None)
                    unidade_medida_item = item.get('unidadeMedida', None)
                    valor_unitario_item = item.get('valorUnitarioEstimado', None)
                    valor_total_item = item.get('valorTotal', None)

                    cursor.execute(
                        """
                        INSERT INTO Item (
                            numero_item, denominacao, quantidade, unidade_medida,
                            valor_unitario_estimado, valor_total, id_contrato
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (numero_item, id_contrato) DO UPDATE
                        SET 
                            denominacao = EXCLUDED.denominacao,
                            quantidade = EXCLUDED.quantidade,
                            unidade_medida = EXCLUDED.unidade_medida,
                            valor_unitario_estimado = EXCLUDED.valor_unitario_estimado,
                            valor_total = EXCLUDED.valor_total
                        RETURNING id_item;
                        """,
                        (numero_item, denominacao_item, quantidade_item, unidade_medida_item, valor_unitario_item, valor_total_item, id_contrato)
                    )
                    id_item = cursor.fetchone()[0]

                # Process textos
                for texto in contrato_interno['textos']:
                    denominacao_texto = texto.get('tipo', None)
                    url_texto = "https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/contrato/texto/download/public?ano=2025&entidade=2002"
                    requisicao = requests.post(url_texto, json=texto)
                    if requisicao.status_code == 200:
                        link_texto = requisicao.json()['id']
                    else:
                        link_texto = None

                    cursor.execute(
                        """
                        INSERT INTO Texto (
                            id_contrato, denominacao, link
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT (id_contrato, denominacao) DO UPDATE
                        SET 
                            link = EXCLUDED.link
                        RETURNING id_texto;
                        """,
                        (id_contrato, denominacao_texto, link_texto)
                    )
                    id_texto = cursor.fetchone()[0]

                # Process empenhos
                for empenho in contrato_interno['empenhos']:
                    numero_empenho = empenho.get('numero', None)
                    valor_empenho = empenho.get('valorEmpenhado', None)
                    valor_pago = empenho.get('valorPago', None)
                    emissao_empenho = empenho.get('emissao', None)

                    cursor.execute(
                        "SELECT id_empenho FROM Empenho WHERE numero_empenho = %s AND id_contrato = %s;",
                        (numero_empenho, id_contrato)
                    )
                    result = cursor.fetchone()
                    if result:
                        id_empenho = result[0]
                        cursor.execute(
                            """
                            UPDATE Empenho
                            SET 
                                emissao = %s
                            WHERE id_empenho = %s;
                            """,
                            (emissao_empenho, id_empenho)
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT INTO Empenho (
                                id_contrato, numero_empenho, emissao
                            ) VALUES (%s, %s, %s)
                            RETURNING id_empenho;
                            """,
                            (id_contrato, numero_empenho, emissao_empenho)
                        )
                        id_empenho = cursor.fetchone()[0]

        conn.commit()  # Commit the transaction

    except Exception as e:
        print(f"Erro ao processar os dados: {e}")
        if conn:
            conn.rollback()  # Rollback the transaction in case of error
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def juntar_dados_licitacao():
    dados_api = get_dados_api()
    dados_internos = get_dados_internos()

    try:
        conn = psycopg2.connect(
            host="localhost",
            database="test",
            user="airflow",
            password="airflow",
            port="5432"
        )
        print("Conexão bem-sucedida ao banco de dados PostgreSQL")
        
        cursor = conn.cursor()
        
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

            nome_advogado = registro['registro']['advogado']['pessoa'].get('nome', None)
            cursor.execute(
                """
                INSERT INTO Advogado (nome)
                VALUES (%s)
                ON CONFLICT (nome) DO UPDATE
                SET nome = EXCLUDED.nome
                RETURNING id_advogado;
                """,
                (nome_advogado,)
            )
            id_advogado = cursor.fetchone()[0]
            
            cursor.execute(
                """
                INSERT INTO Licitacao (
                    numero_licitacao, modalidade, valor_estimado, objeto_resumido,
                    data_emissao, data_abertura, finalidade, forma_julgamento, id_advogado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (numero_licitacao) DO UPDATE
                SET 
                    modalidade = EXCLUDED.modalidade,
                    valor_estimado = EXCLUDED.valor_estimado,
                    objeto_resumido = EXCLUDED.objeto_resumido,
                    data_emissao = EXCLUDED.data_emissao,
                    data_abertura = EXCLUDED.data_abertura,
                    finalidade = EXCLUDED.finalidade,
                    forma_julgamento = EXCLUDED.forma_julgamento,
                    id_advogado = EXCLUDED.id_advogado
                RETURNING id_licitacao;
                """,
                (
                    numero, modalidade, valor_estimado, objeto_resumido,
                    data_emissao, data_abertura, finalidade, forma_julgamento, id_advogado
                )
            )
            id_licitacao = cursor.fetchone()[0]

            for ug in registro['registro']['listUnidadesGestoras']:
                codigo_ug = ug.get('codigo', None)
                cursor.execute(
                """
                INSERT INTO Licitacao_Unidade_Gestora (
                    id_licitacao, codigo_unidade_gestora
                ) VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (id_licitacao, codigo_ug)
            )
            
            for item in registro['registro']['listItens']:
                numero_item = item.get('numero', None)
                denominacao_item = item.get('denominacao', None)
                quantidade_item = item.get('quantidade', None)
                unidade_medida_item = item.get('unidadeMedida', None)
                valor_unitario_item = item.get('valorUnitarioEstimado', None)
                situacao_item = item.get('situacao', None)
                cursor.execute(
                """
                INSERT INTO Item (
                    numero_item, denominacao, quantidade, unidade_medida,
                    valor_unitario_estimado, situacao, id_licitacao, valor_total
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (numero_item, id_licitacao) DO UPDATE
                SET 
                    denominacao = EXCLUDED.denominacao,
                    quantidade = EXCLUDED.quantidade,
                    unidade_medida = EXCLUDED.unidade_medida,
                    valor_unitario_estimado = EXCLUDED.valor_unitario_estimado,
                    situacao = EXCLUDED.situacao,
                    valor_total = EXCLUDED.valor_total
                RETURNING id_item;
                """,
                (
                    numero_item, denominacao_item, quantidade_item, unidade_medida_item,
                    valor_unitario_item, situacao_item, id_licitacao, 
                    (quantidade_item * valor_unitario_item if quantidade_item and valor_unitario_item else None)
                )
            )
            id_item = cursor.fetchone()[0]
            for vencedor in item['listVencedores']:
                nome_fornecedor = vencedor.get('fornecedor', None)
                quantidade_vencedor = vencedor.get('quantidade', None)
                valor_unitario_vencedor = vencedor.get('valorUnitario', None)
                situacao_vencedor = vencedor.get('situacao', None)

                # Ver se fornecedor já existe
                cursor.execute(
                    "SELECT id_fornecedor FROM Fornecedor WHERE nome = %s;",
                    (nome_fornecedor,)
                )
                result = cursor.fetchone()

                if result:
                    id_fornecedor = result[0]  # Fornecedor existe, obter ID
                else:
                    # Fornecedor não existe, então inserir novo registro
                    cursor.execute(
                        """
                        INSERT INTO Fornecedor (nome) 
                        VALUES (%s) 
                        ON CONFLICT (nome) DO UPDATE
                        SET nome = EXCLUDED.nome
                        RETURNING id_fornecedor;
                        """,
                        (nome_fornecedor,)
                    )
                    id_fornecedor = cursor.fetchone()[0]  # obter ID

                # Insert into Vencedor table
                cursor.execute(
                    """
                    INSERT INTO Vencedor (
                        id_item, id_fornecedor, quantidade, valor_unitario, situacao
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id_item, id_fornecedor) DO UPDATE
                    SET 
                        quantidade = EXCLUDED.quantidade,
                        valor_unitario = EXCLUDED.valor_unitario,
                        situacao = EXCLUDED.situacao;
                    """,
                    (id_item, id_fornecedor, quantidade_vencedor, valor_unitario_vencedor, situacao_vencedor)
                )
                
            for texto in registro['registro']['listTextos']:
                denominacao_texto = texto.get('denominacao', None)
                link_texto = None

                # Check if the `licitacao_interno` contains the text and fetch the link
                if licitacao_interno and 'textos' in licitacao_interno:
                    texto_interno = next((t for t in licitacao_interno['textos'] if t['nome'] == denominacao_texto), None)
                    url_texto = "https://transparencia.e-publica.net/epublica-portal/rest/florianopolis/licitacao/texto/download/public?ano=2025&entidade=2002"
                    if texto_interno:
                        requisicao = requests.post(url_texto, json=texto_interno)
                        if requisicao.status_code == 200:
                            link_texto = requisicao.json().get('id', None)

                # Insert the row into the Texto table
                cursor.execute(
                    """
                    INSERT INTO Texto (
                        id_licitacao, denominacao, link
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT (id_licitacao, denominacao) DO UPDATE
                    SET 
                        link = EXCLUDED.link
                    RETURNING id_texto;
                    """,
                    (id_licitacao, denominacao_texto, link_texto)
                )
                id_texto = cursor.fetchone()[0]
            
            # for contrato in licitacao_interno['contratos']:
                # update contrato; colocar id da licitação
                # pass
            
            for empenho in licitacao_interno['empenhos']:
                # inserir empenho
                numero_empenho = empenho.get('numero', None)
                valor_empenho = empenho.get('valorEmpenhado', None)
                valor_pago = empenho.get('valorPago', None)
                emissao_empenho = empenho.get('emissao', None)

                cursor.execute(
                    "SELECT id_empenho FROM Empenho WHERE numero_empenho = %s AND id_licitacao = %s;",
                    (numero_empenho, id_licitacao)
                )
                result = cursor.fetchone()
                if result:
                    id_empenho = result[0]
                    cursor.execute(
                        """
                        UPDATE Empenho
                        SET 
                            emissao = %s
                        WHERE id_empenho = %s;
                        """,
                        (emissao_empenho, id_empenho)
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO Empenho (
                            id_licitacao, numero_empenho, emissao
                        ) VALUES (%s, %s, %s);
                        """,
                        (id_licitacao, numero_empenho, emissao_empenho)
                    )

        conn.commit()  # Commit the transaction

    except Exception as e:
        print(f"Erro ao processar os dados: {e}")
        if conn:
            conn.rollback()  # Rollback the transaction in case of error
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# get_dados_api()

# get_dados_internos(False)
juntar_dados_licitacao()
# juntar_dados_contrato()