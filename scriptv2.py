import requests
import time
import csv
import os

# ================= CONFIGURAÇÕES =================
TOKEN = "INSIRA_SEU_TOKEN"
URL = "https://api.github.com/graphql"
CSV_FILE = 'repositorios_github.csv'
CHECKPOINT_FILE = 'last_cursor.txt'
MAX_REPOS = 1000
REPOS_PER_PAGE = 10
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# ================= QUERY GRAPHQL =================
QUERY = """
query ($cursor: String, $first: Int) {
  search(query: "stars:>1 sort:stars-desc", type: REPOSITORY, first: $first, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Repository {
        name
        owner { login }
        url
        stargazerCount
        createdAt
        updatedAt
        primaryLanguage { name }
        pullRequests(states: MERGED) { totalCount }
        releases { totalCount }
        totalIssues: issues(first: 0) { totalCount }
        closedIssues: issues(states: CLOSED, first: 0) { totalCount }
      }
    }
  }
}
"""


def save_to_csv(repos):
    """Salva os repositórios no CSV de forma incremental."""
    colunas = [
        'name', 'owner', 'url', 'stargazers', 'createdAt', 'updatedAt',
        'primaryLanguage', 'pullRequests', 'releases',
        'totalIssues', 'closedIssues'
    ]

    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        if not file_exists:
            writer.writeheader()

        for repo in repos:
            writer.writerow({
                'name': repo['name'],
                'owner': repo['owner']['login'],
                'url': repo['url'],
                'stargazers': repo['stargazerCount'],
                'createdAt': repo['createdAt'],
                'updatedAt': repo['updatedAt'],
                'primaryLanguage': repo['primaryLanguage']['name'] if repo['primaryLanguage'] else 'None',
                'pullRequests': repo['pullRequests']['totalCount'],
                'releases': repo['releases']['totalCount'],
                'totalIssues': repo['totalIssues']['totalCount'],
                'closedIssues': repo['closedIssues']['totalCount']
            })


def get_checkpoint():
    """Verifica se existe um cursor salvo e conta quantos repos já temos no CSV."""
    cursor = None
    count = 0

    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            cursor = f.read().strip()
            if cursor == "FIM":
                return None, MAX_REPOS

    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            # Conta as linhas (excluindo o cabeçalho)
            count = sum(1 for line in f) - 1

    return cursor, max(0, count)


def fetch_repos():
    cursor, count = get_checkpoint()

    if count >= MAX_REPOS:
        print(f"✅ Coleta já concluída! ({count} repositórios no CSV).")
        return

    print(f"🚀 Iniciando/Retomando coleta a partir do repositório {count}...")

    while count < MAX_REPOS:
        retry_count = 0
        success = False

        # Cálculo para não ultrapassar 1000 na última chamada
        faltando = MAX_REPOS - count
        pedir_agora = min(REPOS_PER_PAGE, faltando)

        while retry_count < 5 and not success:
            variables = {"cursor": cursor, "first": pedir_agora}

            try:
                response = requests.post(
                    URL,
                    json={'query': QUERY, 'variables': variables},
                    headers=HEADERS,
                    timeout=30
                )

                if response.status_code == 200:
                    result = response.json()

                    if 'errors' in result:
                        print(
                            f"❌ Erro na Query: {result['errors'][0]['message']}")
                        return

                    data = result['data']['search']
                    nodes = data['nodes']

                    if not nodes:
                        print("ℹ️ Nenhum repositório retornado. Fim da busca.")
                        break

                    save_to_csv(nodes)

                    count += len(nodes)
                    cursor = data['pageInfo']['endCursor']

                    # Salva o checkpoint
                    with open(CHECKPOINT_FILE, 'w') as f:
                        f.write(cursor if cursor else "FIM")

                    success = True
                    print(f"📦 Coletados: {count}/{MAX_REPOS}...")

                elif response.status_code in [502, 503, 504]:
                    retry_count += 1
                    wait = retry_count * 10
                    print(
                        f"⚠️ Erro {response.status_code}. Tentativa {retry_count}/5. Aguardando {wait}s...")
                    time.sleep(wait)

                elif response.status_code == 403:
                    print("🚫 Rate limit atingido. Pausando por 60 segundos...")
                    time.sleep(60)
                    retry_count += 1
                else:
                    print(
                        f"❌ Erro HTTP {response.status_code}: {response.text}")
                    return

            except requests.exceptions.RequestException as e:
                retry_count += 1
                print(f"🔌 Erro de conexão: {e}. Tentando novamente em 5s...")
                time.sleep(5)

        if not success:
            print(
                "🛑 Falha persistente. O script parou para evitar loop. Tente rodar novamente mais tarde.")
            break

        time.sleep(1)  # Intervalo entre páginas


if __name__ == "__main__":
    print(f"Iniciando coleta de {MAX_REPOS} repositórios...")
    fetch_repos()
    print("🏁 Processo finalizado.")
