import requests
import time
import csv
import os

# Configurações
token = "INSIRA_SEU_TOKEN"
url = "https://api.github.com/graphql"
headers = {"Authorization": f"Bearer {token}"}
csv_file = 'repositorios_github.csv'
MAX_REPOS = 1000
REPOS_PER_PAGE = 15  # Reduzido levemente para evitar 502

query = """
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
    colunas = [
        'name', 'owner', 'url', 'createdAt', 'updatedAt',
        'primaryLanguage', 'pullRequests', 'releases',
        'totalIssues', 'closedIssues'
    ]

    file_exists = os.path.isfile(csv_file)

    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        if not file_exists:
            writer.writeheader()

        for repo in repos:
            writer.writerow({
                'name': repo['name'],
                'owner': repo['owner']['login'],
                'url': repo['url'],
                'createdAt': repo['createdAt'],
                'updatedAt': repo['updatedAt'],
                'primaryLanguage': repo['primaryLanguage']['name'] if repo['primaryLanguage'] else 'None',
                'pullRequests': repo['pullRequests']['totalCount'],
                'releases': repo['releases']['totalCount'],
                'totalIssues': repo['totalIssues']['totalCount'],
                'closedIssues': repo['closedIssues']['totalCount']
            })


def fetch_repos():
    cursor = None
    count = 0
    has_next_page = True

    while has_next_page and count < MAX_REPOS:
        retry_count = 0
        success = False

        while retry_count < 5 and not success:
            variables = {"cursor": cursor, "first": REPOS_PER_PAGE}
            try:
                response = requests.post(
                    url,
                    json={'query': query, 'variables': variables},
                    headers=headers,
                    timeout=30  # Timeout para não travar a execução
                )

                if response.status_code == 200:
                    result = response.json()
                    if 'errors' in result:
                        print(
                            f"Erro na Query: {result['errors'][0]['message']}")
                        return  # Erro de sintaxe ou permissão, para o script

                    data = result['data']['search']
                    nodes = data['nodes']

                    save_to_csv(nodes)

                    count += len(nodes)
                    cursor = data['pageInfo']['endCursor']
                    has_next_page = data['pageInfo']['hasNextPage']
                    success = True
                    print(f"Total coletado: {count}/{MAX_REPOS}...")

                elif response.status_code in [502, 503, 504]:
                    retry_count += 1
                    wait = retry_count * 10
                    print(
                        f"Erro {response.status_code}. Tentativa {retry_count}/5. Esperando {wait}s...")
                    time.sleep(wait)

                elif response.status_code == 403:
                    print(
                        "Rate limit atingido ou acesso negado. Pausando por 1 minuto...")
                    time.sleep(60)
                    retry_count += 1
                else:
                    print(f"Erro HTTP {response.status_code}: {response.text}")
                    return

            except requests.exceptions.RequestException as e:
                retry_count += 1
                print(f"Erro de conexão: {e}. Tentando novamente...")
                time.sleep(5)

        if not success:
            print(
                "Falha persistente na página atual. Abortando para evitar loop infinito.")
            break

        time.sleep(1)  # Intervalo educado entre chamadas


if __name__ == "__main__":
    print(f"Iniciando coleta de {MAX_REPOS} repositórios...")
    fetch_repos()
    print("Processo finalizado.")
