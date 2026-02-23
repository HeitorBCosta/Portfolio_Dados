import requests
from typing import Dict, Any


class APIClient:
    """
    Client genérico para comunicação com APIs HTTP.
    """

    def __init__(self, base_url: str, headers: Dict[str, str] | None = None):
        self.base_url = base_url
        self.headers = headers or {}

    def post(self, files: dict, timeout: int = 60) -> Dict[str, Any]:
        response = requests.post(
            self.base_url,
            files=files,
            headers=self.headers,
            timeout=timeout
        )
        
        # 1. Se der erro oficial (400, 404, 500...)
        if response.status_code != 200:
            print(f"\n❌ ERRO DE STATUS (Status {response.status_code})")
            print(f"RESPOSTA: {response.text}")
            raise RuntimeError(f"Erro API ({response.status_code})")

        # 2. Se deu 200, mas o conteúdo está vazio ou não é JSON
        if not response.text or response.text.strip() == "":
            print("⚠️ API retornou corpo vazio (Status 200).")
            return {}

        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"❌ Falha ao converter para JSON! Conteúdo recebido: {response.text}")
            return {}