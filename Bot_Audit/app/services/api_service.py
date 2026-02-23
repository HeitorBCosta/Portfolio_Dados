# app/services/galera_api_service.py
import json
from typing import List, Dict
from app.clients.api_client import APIClient
import pandas as pd


class GaleraAPIService:
    """
    Serviço responsável por montar payloads da Galera
    e retornar dados crus da API.
    """

    def __init__(self, api_client: APIClient, api_key: str):
        self.api = api_client
        self.api_key = api_key

    # ===============================
    # PLAYERS
    # ===============================
    def get_players_raw(self, daterange: dict) -> List[Dict]:
        files = {
            "reportId": (None, "Reporting___Players"),
            "configurationCode": (None, "33707"),
            "format": (None, "json"),
            "apikey": (None, self.api_key),
            "filters": (
                None,
                json.dumps({
                    "casino": ["4212"],
                    "daterange": json.dumps(daterange),
                    "time_zone_picker": "GMT",
                }),
            ),
            "outputs": (None, "username,signupdate"),
            "jsonOptions": (None, '{"includeLegend":"none"}'),
        }

        response = self.api.post(files)
        return response.get("data", [])

    # ===============================
    # FTD
    # ===============================
    def get_ftd_raw(self, daterange: dict) -> List[Dict]:
        files = {
            "reportId": (None, "Reporting___FTD"),
            "configurationCode": (None, "33707"),
            "format": (None, "json"),
            "apikey": (None, self.api_key),
            "filters": (
                None,
                json.dumps({
                    "casino": ["4212"],
                    "daterange": json.dumps(daterange),
                    "time_zone_picker": "GMT",
                }),
            ),
            "outputs": (None, "username,acceptdatetime"),
            "jsonOptions": (None, '{"includeLegend":"none"}'),
        }

        response = self.api.post(files)
        return response.get("data", [])

    # ===============================
    # PAYMENTS
    # ===============================
    def get_payments_raw(self, daterange: dict) -> List[Dict]:
        files = {
            "reportId": (None, "Reporting___Payment_transactions"),
            "configurationCode": (None, "33707"),
            "format": (None, "json"),
            "apikey": (None, self.api_key),
            "filters": (
                None,
                json.dumps({
                    "casino": ["4212"],
                    "reportby": "1",
                    "internal_account": "0",
                    "daterange": json.dumps(daterange),
                    "time_zone_picker": "Brazil/East",
                }),
            ),
            "outputs": (
                None,
                "transaction_id,username,type,method,firstdeposit,"
                "datetime,acceptdatetime,status,amount,currencycode"
            ),
            "jsonOptions": (None, '{"includeLegend":"none"}'),
        }

        response = self.api.post(files)
        return response.get("data", [])

    # ===============================
    # WALLET
    # ===============================
    def get_wallet_raw(self, daterange: dict, product_name: str) -> List[Dict]:
        files = {
            "reportId": (None, "Reporting___Wallet_business_facts"),
            "configurationCode": (None, "31602"),
            "format": (None, "json"),
            "apikey": (None, self.api_key),
            "filters": (
                None,
                json.dumps({
                    "instance": ["4212"],
                    "sdin": "1",
                    "product": [product_name],
                    "daterange": json.dumps(daterange),
                    "time_zone_picker": "Brazil/East",
                    "reportBy": "hour1,product",
                }),
            ),
            "outputs": (None, "hour1,product,completed_bets,totalbet,ggr,ngr"),
            "jsonOptions": (None, '{"includeLegend":"none"}'),
        }

        response = self.api.post(files)
        return response.get("data", [])
    
    @staticmethod
    def process_players(raw_data: list, target_date: str) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=['hora', 'players_api'])

        df = pd.DataFrame(raw_data)
        
        # Converte data de cadastro
        df['signupdate'] = pd.to_datetime(df['signupdate'])
        
        # AJUSTE DE TIMEZONE (O pulo do gato 🐱)
        df['signupdate'] = df['signupdate'] - pd.Timedelta(hours=3)
        
        # Trunca a hora para o agrupamento
        df['hora'] = df['signupdate'].dt.floor('h')
        
        # Agrupa por hora contando playercodes únicos
        return df.groupby('hora').agg(
            players_api=('playercode', 'nunique')
        ).reset_index()