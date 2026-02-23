from datetime import datetime
from typing import Literal
import pandas as pd

from app.clients.dw_client import DWClient
from app.clients.sql.dw_sql import (
    PLAYERS_BY_HOUR,
    FTD_BY_HOUR,
    PAYMENTS_BY_HOUR,
    WALLET_BY_HOUR,
    WALLET_BY_HOUR_SPORTS,
)

ProductType = Literal["casino", "sports"]


class DWWrapper:
    def __init__(self, dw_client: DWClient):
        self.dw = dw_client

    def get_data(self, audit_type: str, reference_date: datetime, product: ProductType = "casino") -> pd.DataFrame:
        """
        Método único para buscar dados do DW.
        audit_type: 'wallet', 'payments', 'ftd' ou 'players'
        """
        # 1. Mapa de Queries e Mapeamento de Colunas para Padronização
        config = {
            "wallet": {
                "query": WALLET_BY_HOUR if product == "casino" else WALLET_BY_HOUR_SPORTS,
                "rename": {"turnover": "Turnover_dw"}
            },
            "payments": {
                "query": PAYMENTS_BY_HOUR,
                "rename": {"deposit": "deposit_dw"}
            },
            "ftd": {
                "query": FTD_BY_HOUR,
                "rename": {"ftd_users_dw": "ftds_dw"} # O SQL gera 'ftd_users_dw'
            },
            "players": {
                "query": PLAYERS_BY_HOUR,
                "rename": {"qtde_registros_dw": "players_dw"} # O SQL gera 'qtde_registros_dw'
            }
        }

        conf = config.get(audit_type)
        if not conf:
            raise ValueError(f"Tipo de auditoria inválido: {audit_type}")

        # 2. Execução Única
        df = self.dw.fetch_df(conf["query"], params=(reference_date, reference_date))

        # 3. Padronização automática
        if not df.empty:
            df = df.rename(columns=conf["rename"])
            if 'hora' in df.columns:
                df['hora'] = pd.to_datetime(df['hora'])
        
        return df

    # Alias para não quebrar chamadas antigas que usam .by_hour
    def by_hour(self, product: ProductType, reference_date: datetime) -> pd.DataFrame:
        return self.get_data("wallet", reference_date, product)

    # ===============================
    # FTD
    # ===============================
    def get_ftd_by_hour(
        self,
        reference_date: datetime
    ) -> pd.DataFrame:
        return self.dw.fetch_df(
            FTD_BY_HOUR,
            params=(reference_date, reference_date)
        )

    # ===============================
    # PAYMENTS
    # ===============================
    def get_payments_by_hour(
        self,
        reference_date: datetime
    ) -> pd.DataFrame:
        return self.dw.fetch_df(
            PAYMENTS_BY_HOUR,
            params=(reference_date, reference_date)
        )

    # ===============================
    # WALLET
    # ===============================
    def get_wallet_by_hour(
        self,
        product: ProductType,
        reference_date: datetime
    ) -> pd.DataFrame:
        query = self._wallet_query(product)

        return self.dw.fetch_df(
            query,
            params=(reference_date, reference_date)
        )

    @staticmethod
    def _wallet_query(product: ProductType) -> str:
        if product == "casino":
            return WALLET_BY_HOUR
        if product == "sports":
            return WALLET_BY_HOUR_SPORTS

        raise ValueError(f"Produto inválido: {product}")
    # ===============================
    # ALIAS PARA COMPATIBILIDADE
    # ===============================
    def by_hour(self, product: ProductType, reference_date: datetime) -> pd.DataFrame:
        """
        Atalho para get_wallet_by_hour para evitar erro no AuditService.
        """
        return self.get_wallet_by_hour(product, reference_date)

