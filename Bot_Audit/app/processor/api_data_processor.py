import pandas as pd
from typing import List, Dict, Any

class ApiDataProcessor:
    """
    Responsável por normalizar e tratar dados crus vindos da API.
    Centraliza a limpeza de duplicados e ajuste de horas.
    """
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def _to_hour(series: pd.Series) -> pd.Series:
        return (
            pd.to_datetime(series)
            .dt.tz_localize(None)
        ).dt.floor('h')

    # PLAYERS
    @staticmethod
    def process_players(raw_data: list, target_date: str) -> pd.DataFrame:
        if not raw_data: 
            return pd.DataFrame(columns=['hora', 'players_api'])
            
        df = pd.DataFrame(raw_data)

        df['signupdate'] = pd.to_datetime(df['signupdate'])
        
        df['signupdate'] = df['signupdate'] - pd.Timedelta(hours=3)
        
        df['hora'] = df['signupdate'].dt.floor('h')
        
        df_grouped = df.groupby('hora').agg(
            players_api=('playercode', 'nunique')
        ).reset_index()
        
        return df_grouped

    # FTD 
    @staticmethod
    def process_ftd(raw_data: list, target_date: str) -> pd.DataFrame:
        if not raw_data: 
            return pd.DataFrame(columns=['hora', 'ftds_api', 'amount_api'])
            
        df = pd.DataFrame(raw_data)

        df['firstdepositdate'] = pd.to_datetime(df['firstdepositdate'])
        
        df['firstdepositdate'] = df['firstdepositdate'] - pd.Timedelta(hours=3)
        
        df['hora'] = df['firstdepositdate'].dt.floor('h')

        return df.groupby('hora').agg(
            ftds_api=('playercode', 'nunique'),
            amount_api=('firstdeposit', 'sum')
        ).reset_index()
        
    # PAYMENTS
    @staticmethod
    def process_payments(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)

        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
        df = df[df['status'].str.lower() == 'approved'].copy()

        df['hora_base'] = df['acceptdatetime'].replace('', None).fillna(df['datetime'])
        df['hora'] = ApiDataProcessor._to_hour(df['hora_base'])

        df = df.pivot_table(
            index='hora',
            columns='type',
            values='amount',
            aggfunc='sum'
        ).fillna(0).reset_index()

        df.columns = [c.lower() for c in df.columns]
        return df

    # WALLET
    @staticmethod
    def process_wallet(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data: return pd.DataFrame()
        df = pd.DataFrame(raw_data)

        df['hora'] = ApiDataProcessor._to_hour(df['hour1'])

        for col in ['completed_bets', 'totalbet', 'ggr', 'ngr']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return (
            df.rename(columns={
                'completed_bets': 'Apostas',
                'totalbet': 'Turnover',
                'ggr': 'GGR',
                'ngr': 'NGR',
            })[['hora', 'Apostas', 'Turnover', 'GGR', 'NGR']]
            .sort_values('hora')
        )

    @staticmethod
    def wallet_by_hour(raw_data=None, *args, **kwargs) -> pd.DataFrame:
        if raw_data is None or isinstance(raw_data, str):
            return pd.DataFrame(columns=['hora', 'Apostas', 'Turnover', 'GGR', 'NGR'])
        return ApiDataProcessor.process_wallet(raw_data)

