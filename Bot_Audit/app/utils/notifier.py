import requests
import pandas as pd
from datetime import datetime, timedelta

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"


    def process_and_send_audit (self, label, df_result):
        try:
            if df_result is None or df_result.empty:
                print(f"✔️ {label}: Sem dados.")
                return

            now = datetime.now()
            current_hour = now.hour
            df_to_send = df_result.copy()

            # 1. LOCALIZAÇÃO E NORMALIZAÇÃO DA COLUNA HORA
            possible_cols = ['hour', 'Hora', 'hour_api', 'HORA']
            col_hora = next((c for c in possible_cols if c in df_to_send.columns), None)

            if not col_hora:
                print(f"⚠️ {label}: Coluna de hora não encontrada.")
                return

            # Normalização para Int64
            df_to_send[col_hora] = (
                df_to_send[col_hora]
                    .astype(str)
                    .str.extract(r'(\d+)')[0]
                    .astype(float)
                    .astype('Int64')
            )

            df_to_send = df_to_send[df_to_send[col_hora] < current_hour].copy()
            df_to_send = df_to_send[df_to_send[col_hora] != current_hour]

            if 'diff' in df_to_send.columns:
                df_to_send['diff'] = pd.to_numeric(df_to_send['diff'], errors='coerce')
                df_to_send = df_to_send[df_to_send['diff'].abs() > 5]

            if df_to_send.empty:
                print(f"✔️ {label}: Nenhuma divergência em horas fechadas.")
                return

            if "PLAYERS" in label:
                df_to_send = df_to_send.rename(columns={
                    'players_api': 'qtd_players',
                    'players_dw': 'qtde_registros_dw',
                    'diff': 'diferenca'
                })
            elif "FTD" in label:
                df_to_send = df_to_send.rename(columns={
                    'ftd_api': 'FTD_Users',
                    'ftd_dw': 'FTD_DW',
                    'diff': 'diferenca'
                })
            elif "PAYMENTS" in label:
                df_to_send = df_to_send.rename(columns={
                    'deposit_api': 'api_deposit',
                    'deposit_dw': 'dw_deposit',
                    'diff': 'diferenca_Deposit'
                })
                df_to_send['api_withdraw'] = 0
                df_to_send['dw_withdraw'] = 0
                df_to_send['diferenca_Withdraw'] = 0

            self.send_audit_alert(label, df_to_send)
            print(f"✅ Alerta de {label} enviado (somente horas fechadas).")

        except Exception as e:
            print(f"❌ Erro ao processar auditoria de {label}: {e}")

  