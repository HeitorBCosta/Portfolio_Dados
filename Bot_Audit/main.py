import os
from dotenv import load_dotenv
from decouple import config
from datetime import datetime,timedelta
from app.config.settings import Settings 

import requests 
import pandas as pd
import json
from app.clients.api_client import APIClient
from app.clients.dw_client import DWClient
from app.clients.dw_wrapper import DWWrapper
from app.processor.api_data_processor import ApiDataProcessor
from app.services.audit_service import AuditService
from app.utils.notifier import TelegramNotifier

def main():
    load_dotenv()
 
    api_url = config("URL_1")
    api_key = os.getenv("API_KEY_1")
    dw_conn = config("SQL_CONN_STR")
    
    # Config Telegram
    t_token = os.getenv("TELEGRAM_TOKEN")
    t_chat_id = os.getenv("TELEGRAM_CHAT_ID")

    # INSTANCIAÇÃO
    dw_client = DWClient(dw_conn)
    
    notifier = TelegramNotifier(token=t_token, chat_id=t_chat_id)
    
    api_headers = {
        "x-api-key": api_key,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
    }
    api_inst = APIClient(base_url=api_url, headers=api_headers)

    wallet_dw_wrapper = DWWrapper(dw_client)
    wallet_api_processor = ApiDataProcessor()

    audit_service = AuditService(
        wallet_dw_wrapper=wallet_dw_wrapper,
        wallet_api_instance=api_inst,
        wallet_api_processor=wallet_api_processor,
        api_key=api_key  
    )

    # DEFINIÇÃO DAS TASKS
    audit_tasks = [
        ("🎰 WALLET CASINO", lambda: audit_service.run_wallet_audit(product_type="Casino")),
        ("⚽ WALLET SPORTS", lambda: audit_service.run_wallet_audit(product_type="SB")),
        ("💰 PAYMENTS", lambda: audit_service.run_payments_audit()),
        ("👥 PLAYERS", lambda: audit_service.run_players_audit()),
        ("🥇 FTD", lambda: audit_service.run_ftd_audit())
    ]

    # EXECUÇÃO DO CICLO
    print(f"\n🚀 Iniciando Ciclo de Auditoria...")

    for label, task in audit_tasks:
        print(f"🔎 Solicitando dados de {label}...")
        try:
            # Busca os dados através do service
            df_result = task()
            
            # Chama a lógica centralizada no Notifier (Filtra hora e envia)
            notifier.process_and_send_audit(label, df_result)
            
        except Exception as e:
            print(f"❌ Falha crítica na task {label}: {e}")

    print(f"\n🏁 Auditoria finalizada.")

if __name__ == "__main__":
    main()


