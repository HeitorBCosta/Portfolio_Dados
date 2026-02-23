import pandas as pd
from app.utils.dates import get_today_range
import json
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

class AuditService:
    def __init__(self, wallet_dw_wrapper, wallet_api_instance, wallet_api_processor, api_key):
        self.wallet_dw = wallet_dw_wrapper
        self.wallet_api = wallet_api_instance
        self.processor = wallet_api_processor
        self.api_key = api_key
        
        self.CONFIG_WALLET = "xxxx"
        self.CONFIG_PAYMENTS = "xxxxx"
        self.CONFIG_PLAYERS = "xxxx" 

    def _post_to_api(self, report_id, config_code, filters, outputs):
        """
        Envia a requisição para a API delegando ao wallet_api (APIClient).
        """
        payload = {
            "reportId": (None, str(report_id)),
            "configurationCode": (None, str(config_code)),
            "format": (None, "json"),
            "apikey": (None, str(self.api_key)), 
            "filters": (None, json.dumps(filters)),
            "outputs": (None, outputs),
            "jsonOptions": (None, '{"includeLegend": "none"}'),
        }

        try:
            response = self.wallet_api.post(files=payload, timeout=120)
            
            if isinstance(response, dict):
                return response.get('data', [])
            
            return response if isinstance(response, list) else []

        except Exception as e:
            print(f"❌ Erro na chamada do Report {report_id}: {e}")
            return []
    
    def _run_comparison_logic(self, dw_df, api_df, col_base, reference_date):
        if dw_df.empty and api_df.empty:
            print(f"⚠️ Sem dados em ambas as fontes para {col_base}.")
            return pd.DataFrame()
        
        # 1. Preparação das colunas
        def prep_df(df, suffix):
            if df.empty: 
                return pd.DataFrame(columns=['hora', f"{col_base}_{suffix}"])
            
            target_col = next((c for c in df.columns if col_base.lower() in c.lower()), df.columns[-1])
            
            new_df = df[['hora', target_col]].copy()
            new_df.columns = ['hora', f"{col_base}_{suffix}"]
            new_df['hora'] = pd.to_datetime(new_df['hora'])
            return new_df

        df_dw_clean = prep_df(dw_df, "dw")
        df_api_clean = prep_df(api_df, "api")

        # 2. Merge e Limpeza por Data
        comparison = pd.merge(df_dw_clean, df_api_clean, on='hora', how='outer').fillna(0)
        
        target_str = reference_date.strftime('%Y-%m-%d')
        comparison = comparison[comparison['hora'].dt.strftime('%Y-%m-%d') == target_str].copy()
        
        comparison = comparison.sort_values('hora')
        
        col_dw = f"{col_base}_dw"
        col_api = f"{col_base}_api"

        # 3. Cálculo da Diferença Absoluta
        comparison['diff'] = (comparison[col_dw] - comparison[col_api]).abs()
    
        # Threshold fixo em 5 
        threshold = 5 
        
        # 4. Exibição Formatada
        print(f"\n📊 RELATÓRIO DE AUDITORIA: {col_base.upper()} ({target_str})")
        
        if not comparison.empty:
            with pd.option_context('display.float_format', '{:.2f}'.format):

                print(comparison[['hora', col_dw, col_api, 'diff']].to_string(index=False))

            divergencias = comparison[comparison['diff'] > threshold].copy()

            if not divergencias.empty:
                n = len(divergencias)
                print(f"❌ ATENÇÃO: {n} {'linha' if n == 1 else 'linhas'} com divergência significativa (> {threshold})!")
            else:
                print(f"✅ Tudo OK! (Diferenças dentro do limite de {threshold})")
        else:
            print(f"--- Sem dados para {target_str} após filtragem ---")
        
        return comparison

    def run_wallet_audit(self, product_type="Casino"):
        start_date, _ = get_today_range()
        date_str = start_date.strftime('%Y-%m-%d')
        print(f"\n--- 🔍 Iniciando Auditoria Wallet: {product_type} | {date_str} ---")
        dw_product = product_type.lower() if product_type.lower() == "casino" else "sports"
        dw_df = self.wallet_dw.get_data("wallet", start_date, product=dw_product)

        filters = {
            "instance": ["xxx"],
            "sdin": "1",
            "product": [product_type], 
            "startdate": date_str, 
            "enddate": date_str,
            "time_zone_picker": "Brazil/East",
            "reportBy": "hour1,product"
        }
        raw_data = self._post_to_api("ReportingWallet", self.CONFIG_WALLET, filters, "hour1,product,completed_bets,totalbet,ggr,ngr")
        api_df = self.processor.process_wallet(raw_data)
        return self._run_comparison_logic(dw_df, api_df, "Turnover", start_date)

    def run_payments_audit(self):
        start_date, _ = get_today_range()
        date_str = start_date.strftime('%Y-%m-%d')
        print(f"\n--- 💰 Iniciando Auditoria Payments: {date_str} ---")
        dw_df = self.wallet_dw.get_data("payments", start_date)
        filters = {
            "casino": ["xxx"],
            "reportby": "1",
            "internal_account": "0",
            "startdate": f"{date_str} 00:00:00",
            "enddate": f"{date_str} 23:59:59",
            "time_zone_picker": "Brazil/East"
        }
        raw_data = self._post_to_api("ReportingPayment", self.CONFIG_PAYMENTS, filters, "transaction_id,username,type,method,firstdeposit,datetime,acceptdatetime,status,amount,currencycode")
        api_df = self.processor.process_payments(raw_data)
        return self._run_comparison_logic(dw_df, api_df, "deposit", start_date)
    

    def run_ftd_audit(self):
        # 1. Define o período (Mantendo sua lógica de hoje)
        start_date, _ = get_today_range()
        date_str = start_date.strftime('%Y-%m-%d')
        print(f"\n--- 🥇 Iniciando Auditoria FTD Real: {date_str} ---")

        # 2. Busca dados do DW
        dw_df = self.wallet_dw.get_data("ftd", start_date)

        filters = {
            "hasdeposited": "3",
            "fddaterange": json.dumps({
                "startdate": f"{date_str} 00:00:00",
                "enddate": f"{date_str} 23:59:59"
            }),
            "instance": ["xxxxx"],
            "internalaccount": "2"
        }
        
        outputs = "playercode,firstdepositdate,firstdeposit"
        raw_data = self._post_to_api("ReportingPlayers", "0000", filters, outputs)
        
        if not raw_data:
            print(f"⚠️ Nenhum FTD encontrado na API para {date_str}")
            return None

        api_df = self.processor.process_ftd(raw_data, date_str)

        return self._run_comparison_logic(dw_df, api_df, "ftd", start_date)
    
    def run_players_audit(self):
        start_date, _ = get_today_range()
        date_str = start_date.strftime('%Y-%m-%d')
        print(f"\n--- 👥 Iniciando Auditoria de Cadastros (Players): {date_str} ---")

        # 1. Busca dados do DW (Contagem de cadastros por hora)
        dw_df = self.wallet_dw.get_data("players", start_date)

        # 2. Filtros baseados no seu exemplo de sucesso
        filters = {
            "signupdaterange": json.dumps({
                "startdate": f"{date_str} 00:00:00",
                "enddate": f"{date_str} 23:59:59"
            }),
            "instance": ["0000"],
            "internalaccount": "2" 
        }
        
        outputs = "playercode,signupdate,username,internalaccount"
        raw_data = self._post_to_api("ReportingPlayers", "0000", filters, outputs)
        
        if not raw_data:
            print(f"⚠️ Nenhum cadastro encontrado na API para {date_str}")
            return None

        # 4. Processa os dados
        api_df = self.processor.process_players(raw_data, date_str)

        return self._run_comparison_logic(dw_df, api_df, "players", start_date)