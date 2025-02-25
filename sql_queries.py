import yaml

# Define una funcion para cargar la configuración desde el archivo YAML
def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# Cargar la configuración
config = load_config('config.yaml')

# Asignar las variables como constantes


db_config_pro = config['database_pro']
db_name_pro = db_config_pro['name']
db_host_pro = db_config_pro['host']
db_user_pro = db_config_pro['user']
db_password_pro = db_config_pro['password']


db_tables = config['tables']
table_dim_id_market = db_tables['table_dim_id_market']
table_dim_id_pais = db_tables['table_dim_id_pais']
table_id_tickers = db_tables['table_id_tickers']
table_process_daily = db_tables['table_process_daily']
table_raw_stock = db_tables['table_raw_stock']
table_raw_fundamentals = db_tables['table_raw_fundamentals']
table_positions = db_tables['table_positions_stock_market']
table_orders = db_tables['table_orders_stock_market']
table_predictions_stock_daily = db_tables['table_predictions_stock_daily']

list_of_fetures_fundamentals_1 = ['id_ticker',
    'id_market',
    'id_pais',
    'capitalizacion_millones',
    'market_value_eur_millones',
    'float_pct_total_outstdg',
    'free_float_eur_millones',
    'ultimo_precio',
    'percentage_dia_precio',
    'div_yld_y0_year_cerrado',
    'div_yld_y1_year_actual',
    'div_yld_y2',
    'div_yld_y3',
    'per_y0_year_cerrado',
    'per_y1_year_actual',
    'per_y2',
    'per_y3',
    'ev_ebitda_y0_year_cerrado',
    'ev_ebitda_y1_year_actual',
    'ev_ebitda_y2',
    'ev_ebitda_y3',
    'precio_valor_libros_y0_year_cerrado',
    'precio_valor_libros_y1_year_actual',
    'precio_valor_libros_y2',
    'precio_valor_libros_y3',
    'precio_cf_y0_year_cerrado',
    'precio_cf_y1_year_actual',
    'precio_cf_y2',
    'precio_cf_y3',
    'fcf_ev_percentage_y0_year_cerrado',
    'fcf_ev_percentage_y1_year_actual',
    'fcf_ev_percentage_y2',
    'fcf_yld_percentage_y0_year_cerrado',
    'fcf_yld_percentage_y1_year_actual',
    'fcf_yld_percentage_y2',
    'peg_fy1',
    'peg_fy2',
    'margen_ebitda_percentage_y0_year_cerrado',
    'margen_ebitda_percentage_y1_year_actual',
    'margen_ebitda_percentage_y2',
    'margen_ebitda_percentage_y3',
    'deuda_neta_ebitda_y0_year_cerrado',
    'deuda_neta_ebitda_y1_year_actual',
    'deuda_neta_ebitda_y2',
    'deuda_neta_ebitda_y3',
    'recomendacion_numerica',
    'precio_objetivo_12_meses',
    'potencial_precio_objetivo_percentage',
    'roe_y1_year_actual',
    'crecimiento_largo_plazo_percentage',
    'twelve_meses_percentage',
    'ytd_percentage',
    'last_52_wks_low_price',
    'percentage_desde_minimos_1_year',
    'last_52_wks_high_price',
    'percentage_desde_maximos_1_year',
    'div_payout_y0_year_cerrado',
    'div_payout_y1_year_actual',
    'div_payout_y2',
    'div_payout_y3',
    'eca_num_eps',
    'eca_num_ebit',
    'ec_reco_total',
    'ec_reco_up',
    'ec_reco_down',
    'ec_reco_unchng',
    'percentage_mod_recom_positivas_total',
    'ec_reco_pos',
    'ec_reco_neg',
    'positivas_total_percentage',
    'three_y_price_volatility',
    'issue_common_shares_outstdg',
    'average_daily_volume',
    'volumen_acciones_percentage',
    'three_y_beta_rel_to_loc_idx',
    'percentage_capital_contratado_diariamente',
    'bpa_plus_1e_3meses',
    'bpa_plus_1e_actual',
    'var_percentage_bpa_plus_1e_3meses',
    'ebit_plus_1e_3meses',
    'ebit_plus_1e_actual',
    'var_percentage_ebit_plus_1e_3meses',
    'precio_objetivo_3meses',
    'var_percentage_po_3meses',
    'ventas_plus_1e_3meses',
    'ventas_plus_1e_actual',
    'var_percentage_ventas_plus_1e_3meses',
    'media_200',
    'media_50',
    'media_25',
    'dif_percentage_media_200',
    'dif_percentage_media_50',
    'dif_percentage_media_25',
    'media_50_200',
    'volumen_diario_3meses_eur_millones',
    'volumen_dia_3_meses_capitalizacion_percentage',
    'bpa_trimestre_sorpresa_percentage',
    'ventas_trimestre_sorpresa_percentage',
    'ebit_trimestral_sorpresa_percentage',
    'short_interest_percentage_out',
    'short_interest_acciones_actual',
    'short_interest_acciones_3_meses',
    'evolucion_acciones_cortas_3_meses_percentage',
    'position_relative',
    'date']


class SQLQueries:
    def __init__(self, table_id_tickers, table_process_daily, table_raw_stock, table_dim_id_pais, table_positions, table_orders, table_predictions_stock_daily):
        self.table_id_tickers = table_id_tickers
        self.table_process_daily = table_process_daily
        self.table_raw_stock = table_raw_stock
        self.table_dim_id_pais = table_dim_id_pais
        self.table_raw_fundamentals = table_raw_fundamentals
        self.table_positions = table_positions
        self.table_orders = table_orders
        self.table_predictions_stock_daily = table_predictions_stock_daily


    def get_assets_id_query():
        return f"SELECT id, fe_ticker FROM {table_id_tickers};"
        
        
    def get_target():
        query = """
        SELECT 
            t1.id_ticker,
            t1.date,
            t1.close AS close_today,
            (
                SELECT t2.close
                FROM stock_market_db.raw_fact_daily_prices_stock_market t2
                WHERE t2.id_ticker = t1.id_ticker 
                  AND t2.date = DATE_ADD(t1.date, INTERVAL 14 DAY)
                LIMIT 1
            ) AS close_14_days_later,
            (
                SELECT MAX(t2.high)
                FROM stock_market_db.raw_fact_daily_prices_stock_market t2
                WHERE t2.id_ticker = t1.id_ticker 
                  AND t2.date > t1.date 
                  AND t2.date <= DATE_ADD(t1.date, INTERVAL 14 DAY)
            ) AS max_high_next_14_days,
            (
                SELECT MIN(t2.low)
                FROM stock_market_db.raw_fact_daily_prices_stock_market t2
                WHERE t2.id_ticker = t1.id_ticker 
                  AND t2.date > t1.date 
                  AND t2.date <= DATE_ADD(t1.date, INTERVAL 14 DAY)
            ) AS min_low_next_14_days
        FROM stock_market_db.raw_fact_daily_prices_stock_market t1
        WHERE t1.date > "2023-01-01"
        ORDER BY t1.date;
        """
        return query
    
    def get_target_for_tesis():
        query = """
        SELECT 
            t1.id_ticker,
            t1.date,
            t1.close AS close_today,
            (
                SELECT t2.close
                FROM stock_market_db.raw_fact_daily_prices_stock_market t2
                WHERE t2.id_ticker = t1.id_ticker 
                  AND t2.date = DATE_ADD(t1.date, INTERVAL 14 DAY)
                LIMIT 1
            ) AS close_14_days_later,
            (
                SELECT MAX(t2.high)
                FROM stock_market_db.raw_fact_daily_prices_stock_market t2
                WHERE t2.id_ticker = t1.id_ticker 
                  AND t2.date > t1.date 
                  AND t2.date <= DATE_ADD(t1.date, INTERVAL 14 DAY)
            ) AS max_high_next_14_days,
            (
                SELECT MIN(t2.low)
                FROM stock_market_db.raw_fact_daily_prices_stock_market t2
                WHERE t2.id_ticker = t1.id_ticker 
                  AND t2.date > t1.date 
                  AND t2.date <= DATE_ADD(t1.date, INTERVAL 14 DAY)
            ) AS min_low_next_14_days
        FROM stock_market_db.raw_fact_daily_prices_stock_market t1
        WHERE t1.date > "2023-01-01" AND  t1.date < "2023-12-01"
        ORDER BY t1.date;
        """
        return query
    
    
    def get_fundamental_and_fe_ticker_for_model():
        query= """
        SELECT id.fe_ticker, fund.* FROM stock_market_db.raw_fact_daily_stock_market_fundamentals AS fund LEFT JOIN
        stock_market_db.id_tickers_stock_market as id on id.id = fund.id_ticker
        WHERE fund.date <= '2023-11-12'
        """
        return query
        
        
        
