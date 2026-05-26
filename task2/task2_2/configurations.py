DB_CONFIG = {
    'dbname': 'dwh',
    'user': 'postgres',
    'password': 'p@ssw0rd',
    'host': 'localhost',
    'port': 5432
}

# путь к папке с csv-файлами
DATA_DIR = 'data'

# Конфигурация таблиц для загрузки
RD_TABLES_CONFIG = [
    {
        'table_name': 'rd.deal_info',
        'file_name': 'deal_info.csv',
        'primary_keys': ['deal_rk', 'effective_from_date']
    },
    {
        'table_name': 'rd.product',
        'file_name': 'product_info.csv',
        'primary_keys': ['product_rk', 'effective_from_date']
    }
]