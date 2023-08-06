import pandas as pd
import numpy as np

import sys
sys.path.append('../libs')
from fn_database import conectar
from fn_database import inserir_dados

from datetime import datetime


categories = pd.read_csv('categories.csv',sep = ';', encoding='utf-8')
customers = pd.read_csv('customers.csv', sep = ';', encoding='utf-8')
employee_territories = pd.read_csv('employee_territories.csv', sep = ';', encoding='utf-8')
employees = pd.read_csv('employees.csv', sep = ';', encoding='utf-8')
order_details = pd.read_csv('order_details.csv', sep = ';', encoding='utf-8')
orders = pd.read_csv('orders.csv', sep = ';', encoding='utf-8')
products = pd.read_csv('products.csv', sep = ';', encoding='utf-8')
region = pd.read_csv('region.csv', sep = ';', encoding='utf-8')
shippers = pd.read_csv('shippers.csv', sep = ';', encoding='utf-8')
suppliers = pd.read_csv('suppliers.csv', sep = ';', encoding='utf-8')
territories = pd.read_csv('territories.csv', sep = ';', encoding='utf-8')
us_states = pd.read_csv('us_states.csv', sep = ';', encoding='utf-8')



# Adicionando coluna de importação

categories["import_date"] = datetime.now()
customers["import_date"] = datetime.now()
employee_territories["import_date"] = datetime.now()
employees["import_date"] = datetime.now()
order_details["import_date"] = datetime.now()
orders["import_date"] = datetime.now()
products["import_date"] = datetime.now()
region["import_date"] = datetime.now()
shippers["import_date"] = datetime.now()
suppliers["import_date"] = datetime.now()
territories["import_date"] = datetime.now()
us_states["import_date"] = datetime.now()


# Conectando ao database
conexao = conectar('northwind_bronze')


inserir_dados(conexao, 'raw_categories', categories)
inserir_dados(conexao, 'raw_customers', customers)
inserir_dados(conexao, 'raw_employee_territories', employee_territories)
inserir_dados(conexao, 'raw_employees', employees)
inserir_dados(conexao, 'raw_order_details', order_details)
inserir_dados(conexao, 'raw_orders', orders)
inserir_dados(conexao, 'raw_products', products)
inserir_dados(conexao, 'raw_region', region)
inserir_dados(conexao, 'raw_shippers', shippers)
inserir_dados(conexao, 'raw_suppliers', suppliers)
inserir_dados(conexao, 'raw_territories', territories)
inserir_dados(conexao, 'raw_us_states', us_states)