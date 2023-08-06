import pandas as pd
import numpy as np

import sys
sys.path.append('../libs')
from fn_database import conectar
from fn_database import inserir_dados
from fn_database import executar_consulta

# Adicionando a tabela tb_dm_products
query =(
'''
SELECT 
	A.product_id
    ,A.category_id
	,A.supplier_id
	,A.product_name
	,A.discontinued
FROM northwind_bronze.raw_products A
LEFT JOIN northwind_bronze.raw_categories B
	ON A.category_id = B.category_id;
'''
)

conexao = conectar('northwind_bronze')
tb_dm_products = executar_consulta(conexao,query)
conexao = conectar('northwind_silver')
inserir_dados(conexao, 'tb_dm_products', tb_dm_products)

# Adicionando a tabela tb_ft_stock
query = '''
SELECT 
	A.product_id
	,A.unit_price
	,A.units_in_stock
	,A.units_on_order
	,A.reorder_level
	,A.discontinued
    ,sysdate() AS last_update
FROM northwind_bronze.raw_products A
LEFT JOIN northwind_bronze.raw_categories B
	ON A.category_id = B.category_id;
'''
conexao = conectar('northwind_bronze')
tb_ft_stock = executar_consulta(conexao,query)
conexao = conectar('northwind_silver')
inserir_dados(conexao, 'tb_ft_stock', tb_ft_stock)


# Adicionando a tabela tb_dm_orders
query = '''
SELECT 
    order_id
	, customer_id
	, employee_id
	, order_date
	, required_date
	, shipped_date
    ,CASE WHEN required_date < shipped_date THEN 1 ELSE 0 END AS flg_delayed_delivery
    ,CASE WHEN required_date < shipped_date THEN datediff(shipped_date,required_date) ELSE NULL END AS delay_days 
	, import_date
FROM northwind_bronze.raw_orders A'''

conexao = conectar('northwind_bronze')
tb_dm_orders = executar_consulta(conexao,query)
conexao = conectar('northwind_silver')
inserir_dados(conexao, 'tb_dm_orders', tb_dm_orders)

#Adicionando a tabela tb_dm_freight

query = '''
	SELECT DISTINCT 
		order_id
		, ship_via
		, freight
		, ship_name
		, ship_address
		, ship_city
		, ship_region
		, ship_postal_code
		, ship_country
	FROM northwind_bronze.raw_orders A'''

conexao = conectar('northwind_bronze')
tb_ft_freight = executar_consulta(conexao,query)
conexao = conectar('northwind_silver')
inserir_dados(conexao, 'tb_ft_freight', tb_ft_freight)


# Adicionando a tabela tb_ft_orders_details
query = '''
SELECT
	A.order_id
	,A.product_id
	,A.unit_price
	,quantity
	,A.discount
	,CASE WHEN discount > 0 THEN ((unit_price * quantity) - (unit_price * quantity* discount)) ELSE (unit_price * quantity) END AS total_value
	,CASE WHEN discount > 0  THEN 1 ELSE  0 END AS flg_discount
FROM northwind_bronze.raw_order_details A
'''
conexao = conectar('northwind_bronze')
tb_ft_orders_details = executar_consulta(conexao,query)
conexao = conectar('northwind_silver')
inserir_dados(conexao, 'tb_ft_orders_details', tb_ft_orders_details)


conexao = conectar('northwind_bronze')

#Resto das tabelas
tb_dm_customers = executar_consulta(conexao,'SELECT * FROM raw_customers')
tb_dm_categories = executar_consulta(conexao,'SELECT * FROM raw_categories')
tb_dm_employees = executar_consulta(conexao,'SELECT * FROM raw_employees')
tb_dm_shippers = executar_consulta(conexao,'SELECT * FROM raw_shippers')
tb_dm_territories = executar_consulta(conexao,'SELECT * FROM raw_territories')
tb_dm_suppliers = executar_consulta(conexao,'SELECT * FROM raw_suppliers')
tb_dm_us_states = executar_consulta(conexao,'SELECT * FROM raw_us_states')
tb_dm_region = executar_consulta(conexao,'SELECT * FROM raw_region')
tb_dm_employee_territories = executar_consulta(conexao,'SELECT * FROM raw_employee_territories')



conexao = conectar('northwind_silver')
inserir_dados(conexao, 'tb_dm_customers', tb_dm_customers)
inserir_dados(conexao, 'tb_dm_categories', tb_dm_categories)
inserir_dados(conexao, 'tb_dm_employees', tb_dm_employees)
inserir_dados(conexao, 'tb_dm_shippers', tb_dm_shippers)
inserir_dados(conexao, 'tb_dm_territories', tb_dm_territories)
inserir_dados(conexao, 'tb_dm_suppliers', tb_dm_suppliers)
inserir_dados(conexao, 'tb_dm_us_states', tb_dm_us_states)
inserir_dados(conexao, 'tb_dm_region', tb_dm_region)
inserir_dados(conexao, 'tb_dm_employee_territories', tb_dm_employee_territories)