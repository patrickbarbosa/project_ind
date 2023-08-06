import pandas as pd
import numpy as np

import sys
sys.path.append('../libs')
from fn_database import conectar
from fn_database import inserir_dados
from fn_database import executar_consulta


#Adicionando na gold


conexao = conectar('northwind_silver')

#Resto das tabelas
tb_dm_customers = executar_consulta(conexao,'SELECT * FROM tb_dm_customers')
tb_dm_categories = executar_consulta(conexao,'SELECT * FROM tb_dm_categories')
tb_dm_employees = executar_consulta(conexao,'SELECT * FROM tb_dm_employees')
tb_dm_shippers = executar_consulta(conexao,'SELECT * FROM tb_dm_shippers')
tb_dm_territories = executar_consulta(conexao,'SELECT * FROM tb_dm_territories')
tb_dm_suppliers = executar_consulta(conexao,'SELECT * FROM tb_dm_suppliers')
tb_dm_us_states = executar_consulta(conexao,'SELECT * FROM tb_dm_us_states')
tb_dm_region = executar_consulta(conexao,'SELECT * FROM tb_dm_region')
tb_dm_employee_territories = executar_consulta(conexao,'SELECT * FROM tb_dm_employee_territories')
tb_dm_products = executar_consulta(conexao,'SELECT * FROM tb_dm_products')
tb_ft_stock = executar_consulta(conexao,'SELECT * FROM tb_ft_stock')
tb_ft_freight = executar_consulta(conexao,'SELECT * FROM tb_ft_freight')
tb_dm_orders = executar_consulta(conexao,'SELECT * FROM tb_dm_orders')



conexao = conectar('northwind_gold')
inserir_dados(conexao, 'tb_dm_customers', tb_dm_customers)
inserir_dados(conexao, 'tb_dm_categories', tb_dm_categories)
inserir_dados(conexao, 'tb_dm_employees', tb_dm_employees)
inserir_dados(conexao, 'tb_dm_shippers', tb_dm_shippers)
inserir_dados(conexao, 'tb_dm_territories', tb_dm_territories)
inserir_dados(conexao, 'tb_dm_suppliers', tb_dm_suppliers)
inserir_dados(conexao, 'tb_dm_us_states', tb_dm_us_states)
inserir_dados(conexao, 'tb_dm_region', tb_dm_region)
inserir_dados(conexao, 'tb_dm_employee_territories', tb_dm_employee_territories)
inserir_dados(conexao, 'tb_dm_products', tb_dm_products)
inserir_dados(conexao, 'tb_ft_stock', tb_ft_stock)
inserir_dados(conexao, 'tb_ft_freight', tb_ft_freight)
inserir_dados(conexao, 'tb_dm_orders', tb_dm_orders)


query = '''
SELECT 
	A.*
    ,B.order_date
FROM northwind_silver.tb_ft_orders_details A
LEFT JOIN northwind_silver.tb_dm_orders B
	ON A.order_id = B.order_id
'''
conexao = conectar('northwind_silver')
tb_ft_order_detail = executar_consulta(conexao,query)
conexao = conectar('northwind_gold')
inserir_dados(conexao, 'tb_ft_order_detail', tb_ft_order_detail)

query = '''
    WITH CTE AS (
        select 
            CONCAT(DATE_FORMAT(order_date, '%Y-%m'),'-01') AS year_mon
            ,customer_id
            ,ROUND(SUM(total_value),2) AS total_value_orders
            ,COUNT(DISTINCT order_id) AS orders
            ,COUNT(DISTINCT product_id) AS products_distinct
            ,MIN(order_date) AS min_order_date
            ,MAX(order_date) AS max_order_date
        from tb_ft_order_detail
        group by CONCAT(DATE_FORMAT(order_date, '%Y-%m'),'-01'),customer_id
    )
    ,flg_novo AS (
        SELECT customer_id, MIN(year_mon) AS year_mon_ini
        FROM CTE
        GROUP BY customer_id
    )
    ,CTE_2 AS (
        SELECT A.*
        ,ROUND((total_value_orders/orders),2) AS avg_ticket
        ,CASE WHEN B.customer_id IS NULL THEN 0 ELSE 1 END AS flg_Novo
        FROM CTE A
        LEFT JOIN flg_novo B
            ON A.customer_id = B.customer_id AND A.year_mon = B.year_mon_ini
    )
    SELECT 
        *
    FROM CTE_2

'''

conexao = conectar('northwind_gold')
tb_ft_month_customers = executar_consulta(conexao,query)
inserir_dados(conexao, 'tb_ft_month_customers', tb_ft_month_customers)

query = '''
    WITH CTE AS 
        (SELECT 
            DATE_ADD(year_mon, INTERVAL 3 MONTH) AS safra_3m
            ,customer_id
            ,SUM(total_value_orders) AS total_value_orders
            ,SUM(orders) AS orders
            ,MIN(min_order_date) AS min_order_date
            ,MAX(max_order_date) AS max_order_date
            ,ROUND(SUM(total_value_orders)/SUM(orders),2) AS avg_ticket
        FROM tb_ft_month_customers
        WHERE year_mon BETWEEN year_mon AND DATE_ADD(year_mon, INTERVAL 3 MONTH)
        GROUP BY DATE_ADD(year_mon, INTERVAL 3 MONTH), customer_id
    )
    ,flg_novo AS(
        SELECT customer_id, MIN(safra_3m) as min_safra 
        FROM CTE 
        group by customer_id
    )
    SELECT A.*, CASE WHEN B.customer_id IS NULL THEN 0 ELSE 1 END AS flg_new
    FROM CTE A
    LEFT JOIN  flg_novo B
        ON A.customer_id = B.customer_id  AND A.safra_3m = B.min_safra
'''

conexao = conectar('northwind_gold')
tb_ft_3_month_customers = executar_consulta(conexao,query)
inserir_dados(conexao, 'tb_ft_3_month_customers', tb_ft_3_month_customers)