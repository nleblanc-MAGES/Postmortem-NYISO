from services import snowflake_queries as sq
from components import graph_utils as gu, constraint_utils as cu
import plotly.graph_objects as go
from services.database_connection import init_connection
from itables import show, JavascriptFunction
import ipywidgets as widgets

conn=init_connection()

pool_id=5
ftrstartdate='2025-06-01'
ftrenddate='2025-06-30'
histostartdate='2025-03-01'
histoenddate='2025-06-30'
scenario_flows=['ERCOT_1MA_Default','ERCOT_1DA_Default','ERCOT_1MA_DA_LIM','ERCOT_1MA_Outages']
scenario_first_priority='ERCOT_1DA_Default'
scenario_sf=['ERCOT_1MA_Default','ERCOT_1DA_Default','ERCOT_1MA_Outages']
scenario_histo_sp=['ERCOT_1MA_Default',
                    'ERCOT_1DA_Default',
                    'ERCOT_1MA_DA_LIM',
                    'ERCOT_1MA_LoadMax',
                    'ERCOT_1MA_NerdDog',
                    'ERCOT_1MA_NerdDogPerCluster_V1'
                    ]

cu.get_all_cstr_data(pool_id,
                      50011660,
                      ftrstartdate,
                      ftrenddate,
                      histostartdate,
                      histoenddate,
                      scenario_flows,
                      scenario_first_priority,
                      scenario_sf,
                      scenario_histo_sp,
                      conn
                      )