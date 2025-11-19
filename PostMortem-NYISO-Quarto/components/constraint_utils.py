import pandas as pd
from typing import List, Tuple, Any
from services import snowflake_queries as sq
from components import graph_utils as gu
from itables import show

def df_to_cid_ces_and_package_str(df_cid_ces_packid:pd.DataFrame):
    cid_ces_str=','.join(map(str, df_cid_ces_packid['MIN_CID_CES'].unique().tolist()))
    packid_str=','.join(map(str, df_cid_ces_packid['MAG_REF_PACKAGEVERSION__ID'].unique().tolist()))
    return (cid_ces_str,packid_str)

def df_to_scenario_id(df_scenario_id:pd.DataFrame):
    scenario_id=','.join(map(str, df_scenario_id['MAG_REF_SCENARIO_INFO__ID'].unique().tolist()))
    return scenario_id

def get_cdd_data (cid_mag: int
                  ,pool_id: int
                  ,scenario: List[str]
                  ,scenario_sf:List[str]
                  ,scenario_histo_sp:List[str]
                  ,mindate: str
                  ,maxdate: str
                  ,_conn: any) -> Tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    """
    Get flows, categories and outages data
    """
    df_cid_ces_package_str=sq.get_cid_ces_packageid_from_cid_mag(pool_id,cid_mag,_conn)

    df_scenario_id=sq.get_scenario_id(scenario,_conn)
    df_scenario_id_sf=sq.get_scenario_id(scenario_sf,_conn)
    df_scenario_id_sp=sq.get_scenario_id(scenario_histo_sp,_conn)

    scenario_id=df_to_scenario_id(df_scenario_id)
    scenario_id_sf=df_to_scenario_id(df_scenario_id_sf)
    scenario_id_sp=df_to_scenario_id(df_scenario_id_sp)

    cid_ces_str,packid_str=df_to_cid_ces_and_package_str(df_cid_ces_package_str)

    df_histo_SP=sq.get_historical_SP(pool_id
                                     ,cid_mag
                                     ,scenario_id_sp
                                     ,_conn) 

    df_flows=sq.get_flows(cid_mag
                        ,pool_id
                        ,cid_ces_str
                        ,packid_str
                        ,scenario_id
                        ,mindate
                        ,maxdate
                        ,_conn)

    df_catego=sq.get_catego(cid_mag
                        ,cid_ces_str
                        ,packid_str
                        ,pool_id
                        ,scenario_id_sf
                        ,mindate
                        ,maxdate
                        ,_conn)

    df_outages=sq.get_outages(cid_mag
                        ,pool_id
                        ,scenario_sf
                        ,mindate
                        ,maxdate
                        ,_conn)
    
    return(df_flows,df_catego,df_outages,df_histo_SP)

def table_nb_hour_bind(pool_id: int,cid_mag: int,mindate: str,maxdate: str,_conn: any):
    df_nb_hour_bind=sq.get_nb_hour_bind(pool_id
                        ,cid_mag
                        ,mindate
                        ,maxdate
                        ,_conn)
    
    df_nb_hour_bind.fillna(0,inplace=True)

    # print("pool_id ", pool_id, "\n")

    if pool_id == 3: # NEPOOL
        # print("In block \n")
        allowed_scenarios = ['NEPOOL_1MA_Default', 'NEPOOL_1DA_Default', 'SP_DA', 'SP_RT', 'NEPOOL_1MA_AvgHistSP', 'NEPOOL_1DA_AvgHistSP',
                             'NEPOOL_1MA_DL_AvgSP', 'NEPOOL_1DA_DL_AvgSP']
        if allowed_scenarios is not None:
            df_nb_hour_bind = df_nb_hour_bind[df_nb_hour_bind["SCENARIONAME"].isin(allowed_scenarios)]


    # Apply formatting to all other columns
    for col in df_nb_hour_bind.columns:
        if col in ["SP","SP_PER_HOUR"]:
            df_nb_hour_bind[col] = df_nb_hour_bind[col].apply(lambda x: f"${x:,}")


    show(df_nb_hour_bind,
        classes="compact",
        style="font-size: 12px;",
        )
    
def get_all_cstr_data(pool_id: int,
                      cid_mag: int,
                      ftrstartdate: str,
                      ftrenddate: str,
                      histostartdate: str,
                      histoenddate: str,
                      scenario_flows:List[str],
                      scenario_first_priority:str,
                      scenario_sf:List[str],
                      scenario_histo_sp:List[str],
                      _conn: Any
                      ):
    """
    On function to create all the necessary graph for the PM
    """
    table_nb_hour_bind(pool_id
                        ,cid_mag
                        ,ftrstartdate
                        ,ftrenddate
                        ,_conn)

    df_flows,df_catego,df_outages,df_histo_SP=get_cdd_data(cid_mag
                        ,pool_id
                        ,scenario_flows
                        ,scenario_sf
                        ,scenario_histo_sp
                        ,histostartdate
                        ,histoenddate
                        ,_conn)
    
    gu.shadowprice_monthly_fig(df_histo_SP,
                          cid_mag,
                          )
    
    if pool_id == 3:
        # Remove the 1MA_AvgHistSP scenario for the hourly flow, category and
        # transmission outage plots.
        SCENARIOS_TO_REMOVE = {'NEPOOL_1MA_AvgHistSP'}    # or any scenarios you want removed

        df_flows  = df_flows[~df_flows['SCENARIONAME'].isin(SCENARIOS_TO_REMOVE)]
        df_catego = df_catego[~df_catego['SCENARIONAME'].isin(SCENARIOS_TO_REMOVE)]
        df_outages = df_outages[~df_outages['SCENARIONAME'].isin(SCENARIOS_TO_REMOVE)]


    gu.hourly_figure( df_flows,
                  df_catego,
                  df_outages,
                  scenario_first_priority,
                  histostartdate,
                  histoenddate
                  )