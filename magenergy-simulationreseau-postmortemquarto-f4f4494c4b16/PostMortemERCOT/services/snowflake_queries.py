import pandas as pd  # Assuming the result is a Pandas DataFrame
from Snowflake_Natif_Connector import conn_python_snowflake as ntf
from typing import Any,List, Callable


def query_to_df(query:str ,_conn: Any) -> pd.DataFrame:
    """
    Run a query in snowflake and return the result in a Dataframe

    Parameters:
        query (str): the query to run
        conn (Any): Snowflake connection object.

    Returns:
        pd.DataFrame: Result of the query as a DataFrame.
    """
    return ntf.executeQueryNatif(query,_conn)

def get_Load(Scenarios,StartDate,EndDate,_conn: Any) -> pd.DataFrame:
    """
    get Load for a list of scenarios
    """
    query="""WITH ZONE_DATA AS (
             select SCENARIONAME,
              ZONENAME,
              HEDATE,
              DEMANDMW 
              from MAGSNOWFLAKE.DAYZER_CUBES.ZONES_RESULTS_HOURLY
              where SCENARIONAME IN (select value from table(flatten(input=>{0})))
              AND ((ZONETYPE<>'IndustrialLoad') OR (ZONETYPE is null))
              AND DATE between '{1}' and '{2}'
              UNION ALL
              select SCENARIONAME,
              'TOTAL'  AS ZONENAME,
              HEDATE,
              SUM(DEMANDMW) AS DEMANDMW
              from MAGSNOWFLAKE.DAYZER_CUBES.ZONES_RESULTS_HOURLY
              where SCENARIONAME IN (select value from table(flatten(input=>{0})))
              AND DATE between '{1}' and '{2}'
              AND ((ZONETYPE<>'IndustrialLoad') OR (ZONETYPE is null))
              group by SCENARIONAME,HEDATE
              )
              select * from ZONE_DATA 
              order by HEDATE
              ;
    """.format(Scenarios,StartDate,EndDate)
    return ntf.executeQueryNatif(query,_conn)

def get_Wind(Scenarios,StartDate,EndDate,_conn: any) -> pd.DataFrame:
    query="""select SCENARIONAME,HEDATE,SUM(GENERATIONMW) AS WIND_GEN
             from MAGSNOWFLAKE.DAYZER_CUBES.UNITS_RESULTS_HOURLY
             where SCENARIONAME IN (select value from table(flatten(input=>{0})))
             AND DATE between '{1}' and '{2}'
             AND FUELNAME='Wind'
             --AND ZONE ='WEST ERCOT'
             group by SCENARIONAME,HEDATE
             order by HEDATE
              ;
    """.format(Scenarios,StartDate,EndDate)
    return ntf.executeQueryNatif(query,_conn)

def get_PostMortem(pool_id: int, start_date: str, end_date: str, scenario: List[str], _conn: Any) -> pd.DataFrame:
    """
    Executes the Post Mortem query on Snowflake and returns the result.

    Parameters:
        pool_id (int): The pool ID to filter the query.
        start_date (str): The start date for the query in YYYY-MM-DD format.
        end_date (str): The end date for the query in YYYY-MM-DD format.
        product (List): List of product name of scenario you want to use.
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """
    query = f"""
    SET PackId=(SELECT MAX(MAG_REF_PackageVersion__ID) FROM MAGSNOWFLAKE.DAYZER_CUBES.NODES_RESULTS_MONTHLY WHERE DATE = '{start_date}' AND MAG_REF_POOL__ID={pool_id});

    CREATE OR REPLACE TEMPORARY TABLE RESULT_MKT_DA AS 
    SELECT 
        CID_MAG
        ,CID_CES
        , CONSTRAINTNAME
       -- , FACILITYNAME
        , CONTINGENCYNAME
        , CAST(SUM(SHADOWPRICE) AS INT) AS SP_DA
    FROM (
        SELECT DISTINCT
            MAG_REF_POOL__ID, PEAKID, POOLNAME, DATE, HE, CID_MAG, CID_CES,
            CONSTRAINTNAME, FACILITYNAME, CONTINGENCYNAME, SHADOWPRICE
        FROM MAGSNOWFLAKE.DAYZER.PROD_DA_CONSTRAINTS_MAPPED
        WHERE MAG_REF_POOL__ID={pool_id}
        AND DATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND MAG_REF_PACKAGEVERSION__ID=$PackId
    )
    GROUP BY 
        CID_MAG, CID_CES,CONSTRAINTNAME, FACILITYNAME, CONTINGENCYNAME;

    CREATE OR REPLACE TEMPORARY TABLE RESULT_DZR AS 
    SELECT 
        CONSTRAINTMAPPING_MAG_REF__ID AS CID_MAG
        ,CONSTRAINTMAPPING_DAYZER_REF__ID AS CID_CES
        ,CONSTRAINTMAPPING_DAYZER_REF__NAME AS CES_NAME
        ,SCENARIONAME AS PIVOT_COLUMN
        ,CAST(SUM(ABS(SHADOWPRICE)) AS INT) AS PIVOT_VALUE
    FROM 
        MAGSNOWFLAKE.DAYZER.VWMAG_CONSTRAINTS_RESULTS_MONTHLY
    WHERE 
        MAG_REF_POOL__ID={pool_id}
        AND SHADOWPRICE<>0
        AND SCENARIONAME IN (select value from table(flatten(input=>{scenario}))
                            )
        AND MONTH between DATE('{start_date}') AND DATE('{end_date}')
    GROUP BY 
        CONSTRAINTMAPPING_MAG_REF__ID
        ,CONSTRAINTMAPPING_DAYZER_REF__ID
        ,CONSTRAINTMAPPING_DAYZER_REF__NAME 
        ,SCENARIONAME
    ;

    CREATE OR REPLACE TEMPORARY TABLE RESULT_PIVOT AS 
    SELECT 
        * 
    FROM 
        RESULT_DZR PIVOT (SUM(PIVOT_VALUE) FOR PIVOT_COLUMN IN (ANY ORDER BY PIVOT_COLUMN))
    ;

    select 
        A.* 
        ,B.* EXCLUDE(CID_MAG,CES_NAME,CID_CES)
    from 
        RESULT_MKT_DA A
    LEFT JOIN
        RESULT_PIVOT B
    ON
        A.CID_MAG=B.CID_MAG
    order by 
        ABS(SP_DA) DESC;

    """
    return ntf.executeQueryNatif(query, _conn)

def get_flows_old(cid_mag: int,
              pool_id: int,
              scenario: List[str],
              Mindate :str,
              Maxdate :str,
              _conn: Any) -> pd.DataFrame:
    """
    Get the flows hourly for a given constraint, a timeframe and a list of scenarios

    Parameters:
        cid_mag (int): unique cid of the constraint.
        pool_id (int): pool_id of the constraint
        Mindate (str): The Mindate to take date for the query in YYYY-MM-DD format.
        Maxdate (str): The Maxdate to take date for the query in YYYY-MM-DD format.
        scenario (List): List of scenario you want to use.
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """
    query=f"""

    WITH DEFINITION AS (
    select
        MAG_REF_POOL__ID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS,'_',1) AS MONITOREDDAYZERELEMENTIDS
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS_DIR,'_',1) AS MONITOREDDAYZERELEMENTIDS_DIR
    from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_CONSTRAINTS_DETAILS A
    WHERE (MAG_REF_POOL__ID ={pool_id}
    OR
    MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                            from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT where MAG_REF_POOL__ID={pool_id}
                            )
    )
    AND TRY_TO_NUMBER(MONITOREDDAYZERELEMENTIDS) IS NOT NULL
    )
    
    ,TRANSMISSION_ELEMENTS AS (
     select
        MAG_REF_POOL__ID
        ,MAG_REF_PACKAGEVERSION__ID
        ,DAYZERELEMENTID
        ,FROMBUSNAME
        ,TOBUSNAME
     from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_TRANSMISSION_ELEMENTS_DETAILS   
    WHERE (MAG_REF_POOL__ID ={pool_id}
    OR
    MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                            from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT where MAG_REF_POOL__ID={pool_id}
                            )
    )    
    )
    ,DEFINITION_WITH_TE AS (
    select 
        A.MAG_REF_POOL__ID
        ,CES_CID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,FROMBUSNAME,TOBUSNAME) AS FROMBUSNAME
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,TOBUSNAME,FROMBUSNAME) AS TOBUSNAME
    from 
        DEFINITION A
    INNER JOIN 
        TRANSMISSION_ELEMENTS B
    ON 
        A.MONITOREDDAYZERELEMENTIDS=B.DAYZERELEMENTID
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,MAG_CID_LINK AS (
    SELECT 
        MAG_CID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,MAG_REF_POOL__ID
    FROM 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC
    WHERE
        MAG_CID={cid_mag}
    )

   ,SCENARIO_DZR_A AS (
    select distinct 
        SCENARIONAME
        ,HEDATE
        ,CONSTRAINTMAPPING_DAYZER_REF__ID
        ,FLOWS
        ,ABS(SHADOWPRICE) AS SP_DZR
        ,IFF(ABS(MINFLOWLIMIT)>9999,NULL,MINFLOWLIMIT) AS MINLIMIT
        ,IFF(ABS(MAXFLOWLIMIT)>9999,NULL,MAXFLOWLIMIT) AS MAXLIMIT
        ,SIMULATIONDATE
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,A.MAG_REF_POOL__ID
    FROM 
        MAGSNOWFLAKE.DAYZER_CUBES.CONSTRAINTS_RESULTS_HOURLY  A
    WHERE 
        SCENARIONAME IN (select value from table(flatten(input=>{scenario})))
        AND A.MAG_REF_POOL__ID ={pool_id}
        AND CAST(DATEADD(HOUR,-1,HEDATE) AS DATE) between date('{Mindate}') AND date('{Maxdate}')
    )
    
    ,SCENARIO_DZR_B AS (
    select
        SCENARIONAME
        ,HEDATE
        ,MAG_CID
        ,CES_CID
        ,FLOWS
        ,SP_DZR
        ,MINLIMIT
        ,MAXLIMIT
        ,SIMULATIONDATE
        ,A.MAG_REF_PACKAGEVERSION__ID
    FROM
        SCENARIO_DZR_A A
    INNER JOIN 
        MAG_CID_LINK B
    ON 
        A.CONSTRAINTMAPPING_DAYZER_REF__ID=B.CES_CID 
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID 
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,AVOID_DOUBLONS AS (
    select 
        HEDATE,MAG_CID,SCENARIONAME,MIN(CES_CID) AS MIN_CID_CES
    from 
        SCENARIO_DZR_B
    GROUP BY 
        HEDATE,MAG_CID,SCENARIONAME
    )
    
    ,SCENARIO_DZR AS (
    select 
        A.* 
    from 
        SCENARIO_DZR_B A
    INNER JOIN 
        AVOID_DOUBLONS B
    ON A.HEDATE=B.HEDATE 
    AND A.CES_CID=B.MIN_CID_CES
    AND A.SCENARIONAME=B.SCENARIONAME
    )

    ,MKT_RESULTS AS (
    select distinct 
        CID_MAG,DATE,HE,SHADOWPRICE AS SP_DA
    from (select distinct
            MAG_REF_POOL__ID,POOLNAME,DATE,HE,CID_MAG,CONSTRAINTNAME,FACILITYNAME,CONTINGENCYNAME,SHADOWPRICE
            from MAGSNOWFLAKE.DAYZER.PROD_DA_CONSTRAINTS_MAPPED
            where MAG_REF_POOL__ID={pool_id}
            AND CID_MAG={cid_mag}
        )
    )

    ,MKT_RESULTS_RT AS (
    select distinct CID_MAG,DATE,HE,SP_RT
     from (select distinct
        MAG_REF_POOL__ID,POOLNAME,DATE,HE,CID_MAG,CONSTRAINTNAME,FACILITYNAME,CONTINGENCYNAME,SP_RT
        from MAGSNOWFLAKE.DAYZER.PROD_RT_CONSTRAINTS_MAPPED
        where MAG_REF_POOL__ID={pool_id}
        AND CID_MAG={cid_mag})
    ) 
    
    select 
        SCENARIONAME
        ,HEDATE
        ,MAG_CID
        ,FLOWS
        ,ABS(SP_DZR) AS SP_DZR
        ,ABS(IFNULL(SP_DA,0)) AS SP_DA
        ,ABS(IFNULL(SP_RT,0)) AS SP_RT
        ,IFF(ABS(MINLIMIT)>8000,NULL,MINLIMIT) AS MINLIMIT
        ,IFF(ABS(MAXLIMIT)>8000,NULL,MAXLIMIT) AS MAXLIMIT
        ,SIMULATIONDATE
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,A.CES_CID
        ,FROMBUSNAME
        ,TOBUSNAME
    from 
        SCENARIO_DZR A
    LEFT JOIN 
        MKT_RESULTS B
    ON A.HEDATE=DATEADD(HOUR,B.HE,B.DATE)
    LEFT JOIN 
        MKT_RESULTS_RT C
    ON A.HEDATE=DATEADD(HOUR,C.HE,C.DATE)
    LEFT JOIN 
        DEFINITION_WITH_TE D
    ON 
        A.MAG_REF_PACKAGEVERSION__ID=D.MAG_REF_PACKAGEVERSION__ID
        AND A.CES_CID=D.CES_CID
    order by HEDATE
    """
    return ntf.executeQueryNatif(query,_conn) 

def get_flows(cid_mag: int,
              pool_id: int,
              cid_ces_str: str, 
              packid_str: str,
              Scenario_id: List[int],
              Mindate :str,
              Maxdate :str,
              _conn: Any) -> pd.DataFrame:
    """
    Get the flows hourly for a given constraint, a timeframe and a list of scenarios

    Parameters:
        cid_mag (int): unique cid of the constraint.
        pool_id (int): pool_id of the constraint
        cid_ces_str (str): str of all the cid_ces involved.
        packid_str (str): str of all the package_id involved.
        Mindate (str): The Mindate to take date for the query in YYYY-MM-DD format.
        Maxdate (str): The Maxdate to take date for the query in YYYY-MM-DD format.
        product (List): List of scenario_id you want to use.
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """
    query=f"""
    ALTER SESSION SET QUERY_TAG = 'NERD_MONKEY';

    WITH DEFINITION AS (
    select
        MAG_REF_POOL__ID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS,'_',1) AS MONITOREDDAYZERELEMENTIDS
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS_DIR,'_',1) AS MONITOREDDAYZERELEMENTIDS_DIR
    from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_CONSTRAINTS_DETAILS A
    WHERE (MAG_REF_POOL__ID ={pool_id}
    OR
    MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                            from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT where MAG_REF_POOL__ID={pool_id}
                            )
    )
    AND TRY_TO_NUMBER(MONITOREDDAYZERELEMENTIDS) IS NOT NULL
    AND CES_CID IN ({cid_ces_str})
    AND MAG_REF_PACKAGEVERSION__ID IN ({packid_str})
    )
    
    ,TRANSMISSION_ELEMENTS AS (
     select
        MAG_REF_POOL__ID
        ,MAG_REF_PACKAGEVERSION__ID
        ,DAYZERELEMENTID
        ,FROMBUSNAME
        ,TOBUSNAME
     from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_TRANSMISSION_ELEMENTS_DETAILS   
    WHERE (MAG_REF_POOL__ID ={pool_id}
    OR
    MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                            from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT where MAG_REF_POOL__ID={pool_id}
                            )
    )    
    )
    ,DEFINITION_WITH_TE AS (
    select 
        A.MAG_REF_POOL__ID
        ,CES_CID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,FROMBUSNAME,TOBUSNAME) AS FROMBUSNAME
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,TOBUSNAME,FROMBUSNAME) AS TOBUSNAME
    from 
        DEFINITION A
    INNER JOIN 
        TRANSMISSION_ELEMENTS B
    ON 
        A.MONITOREDDAYZERELEMENTIDS=B.DAYZERELEMENTID
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,MAG_CID_LINK AS (
    SELECT 
        MAG_CID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,MAG_REF_POOL__ID
    FROM 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC
    WHERE
        MAG_CID={cid_mag}
    )

   ,SCENARIO_DZR_A AS (
    select distinct 
        SCENARIONAME
        ,HEDATE
        ,CONSTRAINTMAPPING_DAYZER_REF__ID
        ,FLOWS
        ,ABS(SHADOWPRICE) AS SP_DZR
        ,IFF(ABS(MINFLOWLIMIT)>9999,NULL,MINFLOWLIMIT) AS MINLIMIT
        ,IFF(ABS(MAXFLOWLIMIT)>9999,NULL,MAXFLOWLIMIT) AS MAXLIMIT
        ,SIMULATIONDATE
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,A.MAG_REF_POOL__ID
    FROM 
        MAGSNOWFLAKE.DAYZER_CUBES.CONSTRAINTS_RESULTS_HOURLY  A
    WHERE 
        MAG_REF_SCENARIO_INFO__ID IN ({Scenario_id})
        AND A.MAG_REF_POOL__ID ={pool_id}
        AND A.CONSTRAINTMAPPING_DAYZER_REF__ID IN ({cid_ces_str})
        AND A.MAG_REF_PACKAGEVERSION__ID IN ({packid_str})
        AND CAST(DATEADD(HOUR,-1,HEDATE) AS DATE) between date('{Mindate}') AND date('{Maxdate}')
    )
    
    ,SCENARIO_DZR_B AS (
    select
        SCENARIONAME
        ,HEDATE
        ,MAG_CID
        ,CES_CID
        ,FLOWS
        ,SP_DZR
        ,MINLIMIT
        ,MAXLIMIT
        ,SIMULATIONDATE
        ,A.MAG_REF_PACKAGEVERSION__ID
    FROM
        SCENARIO_DZR_A A
    INNER JOIN 
        MAG_CID_LINK B
    ON 
        A.CONSTRAINTMAPPING_DAYZER_REF__ID=B.CES_CID 
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID 
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,AVOID_DOUBLONS AS (
    select 
        HEDATE,MAG_CID,SCENARIONAME,MIN(CES_CID) AS MIN_CID_CES
    from 
        SCENARIO_DZR_B
    GROUP BY 
        HEDATE,MAG_CID,SCENARIONAME
    )
    
    ,SCENARIO_DZR AS (
    select 
        A.* 
    from 
        SCENARIO_DZR_B A
    INNER JOIN 
        AVOID_DOUBLONS B
    ON A.HEDATE=B.HEDATE 
    AND A.CES_CID=B.MIN_CID_CES
    AND A.SCENARIONAME=B.SCENARIONAME
    )

    ,MKT_RESULTS AS (
    select distinct 
        CID_MAG,DATE,HE,SHADOWPRICE AS SP_DA
    from (select distinct
            MAG_REF_POOL__ID,POOLNAME,DATE,HE,CID_MAG,CONSTRAINTNAME,FACILITYNAME,CONTINGENCYNAME,SHADOWPRICE
            from MAGSNOWFLAKE.DAYZER.PROD_DA_CONSTRAINTS_MAPPED
            where MAG_REF_POOL__ID={pool_id}
            AND CID_MAG={cid_mag}
        )
    )

    ,MKT_RESULTS_RT AS (
    select distinct CID_MAG,DATE,HE,SP_RT
     from (select distinct
        MAG_REF_POOL__ID,POOLNAME,DATE,HE,CID_MAG,CONSTRAINTNAME,FACILITYNAME,CONTINGENCYNAME,SP_RT
        from MAGSNOWFLAKE.DAYZER.PROD_RT_CONSTRAINTS_MAPPED
        where MAG_REF_POOL__ID={pool_id}
        AND CID_MAG={cid_mag})
    ) 
    
    select 
        SCENARIONAME
        ,HEDATE
        ,MAG_CID
        ,FLOWS
        ,ABS(SP_DZR) AS SP_DZR
        ,ABS(IFNULL(SP_DA,0)) AS SP_DA
        ,ABS(IFNULL(SP_RT,0)) AS SP_RT
        ,IFF(ABS(MINLIMIT)>8000,NULL,MINLIMIT) AS MINLIMIT
        ,IFF(ABS(MAXLIMIT)>8000,NULL,MAXLIMIT) AS MAXLIMIT
        ,SIMULATIONDATE
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,A.CES_CID
        ,FROMBUSNAME
        ,TOBUSNAME
    from 
        SCENARIO_DZR A
    LEFT JOIN 
        MKT_RESULTS B
    ON A.HEDATE=DATEADD(HOUR,B.HE,B.DATE)
    LEFT JOIN 
        MKT_RESULTS_RT C
    ON A.HEDATE=DATEADD(HOUR,C.HE,C.DATE)
    LEFT JOIN 
        DEFINITION_WITH_TE D
    ON 
        A.MAG_REF_PACKAGEVERSION__ID=D.MAG_REF_PACKAGEVERSION__ID
        AND A.CES_CID=D.CES_CID
    order by HEDATE
    """
    return ntf.executeQueryNatif(query,_conn) 

def get_cid_ces_packageid_from_cid_mag(pool_id: int,cid_mag: int,_conn: any) -> pd.DataFrame:
    """
    getting all ces id and packageid for a mag_cid

    Parameters:
        pool_id (int): pool_id
        cid_mag (int): cid_mag
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """ 
    query=f"""
    WITH TEMP_A AS (
    select 
        MAG_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,MIN(CES_CID) AS MIN_CID_CES
    from 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC
    where 
        MAG_REF_POOL__ID={pool_id}
        AND MAG_CID={cid_mag}
    group by 
        MAG_CID
        ,MAG_REF_PACKAGEVERSION__ID
    )
    
    select distinct 
        MIN_CID_CES,MAG_REF_PACKAGEVERSION__ID
    from 
        TEMP_A;
    
    """
    return ntf.executeQueryNatif(query,_conn)

def get_catego_old(cid_mag: int,
               pool_id: int, 
               scenario: List[str],
               mindate: str,
               maxdate: str,
               _conn: any) -> pd.DataFrame:
    """
    Get the category for a period and different scenario

    Parameters:
        cid_mag (int): unique cid of the constraint.
        pool_id (int): pool_id of the constraint.
        scenario (List): list of scenario you want to see
        mindate (str): first date of the interval.
        maxdate (str): last date of the interval.
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """
    query=f"""

    WITH DEFINITION AS (
    select
        MAG_REF_POOL__ID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS,'_',1) AS MONITOREDDAYZERELEMENTIDS
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS_DIR,'_',1) AS MONITOREDDAYZERELEMENTIDS_DIR
    from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_CONSTRAINTS_DETAILS A
    WHERE (
        MAG_REF_POOL__ID ={pool_id}
    OR
        MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                                from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT 
                                where MAG_REF_POOL__ID={pool_id}
                                )
    )
        AND TRY_TO_NUMBER(MONITOREDDAYZERELEMENTIDS) IS NOT NULL
    )

    ,TRANSMISSION_ELEMENTS AS (
     select
        MAG_REF_POOL__ID
        ,MAG_REF_PACKAGEVERSION__ID
        ,DAYZERELEMENTID
        ,FROMBUSNAME
        ,TOBUSNAME
     from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_TRANSMISSION_ELEMENTS_DETAILS   
    WHERE (MAG_REF_POOL__ID ={pool_id}
    OR
    MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                            from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT where MAG_REF_POOL__ID={pool_id}
                            )
    )
    )    
    
    ,DEFINITION_WITH_TE AS (
    select 
        A.MAG_REF_POOL__ID
        ,CES_CID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,FROMBUSNAME,TOBUSNAME) AS FROMBUSNAME
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,TOBUSNAME,FROMBUSNAME) AS TOBUSNAME
    from 
        DEFINITION A
    INNER JOIN 
        TRANSMISSION_ELEMENTS B
    ON 
        A.MONITOREDDAYZERELEMENTIDS=B.DAYZERELEMENTID
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,MAG_CID_LINK AS (
    SELECT 
        MAG_CID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,MAG_REF_POOL__ID
    FROM 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC
    WHERE
        MAG_CID={cid_mag}
    
    )
    ,SCENARIO_DZR_A AS (
    SELECT
        SCENARIONAME 
        ,MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,CONSTRAINTID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,A.MAG_REF_POOL__ID
        ,WIND_IMPACT AS WIND
        ,SOLAR_IMPACT AS SOLAR
        ,HYDRO_IMPACT AS HYDRO
        ,GEO_IMPACT AS GEO
        ,IE_IMPACT AS IE
        ,NRGEN_IMPACT AS OTHERS_UNITS
        ,LOAD_IMPACT AS LOAD
        ,INDUSTRIALLOAD_IMPACT AS INDL_LOAD
    FROM 
        MAGSNOWFLAKE.dayzer_cubes.category_results_hourly A
    where 
        SCENARIONAME IN (select value from table(flatten(input=>{scenario})))
        AND CAST(DATEADD(HOUR,-1,HEDATE) AS DATE) between date('{mindate}') AND date('{maxdate}')
    )

    ,SCENARIO_DZR_B AS (
    select
        SCENARIONAME 
        ,MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,MAG_CID
        ,CES_CID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,WIND
        ,SOLAR
        ,HYDRO
        ,GEO
        ,IE
        ,OTHERS_UNITS
        ,LOAD
        ,INDL_LOAD
    FROM
        SCENARIO_DZR_A A
    INNER JOIN 
        MAG_CID_LINK B
    ON 
        A.CONSTRAINTID=B.CES_CID 
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID 
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,AVOID_DOUBLONS AS (
    select 
        MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,MAG_CID
        ,MIN(CES_CID) AS MIN_CID_CES
    from 
        SCENARIO_DZR_B 
    GROUP BY 
        MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,MAG_CID
    )

    ,FINAL_CATEGORY_TABLE AS (
    select 
        A.* 
    from 
        SCENARIO_DZR_B A
    INNER JOIN 
        AVOID_DOUBLONS B
    ON 
        A.HEDATE=B.HEDATE 
        AND A.CES_CID=B.MIN_CID_CES 
        AND A.MAG_REF_SCENARIO_INFO__ID=B.MAG_REF_SCENARIO_INFO__ID
    )

    select 
        A.* 
        ,B.FROMBUSNAME
        ,B.TOBUSNAME
    from 
        FINAL_CATEGORY_TABLE A
    LEFT JOIN 
        DEFINITION_WITH_TE B
    ON 
        A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
        AND A.CES_CID=B.CES_CID
    ORDER BY
        A.HEDATE

        """
    return ntf.executeQueryNatif(query,_conn)

def get_catego(cid_mag: int,
               cid_ces_str: str, 
               packid_str: str,
               pool_id: int, 
               scenario_id: List[int],
               mindate: str,
               maxdate: str,
               _conn: any) -> pd.DataFrame:
    """
    Get the category for a period and different scenario

    Parameters:
        cid_mag (int): unique cid of the constraint.
        cid_ces_str: str, 
        packid_str: str,
        pool_id (int): pool_id of the constraint.
        scenario_id (List): list of scenario you want to see
        mindate (str): first date of the interval.
        maxdate (str): last date of the interval.
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """
    query=f"""
    ALTER SESSION SET QUERY_TAG = 'NERD_MONKEY';

    WITH DEFINITION AS (
    select
        MAG_REF_POOL__ID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS,'_',1) AS MONITOREDDAYZERELEMENTIDS
        ,SPLIT_PART(MONITOREDDAYZERELEMENTIDS_DIR,'_',1) AS MONITOREDDAYZERELEMENTIDS_DIR
    from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_CONSTRAINTS_DETAILS A
    WHERE (
        MAG_REF_POOL__ID ={pool_id}
    OR
        MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                                from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT 
                                where MAG_REF_POOL__ID={pool_id}
                                )
    )
        AND TRY_TO_NUMBER(MONITOREDDAYZERELEMENTIDS) IS NOT NULL
        AND CES_CID IN ({cid_ces_str})
        AND MAG_REF_PACKAGEVERSION__ID IN ({packid_str})
    )

    ,TRANSMISSION_ELEMENTS AS (
     select
        MAG_REF_POOL__ID
        ,MAG_REF_PACKAGEVERSION__ID
        ,DAYZERELEMENTID
        ,FROMBUSNAME
        ,TOBUSNAME
     from 
        MAGSQLSERVER.DAYZERSTUDY.REF_DAYZER_TRANSMISSION_ELEMENTS_DETAILS   
    WHERE (MAG_REF_POOL__ID ={pool_id}
    OR
    MAG_REF_POOL__ID  IN (select distinct MAG_REF_POOLHYBRID__ID 
                            from MAGSNOWFLAKE.DAYZER.LINK_HYBRID_MKT where MAG_REF_POOL__ID={pool_id}
                            )
    )
    )    
    
    ,DEFINITION_WITH_TE AS (
    select 
        A.MAG_REF_POOL__ID
        ,CES_CID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,FROMBUSNAME,TOBUSNAME) AS FROMBUSNAME
        ,IFF(MONITOREDDAYZERELEMENTIDS_DIR=1,TOBUSNAME,FROMBUSNAME) AS TOBUSNAME
    from 
        DEFINITION A
    INNER JOIN 
        TRANSMISSION_ELEMENTS B
    ON 
        A.MONITOREDDAYZERELEMENTIDS=B.DAYZERELEMENTID
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,MAG_CID_LINK AS (
    SELECT 
        MAG_CID
        ,CES_CID
        ,MAG_REF_PACKAGEVERSION__ID
        ,MAG_REF_POOL__ID
    FROM 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC
    WHERE
        MAG_CID={cid_mag}
    
    )
    ,SCENARIO_DZR_A AS (
    SELECT
        SCENARIONAME 
        ,MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,CONSTRAINTID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,A.MAG_REF_POOL__ID
        ,WIND_IMPACT AS WIND
        ,SOLAR_IMPACT AS SOLAR
        ,HYDRO_IMPACT AS HYDRO
        ,GEO_IMPACT AS GEO
        ,IE_IMPACT AS IE
        ,NRGEN_IMPACT AS OTHERS_UNITS
        ,LOAD_IMPACT AS LOAD
        ,INDUSTRIALLOAD_IMPACT AS INDL_LOAD
    FROM 
        MAGSNOWFLAKE.dayzer_cubes.category_results_hourly A
    where 
        MAG_REF_SCENARIO_INFO__ID IN ({scenario_id})
        AND A.CONSTRAINTID IN ({cid_ces_str})
        AND A.MAG_REF_PACKAGEVERSION__ID IN ({packid_str})
        AND CAST(DATEADD(HOUR,-1,HEDATE) AS DATE) between date('{mindate}') AND date('{maxdate}')
    )

    ,SCENARIO_DZR_B AS (
    select
        SCENARIONAME 
        ,MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,MAG_CID
        ,CES_CID
        ,A.MAG_REF_PACKAGEVERSION__ID
        ,WIND
        ,SOLAR
        ,HYDRO
        ,GEO
        ,IE
        ,OTHERS_UNITS
        ,LOAD
        ,INDL_LOAD
    FROM
        SCENARIO_DZR_A A
    INNER JOIN 
        MAG_CID_LINK B
    ON 
        A.CONSTRAINTID=B.CES_CID 
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID 
        AND A.MAG_REF_POOL__ID=B.MAG_REF_POOL__ID
    
    )

    ,AVOID_DOUBLONS AS (
    select 
        MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,MAG_CID
        ,MIN(CES_CID) AS MIN_CID_CES
    from 
        SCENARIO_DZR_B 
    GROUP BY 
        MAG_REF_SCENARIO_INFO__ID
        ,HEDATE
        ,MAG_CID
    )

    ,FINAL_CATEGORY_TABLE AS (
    select 
        A.* 
    from 
        SCENARIO_DZR_B A
    INNER JOIN 
        AVOID_DOUBLONS B
    ON 
        A.HEDATE=B.HEDATE 
        AND A.CES_CID=B.MIN_CID_CES 
        AND A.MAG_REF_SCENARIO_INFO__ID=B.MAG_REF_SCENARIO_INFO__ID
    )

    select 
        A.* 
        ,B.FROMBUSNAME
        ,B.TOBUSNAME
    from 
        FINAL_CATEGORY_TABLE A
    LEFT JOIN 
        DEFINITION_WITH_TE B
    ON 
        A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
        AND A.CES_CID=B.CES_CID

        """
    return ntf.executeQueryNatif(query,_conn)

def get_outages(cid_mag: int,pool_id: int,scenario: List[str],mindate: str,maxdate: str,_conn: any) -> pd.DataFrame:
    """
    Get outages for a period and different scenario

    Parameters:
        cid_mag (int): unique cid of the constraint.
        pool_id (int): pool id of the constraint
        scenario (List): list of scenario you want to see
        mindate (str): first date of the interval.
        maxdate (str): last date of the interval.
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """
    query=f"""
    WITH BASE_A AS (
    select 
        MDB_SCENARIONAME
        ,MAG_CID
        ,CES_CID
        ,DATE
        ,OUTAGEMAPPING_DAYZER_REF__ID AS OUTAGEID
        ,EQKEY
        ,ILODF
        ,AVG(AVGREDIRECTEDFLOW) AS AVG_REDIRECTED_FLOW
        ,STARTDATE
        ,ENDDATE
        ,STATUS 
        ,DATEDIFF(DAY, LAG(DATE) OVER (PARTITION BY EQKEY ORDER BY DATE), DATE) AS DateDiff
    from 
        MAGSNOWFLAKE.DAYZER_CUBES.LOR_RESULTS_DAILY A
    INNER JOIN 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC B
    ON 
        A.CONSTRAINTMAPPING_DAYZER_REF__ID=B.CES_CID 
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
    where 
        MAG_CID={cid_mag}
        AND MDB_SCENARIONAME IN (select value from table(flatten(input=>{scenario})))
        AND B.MAG_REF_POOL__ID={pool_id}
        AND ABS(AVGREDIRECTEDFLOW)>=1
        AND A.DATE between date('{mindate}') AND date('{maxdate}')
        AND ENDDATE<='2049-01-01'
    group by 
        MDB_SCENARIONAME
        ,DATE
        ,EQKEY
        ,ILODF
        ,STARTDATE
        ,ENDDATE
        ,STATUS
        ,MAG_CID
        ,CES_CID
        ,OUTAGEID
    )


    ,AVOID_DOUBLONS AS (
    select 
        MDB_SCENARIONAME
        ,DATE
        ,MAG_CID
        ,MIN(CES_CID) AS MIN_CID_CES
    from 
        BASE_A 
    GROUP BY 
        MDB_SCENARIONAME
        ,DATE
        ,MAG_CID
    )

    ,BASE AS (
    select 
        A.* 
    from 
        BASE_A A
    INNER JOIN 
        AVOID_DOUBLONS B
    ON 
        A.DATE=B.DATE 
        AND A.CES_CID=B.MIN_CID_CES
        AND A.MDB_SCENARIONAME=B.MDB_SCENARIONAME
    )

    select  
        MDB_SCENARIONAME AS SCENARIONAME
        ,DATE
        ,EQKEY
        ,OUTAGEID
        ,ILODF
        ,AVG_REDIRECTED_FLOW
        ,TO_DATE(STARTDATE) AS STARTDATE
        ,TO_DATE(ENDDATE) AS ENDDATE
        ,STATUS 
    from 
        BASE
    order by 
        DATE
            
    """
    return ntf.executeQueryNatif(query,_conn)

def get_scenario_id (scenario: List[str],_conn: Any):
    query=f"""
    select distinct 
        MAG_REF_SCENARIO_INFO__ID 
    from 
        MAGSNOWFLAKE.DAYZER.CONSTRAINT_SCENARIO_TO_BE_CUBED
    where 
        SCENARIONAME IN (select value from table(flatten(input=>{scenario})))
    """
    return ntf.executeQueryNatif(query,_conn)

def get_nb_hour_bind(pool_id: int,cid_mag: int,mindate: str,maxdate: str,_conn: any) -> pd.DataFrame:
    query=f"""
    SET PoolName=(select distinct MARKET from MAGSQLSERVER.DAYZERSTUDY.MAG_REF_MARKET where MAG_REF_MARKET__ID={pool_id});

    WITH NB_HOUR_PEAKID AS (
    select A.FTR_PEAKID AS PEAKID,COUNT(*) AS NB_HOUR
    from MAGSNOWFLAKE.DAYZER_CUBES_STAGING.YESENERGY_PEAKS A
    where MAG_REF_POOL__ID={pool_id}
    AND DATE_TRUNC(DAY,DATEADD(HOUR,-1,DATETIME)) between DATE('{mindate}') AND DATE('{maxdate}')
    group by FTR_PEAKID
    )

    ,RESULTS_MKT_DA AS (
    SELECT 
        'SP_DA' AS SCENARIO
        , CAST(SUM(SHADOWPRICE) AS INT) AS SP
        ,COUNT(*) AS NB_HOUR_BIND
        ,CAST(IFF(NB_HOUR_BIND=0,0,SP/NB_HOUR_BIND) AS INT) AS SP_PER_HOUR
        ,NULL AS MINLIMIT
        ,NULL AS MAXLIMIT
    FROM (
        SELECT DISTINCT
            MAG_REF_POOL__ID, PEAKID, POOLNAME, DATE, HE, CID_MAG,
            CONSTRAINTNAME, FACILITYNAME, CONTINGENCYNAME, SHADOWPRICE
        FROM MAGSNOWFLAKE.DAYZER.PROD_DA_CONSTRAINTS_MAPPED
        WHERE MAG_REF_POOL__ID={pool_id}
        AND DATE BETWEEN DATE('{mindate}') AND DATE('{maxdate}')
        AND CID_MAG={cid_mag}

    )
    )

    ,RESULTS_MKT_RT AS (
    SELECT 
        'SP_RT' AS SCENARIO
        , CAST(SUM(SP_RT) AS INT) AS SP
        ,COUNT(*) AS NB_HOUR_BIND
        ,CAST(IFF(NB_HOUR_BIND=0,0,SP/NB_HOUR_BIND) AS INT) AS SP_PER_HOUR
        ,NULL AS MINLIMIT
        ,NULL AS MAXLIMIT
    FROM (
        SELECT DISTINCT
            MAG_REF_POOL__ID, PEAKID, POOLNAME, DATE, HE, CID_MAG,
            CONSTRAINTNAME, FACILITYNAME, CONTINGENCYNAME, SP_RT
        FROM MAGSNOWFLAKE.DAYZER.PROD_RT_CONSTRAINTS_MAPPED
        WHERE MAG_REF_POOL__ID={pool_id}
        AND DATE BETWEEN DATE('{mindate}') AND DATE('{maxdate}')
        AND CID_MAG={cid_mag}

    )
    )

    ,RESULT_SC_A AS (
    select distinct
        MAG_CID,PEAKID,SHADOWCOST
    from
        MAGSNOWFLAKE.DAYZER.VWMAG_SHADOWCOST A
    INNER JOIN
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC B
    ON
        A.CID_CES=B.CES_CID
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
    where 
        POOLNAME=$PoolName
        AND AUCTIONDATE=STARTDATE
        AND STARTDATE='{mindate}'
        AND ENDDATE='{maxdate}'
        AND MAG_CID={cid_mag}

)

    ,RESULTS_SC AS (
    select 
        'SP_SC_1MA' AS SCENARIO
        ,CAST(SUM(ABS(SHADOWCOST)) AS INT) AS SP
        ,NULL AS NB_HOUR_BIND
        ,NULL AS SP_PER_HOUR
        ,NULL AS MINLIMIT
        ,NULL AS MAXLIMIT
    from
        RESULT_SC_A
    )

    ,RESULTS_SCENARIO AS (
    select 
        SCENARIONAME
        ,CAST(SUM(ABS(SHADOWPRICE)) AS INT) AS SP 
        ,SUM(CAST(NB_HOUR*BINDINGHOURSPCT AS INT)) AS NB_HOUR_BIND
        ,CAST(IFF(NB_HOUR_BIND=0,0,SP/NB_HOUR_BIND) AS INT) AS SP_PER_HOUR
        ,AVG(MINLOWERLIMIT) AS MINLIMIT
        ,AVG(MAXUPPERLIMIT) AS MAXLIMIT
    from 
        MAGSNOWFLAKE.DAYZER.VWMAG_CONSTRAINTS_RESULTS_MONTHLY A
    LEFT JOIN
        NB_HOUR_PEAKID B
    ON
        A.PEAKID=B.PEAKID
        WHERE MAG_REF_POOL__ID={pool_id}
        AND MONTH=  DATE('{mindate}')
        AND CONSTRAINTMAPPING_MAG_REF__ID={cid_mag}
        AND MAG_REF_PRODUCT__ID IN (1,2)
    group by SCENARIONAME
    )

    select * from RESULTS_SCENARIO
    UNION
    select * from RESULTS_MKT_DA
    UNION
    select * from RESULTS_SC
    UNION
    select * from RESULTS_MKT_RT
    ;
    """
    return ntf.executeQueryNatif(query,_conn)

def get_historical_SP(pool_id: int,cid_mag: int,scenario_id_sp:List[int] ,_conn: any) -> pd.DataFrame:
    """
    For a constraint, get all the ShadowPrice DAM,RT, ShadowCost and Predicted ShadowPrice from scenario selected by the user

    Parameters:
        pool_id (int): pool id of the constraint
        cid_mag (int): unique cid of the constraint.
        scenario_id (List): list of scenario you want to see
        conn (Any): The Snowflake connection object.

    Returns:
        pd.DataFrame: The result of the query as a Pandas DataFrame.
    """ 
    query=f"""
    ALTER SESSION SET QUERY_TAG = 'NERD_MONKEY';

    CREATE OR REPLACE TEMPORARY TABLE UNION_ALL_RESULTS AS 
    WITH MONITORED_LINE AS (
    select 
        MAG_CID
        ,MIN(CES_CID) AS MIN_CID_CES
        ,MIN(CES_NAME) MIN_CES_NAME
        ,CONCAT(' MAG: ',MAG_CID,' CES: ',MIN_CID_CES,' CTG: ',SPLIT_PART(MIN_CES_NAME,':',2)) AS NAME
        ,SPLIT_PART(MIN_CES_NAME,':',2) AS CTG
    from 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC
    where 
        SPLIT_PART(CES_NAME,':',0)=(
                                        select top 1
                                            SPLIT_PART(CES_NAME,':',0) BRANCH, 
                                        from 
                                            MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC
                                        where 
                                            MAG_CID={cid_mag}
                                        )
    group by 
        MAG_CID
    )

    ,RESULT_DA AS (
    select 
        DATE_TRUNC(MONTH, DATE) AS FTRMONTH
        ,CID_MAG
        ,NAME
        ,CTG
        ,PEAKID
        ,'SP_DA' AS PIVOT_COLUMN
        ,SUM(SHADOWPRICE) AS PIVOT_VALUE
    from 
    (
        select distinct
            MAG_REF_POOL__ID
            ,POOLNAME
            ,DATE
            ,HE
            ,PEAKID
            ,CID_MAG
            ,CONSTRAINTNAME
            ,FACILITYNAME
            ,CONTINGENCYNAME
            ,SHADOWPRICE
            ,NAME
            ,CTG
        from 
            MAGSNOWFLAKE.DAYZER.PROD_DA_CONSTRAINTS_MAPPED A
        INNER JOIN 
            MONITORED_LINE B
        ON 
            A.CID_MAG=B.MAG_CID
        where 
            MAG_REF_POOL__ID={pool_id}
    )
    group by 
        DATE_TRUNC(MONTH, DATE)
        ,CID_MAG
        ,PEAKID
        ,NAME
        ,CTG
    )

    , RESULT_RT AS (
    select 
        DATE_TRUNC(MONTH, DATE) AS FTRMONTH
        ,CID_MAG
        ,NAME
        ,CTG
        ,PEAKID
        ,'SP_RT' AS PIVOT_COLUMN
        ,SUM(SP_RT) AS PIVOT_VALUE
    from 
    (
        select distinct
            MAG_REF_POOL__ID
            ,POOLNAME
            ,DATE
            ,HE
            ,PEAKID
            ,CID_MAG
            ,CONSTRAINTNAME
            ,FACILITYNAME
            ,CONTINGENCYNAME
            ,SP_RT
            ,NAME
            ,CTG
        from 
            MAGSNOWFLAKE.DAYZER.PROD_RT_CONSTRAINTS_MAPPED A
        INNER JOIN 
            MONITORED_LINE B
        ON 
            A.CID_MAG=B.MAG_CID
        where 
            MAG_REF_POOL__ID={pool_id}
    )
    group by 
        DATE_TRUNC(MONTH, DATE)
        ,CID_MAG
        ,PEAKID
        ,NAME
        ,CTG
    )

    ,RESULT_MKT_RT_DA AS (
    select 
        * 
    from 
        RESULT_DA
    UNION
    select 
        * 
    from 
        RESULT_RT
    )

    ,SHADOWCOST_DATA AS (
    select distinct 
        B.MAG_CID
        ,A.* EXCLUDE (CID_CES,NAME_CES,MAG_REF_PACKAGEVERSION__ID,SHADOWCOST)
        ,ABS(A.SHADOWCOST) AS SHADOWCOST
        ,C.NAME
        ,C.CTG
        , CASE
            WHEN -- 1MA
                ISANNUAL = 0
                AND STARTDATE = AUCTIONDATE
            THEN 'SC_1MA'

            WHEN -- FW
                ISANNUAL = 0
                AND STARTDATE > AUCTIONDATE 
                AND DATEDIFF(MONTH, STARTDATE, ENDDATE) <= 1
            THEN CONCAT('SC_', DATEDIFF(MONTH, AUCTIONDATE, STARTDATE)+1,'MA')

            WHEN -- Q2 MISO
                ISANNUAL = 1
                AND STARTDATE > AUCTIONDATE
                AND MAG_REF_MARKET__ID = 0
                AND DATEDIFF(MONTH, STARTDATE, ENDDATE) > 1
                AND MONTH(STARTDATE) = 9
                AND MONTH(AUCTIONDATE) NOT IN (4,5)
            THEN 'SC_Q2'

            WHEN -- Q3 MISO
                ISANNUAL = 1
                AND STARTDATE > AUCTIONDATE
                AND MAG_REF_MARKET__ID = 0
                AND DATEDIFF(MONTH, STARTDATE, ENDDATE) > 1
                AND MONTH(STARTDATE) = 12
                AND MONTH(AUCTIONDATE) NOT IN (4,5)
            THEN 'SC_Q3'

            WHEN -- Q4 MISO
                ISANNUAL = 1
                AND STARTDATE > AUCTIONDATE
                AND MAG_REF_MARKET__ID = 0
                AND DATEDIFF(MONTH, STARTDATE, ENDDATE) > 1
                AND MONTH(STARTDATE) = 3
                AND MONTH(AUCTIONDATE) NOT IN (4,5)
            THEN 'SC_Q4'

            WHEN
                ISANNUAL = 1
                AND MAG_REF_MARKET__ID = 5 
            THEN CONCAT('SC_1YA_S',ROUND)

            WHEN 
                ISANNUAL = 1
                AND MAG_REF_MARKET__ID = 0
                AND MONTH(AUCTIONDATE) IN (4,5)
            THEN CONCAT('SC_1YA_R',ROUND) 

            WHEN 
                ISANNUAL = 1
                AND MAG_REF_MARKET__ID = 4

            THEN CONCAT('SC_1YA')

        END AS TYPE_AUCTION
    from 
        MAGSNOWFLAKE.DAYZER.VWMAG_SHADOWCOST A
    INNER JOIN 
        MAGSQLSERVER.DAYZERSTUDY.MAG_CES_CONSTRAINTS_MAP_HISTORIC B
    ON 
        A.CID_CES=B.CES_CID 
        AND A.MAG_REF_PACKAGEVERSION__ID=B.MAG_REF_PACKAGEVERSION__ID
        AND A.MAG_REF_MARKET__ID=B.MAG_REF_POOL__ID
    INNER JOIN
        MONITORED_LINE C
    ON 
        B.MAG_CID=C.MAG_CID
    WHERE 
        MAG_REF_MARKET__ID={pool_id}
    )


    ,SHADOWCOST_DATA_FINAL AS (
    select 
        STARTDATE
        ,MAG_CID
        ,NAME
        ,CTG
        ,PEAKID
        ,TYPE_AUCTION AS PIVOT_COLUMN
        ,CAST(SUM(SHADOWCOST) AS INT) AS PIVOT_VALUE
    from 
        SHADOWCOST_DATA
    group by 
        MAG_CID
        ,STARTDATE
        ,TYPE_AUCTION
        ,PEAKID
        ,NAME
        ,CTG
    )

    ,RESULT_DZR AS (
    select 
        MONTH
        ,CONSTRAINTMAPPING_MAG_REF__ID AS CID_MAG
        ,B.NAME
        ,B.CTG
        ,PEAKID
        ,SCENARIONAME AS PIVOT_COLUMN
        ,CAST(SUM(SHADOWPRICE) AS INT) AS PIVOT_VALUE
    from 
        MAGSNOWFLAKE.DAYZER.VWMAG_CONSTRAINTS_RESULTS_MONTHLY A
    INNER JOIN 
        MONITORED_LINE B
    ON 
        A.CONSTRAINTMAPPING_MAG_REF__ID=B.MAG_CID
    WHERE 
        SHADOWPRICE<>0
        AND MAG_REF_SCENARIO_INFO__ID IN ({scenario_id_sp})
    group by 
        MONTH
        ,PEAKID
        ,B.NAME
        ,B.CTG
        ,CONSTRAINTMAPPING_MAG_REF__ID
        ,CONSTRAINTMAPPING_DAYZER_REF__NAME 
        ,SCENARIONAME
    )

    select * from SHADOWCOST_DATA_FINAL
    UNION
    select * from RESULT_DZR
    UNION
    select * from RESULT_MKT_RT_DA
    ;



    select * from 
    UNION_ALL_RESULTS PIVOT (SUM(PIVOT_VALUE) FOR PIVOT_COLUMN IN (ANY ORDER BY PIVOT_COLUMN));
    """
    df=ntf.executeQueryNatif(query,_conn)
    return df