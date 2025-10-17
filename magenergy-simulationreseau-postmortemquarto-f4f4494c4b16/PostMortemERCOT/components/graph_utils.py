import pandas as pd
import plotly.graph_objects as go
import numpy as np
import difflib
from typing import Union,List,Any,Optional
from plotly.subplots import make_subplots
from utils.constants import COLOR_PALETTE,COLOR_MAP

pd.set_option('future.no_silent_downcasting', True)

def create_graph_load(df_Load: pd.DataFrame,LoadZone: str,startrange: str, endrange: str) -> None:
    """
    Create a load graph based a Df and which loadZone you want to see
    """
    fig=go.Figure()
    df_Load_zone=df_Load[df_Load['ZONENAME'] == LoadZone] #TOTAL ,SOUTH ERCOT, NORTH ERCOT, WEST ERCOT

    for scenario in sorted(df_Load_zone['SCENARIONAME'].unique()):
        df_filtered = df_Load_zone[df_Load_zone['SCENARIONAME'] == scenario]
        fig.add_trace(go.Scatter(x=df_filtered['HEDATE'],
                                 y=df_filtered['DEMANDMW'],
                                 name=scenario,
                                 mode='lines',
                                 )
        )

    # Adding titles
    fig.update_layout(
        title=f"Demand {LoadZone} Over Time by Scenario",
        xaxis_title="Date",
        yaxis_title="Demand (MW)",
        xaxis=dict(
            range=[startrange, endrange]  # Set default date range
        )
    )
    fig.show()

def create_graph_wind(df_Wind: pd.DataFrame,startrange: str, endrange: str):
    """
    Create a wind graph based on Df
    """
    fig=go.Figure()
    for scenario in sorted(df_Wind['SCENARIONAME'].unique()):
        df_filtered = df_Wind[df_Wind['SCENARIONAME'] == scenario]
        fig.add_trace(go.Scatter(x=df_filtered['HEDATE'],
                                 y=df_filtered['WIND_GEN'],
                                 name=scenario,
                                 mode='lines',
                                 )
        )
    # Adding titles
    fig.update_layout(
        title="Wind Generation Time by Scenario",
        xaxis_title="Date",
        yaxis_title="Wind (MW)",
        xaxis=dict(
            range=[startrange, endrange]  # Set default date range
        )
    )

    fig.show()



def hourly_figure(
    df_flows: pd.DataFrame,
    df_categories: pd.DataFrame,
    df_outages: pd.DataFrame,
    Scenario_first_priority: str,
    startdate: Any,
    enddate: Any,
) -> Any:
    """
    Creates the hourly figure showing flows, categories, and outages.
    """
    fig = make_subplots(
        rows=3, cols=1,
        specs=[[{"secondary_y": True}], [{}], [{}]],
        subplot_titles=("Flows", "Categories", "Transmissions Outages")
    )
    # Add flow traces

    for scenario in sorted(df_flows['SCENARIONAME'].unique()):

        df_filtered = df_flows[df_flows['SCENARIONAME'] == scenario].copy()
        if scenario==Scenario_first_priority:
            typetrace=True
        else:
            typetrace='legendonly'

        scenario_to_da=Scenario_first_priority

        df_filtered['DAY_NAME'] = df_filtered['HEDATE'].dt.strftime('%A')
        df_categories['DAY_NAME'] = df_categories['HEDATE'].dt.strftime('%A')
        
        add_flow_hourly_traces(fig, 
                               df_filtered, 
                               scenario, 
                               typetrace,
                               scenario_to_da, 
                               row=1, 
                               col=1
                               )
    # Add category traces
    first_scenario=True
    for scenario in sorted(df_categories['SCENARIONAME'].unique()):
        add_category_hourly_traces(fig
                     ,df_categories
                     ,scenario
                     ,first_scenario
                     ,row=2
                     ,col=1
                     )
    
        # Add flows hourly on category traces
        add_flows_on_category_hourly_traces(fig,
                                            df_flows,
                                            scenario,
                                            first_scenario,
                                            row=2, 
                                            col=1
                                            )

        # Add outage traces
        add_outage_daily_traces(fig, 
                                df_outages, 
                                scenario,
                                first_scenario,
                                row=3, 
                                col=1
                                )
        first_scenario=False

    #Add button to manage different categories
    create_update_button(fig
                         ,df_categories)

    # Update layout
    update_fig(fig,startdate,enddate)
    # Render figure
    fig.show(config={'scrollZoom': True})


def add_flow_hourly_traces(fig: go.Figure, 
                           df: pd.DataFrame, 
                           scenario: str, 
                           typetrace: Any,
                           scenario_to_da: str,
                           row: int, 
                           col: int) -> None:
    """
    Adds flow traces (lines, limits, shadow prices) to the figure.
    """
    fig.add_trace(
        go.Scatter(
            x=df['HEDATE'], 
            y=df['FLOWS'].round(0),
            mode='lines', 
            name=scenario,
            legendgroup=scenario, 
            legendgrouptitle_text=scenario,
            connectgaps=False, 
            visible=typetrace,
            hovertemplate='Flow: %{y}<br>Package ID: %{customdata[0]}<br> %{customdata[2]} → %{customdata[3]}',
            customdata=df[['MAG_REF_PACKAGEVERSION__ID', 'DAY_NAME','FROMBUSNAME','TOBUSNAME']].values
        ),
        row=row, col=col
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['HEDATE'], 
            y=df['SP_DZR'],
            name=f'SP_{scenario}', 
            legendgroup=scenario, 
            legendgrouptitle_text=scenario,
            fill='tozeroy',
            mode='none', 
            visible=typetrace, 
            hovertemplate='%{y}'
        ),
        row=row, col=col, secondary_y=True
    )

    # if histo=='Get same scenario':
    fig.add_trace(
        go.Scatter(
            x=df['HEDATE'], 
            y=df['MINLIMIT'],
            mode='lines', 
            name=f'MINLIMIT_{scenario}',
            legendgroup=scenario, 
            legendgrouptitle_text=scenario,
            visible=typetrace, 
            hovertemplate='%{y}'
        ),
        row=row, 
        col=col
    )
    fig.add_trace(
        go.Scatter(
            x=df['HEDATE'], 
            y=df['MAXLIMIT'],
            mode='lines', 
            name=f'MAXLIMIT_{scenario}',
            legendgroup=scenario, 
            legendgrouptitle_text=scenario,
            visible=typetrace, 
            hovertemplate='%{y}'
        ),
        row=row, 
        col=col
    )


    if scenario == scenario_to_da:
        fig.add_trace(
            go.Scatter(
                x=df['HEDATE']
                , y=df['SP_DA']
                , name='SP_DA'
                # ,legendgroup='SP_MKT'
                # ,legendgrouptitle_text='SP_MKT'
                , fill='tozeroy'
                ,mode='none'
                ,hovertemplate='%{y}'
                ,fillcolor='rgba(255, 0, 0, 0.5)'
            ),secondary_y=True
            ,row=1
            ,col=1
        )
        fig.add_trace(
            go.Scatter(
                x=df['HEDATE']
                , y=df['SP_RT']
                , name='SP_RT'
                # ,legendgroup='SP_MKT'
                # ,legendgrouptitle_text='SP_MKT'
                , fill='tozeroy'
                ,mode='none'
                ,hovertemplate='%{y}'
                ,fillcolor='rgba(0, 0, 255, 0.5)'
            ),secondary_y=True
            ,row=1
            ,col=1
        )


def add_category_hourly_traces(fig: go.Figure
                               , df: pd.DataFrame
                               ,scenario_to_trace: str
                               ,first_scenario: bool
                               , row: int
                               , col: int) -> None:
    """
    Adds traces for categories (Hydro, Wind, Load, Solar, etc.) to the figure.
    """
    categories = ['HYDRO', 'WIND', 'LOAD', 'SOLAR', 'OTHERS_UNITS', 'IE', 'GEO','INDL_LOAD']
    colors = ['lightblue', 'lightgreen', 'lightpink', 'orange', 'lightgrey', 'purple', 'brown','#d62728']
    scenariotype=scenario_to_trace.split("_",1)[1] #Recupérer juste le produit type
    for category, color in zip(categories, colors):
        fig.add_trace(
            go.Scatter(
                x=df[df['SCENARIONAME']==scenario_to_trace]['HEDATE'],
                y=df[df['SCENARIONAME']==scenario_to_trace][category].clip(upper=0).round(0),
                # y=df[category].round(0),
                mode='none', 
                name=category+'_'+scenariotype,
                legendgroup='Category', 
                legendgrouptitle_text='Category',
                stackgroup='pos', 
                line=dict(color=color), 
                fillcolor=color,
                hovertemplate='%{y}',
                visible=first_scenario,
            ),
            row=row, col=col
        )

        fig.add_trace(
            go.Scatter(
                x=df[df['SCENARIONAME']==scenario_to_trace]['HEDATE'], 
                y=df[df['SCENARIONAME']==scenario_to_trace][category].clip(lower=0).round(0),
                mode='none', 
                name=category+'_'+scenariotype, 
                showlegend=False,
                # legendgroup='Category',
                stackgroup='neg',
                line=dict(color=color), 
                fillcolor=color,
                hovertemplate='%{y}',
                visible=first_scenario
            ),
            row=row, col=col
        )

def add_flows_on_category_hourly_traces(fig: go.Figure,
                                         df: pd.DataFrame,
                                         scenario_sf_name_to_show: str,
                                         first_scenario: bool,
                                         row: int, 
                                         col: int
                                         ) -> None:
    """
    Adds trace of hourly flows on category graph
    """
    df_filtered=df[df['SCENARIONAME'] == scenario_sf_name_to_show]

    fig.add_trace(
    go.Scattergl(x=df_filtered['HEDATE']
           , y=df_filtered['FLOWS'].round(0)
            , mode='lines+markers'
            ,marker=dict(size=1)
            # , mode='lines'
            ,legendgroup='Category'
            ,legendgrouptitle_text='Category'
            ,name=scenario_sf_name_to_show
            ,hovertemplate='Flow: %{y}<br>Package ID: %{customdata[0]}<br> %{customdata[1]} → %{customdata[2]}'
            ,customdata=df_filtered[['MAG_REF_PACKAGEVERSION__ID','FROMBUSNAME','TOBUSNAME']].values
            ,visible=first_scenario
           )
    ,row=row
    ,col=col
    )

def add_outage_daily_traces_slider(fig: go.Figure, 
                            df_outages: pd.DataFrame, 
                            scenario_to_trace: str,
                            first_scenario: bool,
                            row: int, 
                            col: int
                            ) -> None:
    """
    Adds traces for outages (positive and negative flows).
    """
    df_outages_filtered=df_outages[df_outages['SCENARIONAME'] == scenario_to_trace] #dataframe 

    #sort the dataframe
    df_outages_filtered.sort_values(by='AVG_REDIRECTED_FLOW', ascending=False, inplace=True)
    if pd.notna(df_outages_filtered['AVG_REDIRECTED_FLOW'].abs().max()):
        max_flow=int(df_outages_filtered['AVG_REDIRECTED_FLOW'].abs().max())
    else:
        max_flow=0
    steps = []
    
    for threshold in range (1,max_flow+1):
        visible= True if (threshold == 1 and first_scenario == True) else False
        #code to add the Scenarioname in the hovertext
        hovertext_scenario=[f"<b>{scn}</b>" for scn in df_outages_filtered['SCENARIONAME']]
        fig.add_trace(
            go.Scatter(
                x=df_outages_filtered['DATE'], 
                y=[0] * len(df_outages_filtered['DATE']),
                hovertext=hovertext_scenario, 
                hoverinfo='x+text',
                showlegend=False,
                mode='markers',    # Marker mode ensures hover points are enabled
                marker=dict(opacity=0), 
                name=threshold,
                visible=visible,
                meta=scenario_to_trace
            ),
            row=row, col=col
        )

        df_outages_Pos = df_outages_filtered[df_outages_filtered['AVG_REDIRECTED_FLOW'] >= threshold]
        df_outages_Neg = df_outages_filtered[df_outages_filtered['AVG_REDIRECTED_FLOW'] <= -threshold]

        for outage_type, df_filtered in zip(['Pos', 'Neg'], [df_outages_Pos, df_outages_Neg]):
            # df_filtered.sort_values(by='AVG_REDIRECTED_FLOW', ascending=False, inplace=True)
            # if threshold==1:
            #     st.write(df_filtered)
            for outage in df_filtered['EQKEY'].unique():
                df_outage = df_filtered[df_filtered['EQKEY'] == outage]

                hovertexts = [
                    f"{outage}: {y} <br> <b>OutageID:</b>{oid} <br> <b>StartDate:</b>{sdt} <br> <b>EndDate:</b>{edt}"
                    for oid, y, sdt, edt in zip(
                        df_outage['OUTAGEID'], df_outage['AVG_REDIRECTED_FLOW'].round(0),
                        df_outage['STARTDATE'], df_outage['ENDDATE']
                    )
                ]

                fig.add_trace(
                    go.Bar(
                        x=df_outage['DATE'], 
                        y=df_outage['AVG_REDIRECTED_FLOW'].round(0),
                        hovertext=hovertexts, 
                        hoverinfo='x+text',
                        showlegend=False, 
                        legendgroup=outage,
                        visible=visible,
                        name=threshold,
                        meta=scenario_to_trace
                    ),
                    row=row, col=col
                )



    steps= []

    for threshold in range (1,max_flow+1): #building the step function to not affect previous graph
        visible_array =[]                    #and just affect the outages graph
        for j in range(len(fig.data)):
            if fig.data[j]['yaxis']!='y4': #if it's not in the outage graph, keep the default visible value
                visible_array.append(fig.data[j]['visible'])
            else:
                if fig.data[j]['name']==threshold: #if the value is superior of threshold, make it visible
                    visible_array.append(True)
                else:
                    visible_array.append(False)

        # Add step for the slider
        step = dict(
            method="update",
            args=[{"visible": visible_array},  # Update visibility
                    ], 
            label=f"{threshold} MW"  # Slider label
        )
        steps.append(step)
    
    sliders = [dict(
    active=max_flow,
    currentvalue={"prefix": "Outage impact: >"},
    pad={"t": 35},
    steps=steps
    )]

    fig.update_layout(sliders=sliders)


def add_outage_daily_traces(fig: go.Figure, 
                            df_outages: pd.DataFrame, 
                            scenario_to_trace: str,
                            first_scenario: bool,
                            row: int, 
                            col: int
                            ) -> None:
    """
    Adds traces for outages (positive and negative flows).
    """
    df_outages_filtered=df_outages[df_outages['SCENARIONAME'] == scenario_to_trace].copy() #dataframe 

    #sort the dataframe
    df_outages_filtered.sort_values(by='AVG_REDIRECTED_FLOW', ascending=False, inplace=True)
    if pd.notna(df_outages_filtered['AVG_REDIRECTED_FLOW'].abs().max()):
        max_flow=int(df_outages_filtered['AVG_REDIRECTED_FLOW'].abs().max())
    else:
        max_flow=0

    visible= True if first_scenario == True else False
    #code to add the Scenarioname in the hovertext
    hovertext_scenario=[f"<b>{scn}</b>" for scn in df_outages_filtered['SCENARIONAME']]
    fig.add_trace(
        go.Scatter(
            x=df_outages_filtered['DATE'], 
            y=[0] * len(df_outages_filtered['DATE']),
            hovertext=hovertext_scenario, 
            hoverinfo='x+text',
            showlegend=False,
            mode='markers',    # Marker mode ensures hover points are enabled
            marker=dict(opacity=0), 
            visible=visible,
            meta=scenario_to_trace
        ),
        row=row, col=col
    )

    df_outages_Pos = df_outages_filtered[df_outages_filtered['AVG_REDIRECTED_FLOW'] >= 1]
    df_outages_Neg = df_outages_filtered[df_outages_filtered['AVG_REDIRECTED_FLOW'] <= -1]

    for outage_type, df_filtered in zip(['Pos', 'Neg'], [df_outages_Pos, df_outages_Neg]):
        for outage in df_filtered['EQKEY'].unique():
            df_outage = df_filtered[df_filtered['EQKEY'] == outage]

            hovertexts = [
                f"{outage}: {y} <br> <b>OutageID:</b>{oid} <br> <b>StartDate:</b>{sdt} <br> <b>EndDate:</b>{edt}"
                for oid, y, sdt, edt in zip(
                    df_outage['OUTAGEID'], df_outage['AVG_REDIRECTED_FLOW'].round(0),
                    df_outage['STARTDATE'], df_outage['ENDDATE']
                )
            ]

            fig.add_trace(
                go.Bar(
                    x=df_outage['DATE'], 
                    y=df_outage['AVG_REDIRECTED_FLOW'].round(0),
                    hovertext=hovertexts, 
                    hoverinfo='x+text',
                    showlegend=False, 
                    legendgroup=outage,
                    visible=visible,
                    meta=scenario_to_trace
                ),
                row=row, col=col
            )



def create_update_button(fig
                        ,df_catego: pd.DataFrame
                         ):
    """
    This will update the category graph depending of the scenario selected
    """

    scenarios=sorted(df_catego["SCENARIONAME"].unique())
    buttons=[]
    for scenario in scenarios:
        visibility_update =[]                    #just affect the category and outage graph
        scenariotype=scenario.split("_",1)[1]

        for j in range(len(fig.data)):
            if fig.data[j]['yaxis'] not in ['y3','y4']: #if it's not in the category graph, keep the default visible value
                visibility_update.append(fig.data[j]['visible'])
            elif fig.data[j]['yaxis']=='y3': #if it's the category graph:
                if scenariotype in fig.data[j]['name']: #if the scenario name is in the name make it visible
                    visibility_update.append(True)
                else:
                    visibility_update.append(False)
            else: #if it's the outage graph:
                if fig.data[j]['meta']==scenario:
                    visibility_update.append(True)
                    # visibility_update.append(True)

                else:
                    visibility_update.append(False)


                

        buttons.append(
            dict(
                label=scenario,
                method="update",
                args=[{"visible": visibility_update}]
            )
        )

    fig.update_layout(
    updatemenus=[
        dict(
            type="buttons",
            direction="left",
            buttons=buttons,
            x=0.7, y=1.15
        )
    ]
    )



def update_fig(fig: go.Figure,startdate:str,enddate:str) -> None:
    """
    Updates the layout for the figure.
    """
    fig.update_xaxes(
        matches='x',
    )

    fig.update_xaxes(  
            row=1, col=1          
    )
    fig.update_yaxes(autorange=True)
    fig.update_xaxes(autorange=True)

    fig.update_layout(
        dragmode='pan', 
        title='Constraint Driver Decomposition',
        height=800, 
        bargap=0, 
        barmode='relative', 
        hovermode='x unified',
        xaxis_hoverformat='%Y-%m-%d %H:%M (%a)',
        legend=dict(
            groupclick="togglegroup"
            
            ),

        xaxis=dict(
            range=[startdate, enddate]  # Set default date range
        ),
        yaxis=dict(fixedrange=False, title='Flows (MW)'),
        yaxis2=dict(fixedrange=False, title='ShadowPrice ($)'),
        yaxis3=dict(fixedrange=False, matches='y', title='Flows (MW)'),
        margin=dict(l=50, r=50, t=50, b=100),
    )


# def shadowprice_monthly_fig(fig: go.Figure, df_histoSP: pd.DataFrame, colors: List[str]):
#     """
#     Trace monthly shadowprice

#     Parameters:
#         fig (go.Figure): The Plotly figure to which the traces are added.
#         df_histoSP (pd.DataFrame): The DataFrame

#     Returns:
#         None
#     """
    
#     column_names = df_histoSP_peak_ctg.drop(columns=['STARTDATE', 'MAG_CID', 'PEAKID', 'NAME','CTG']).columns.tolist()
#     df_histoSP_peak_ctg[column_names]=df_histoSP_peak_ctg[column_names].fillna(0).abs()

#     for i, column in enumerate(column_names):
#         df_histoSP_peak_ctg=df_histoSP_peak_ctg.sort_values(by=column, ascending=False)
#         fig.add_trace(
#             go.Bar(
#                 x=df_histoSP_peak_ctg['STARTDATE'],
#                 y=df_histoSP_peak_ctg[column],
#                 name=column, 
#                 hovertemplate='%{x}<br> <b>PeakId:</b> %{customdata[1]} <br> <b>SP:</b>%{y:$,.2f}<br> <b>CTG:</b> %{customdata[0]}',
#                 customdata=df_histoSP_peak_ctg[['CTG','PEAKID']].values,
#                 visible=True if column == "'SP_DA'" else 'legendonly',
#                 marker=dict(color=colors[i],line=dict(width=0.2, color='black')), 
#                 legendgroup=column.replace("'", ""),
#             ), row=2, col=1
#         )

def shadowprice_monthly_fig(df_histoSP: pd.DataFrame,cid_mag:int):
    """
    Trace monthly shadowprice

    Parameters:
        fig (go.Figure): The Plotly figure to which the traces are added.
        df_histoSP (pd.DataFrame): The DataFrame

    Returns:
        None
    """
    fig=go.Figure()

    df_histoSP_only_constraint=df_histoSP[df_histoSP['MAG_CID']==cid_mag].copy()
    create_graph_for_constraint(fig,df_histoSP_only_constraint,'Main Constraint')

    create_graph_for_constraint(fig,df_histoSP,'All')

    create_update_button_SP(fig,df_histoSP)

    fig.update_layout(
        dragmode='pan', 
        title='Historical ShadowPrice',
        bargap=0, 
        yaxis=dict(fixedrange=False, title='ShadowPrice ($)'),
        legend=dict(groupclick="togglegroup", orientation="h", font_size=10),
    )                  
    fig.show()

def create_graph_for_constraint(fig,df_histoSP,type):
    """
    This create the graph for the constraint
    """
    column_names = df_histoSP.drop(columns=['STARTDATE', 'MAG_CID', 'PEAKID', 'NAME','CTG']).columns.tolist()
    df_histoSP[column_names]=df_histoSP[column_names].fillna(0).abs().infer_objects()
    
    for i, column in enumerate(column_names):
        if type == 'Main Constraint' and column == "'SP_DA'":
            visible_default=True
        elif type == 'Main Constraint' and column != "'SP_DA'":
            visible_default='legendonly'
        else:
            visible_default=False

        df_histoSP=df_histoSP.sort_values(by=column, ascending=False)
        color_key = column.replace("'", "")
        color = COLOR_MAP.get(color_key, COLOR_PALETTE[i % len(COLOR_PALETTE)])        
        
        fig.add_trace(
            go.Bar(
                x=df_histoSP['STARTDATE'],
                y=df_histoSP[column],
                name=column, 
                hovertemplate='%{x}<br> <b>PeakId:</b> %{customdata[1]} <br> <b>SP:</b>%{y:$,.2f}<br> <b>CTG:</b> %{customdata[0]}',
                customdata=df_histoSP[['CTG','PEAKID']].values,
                # visible=True if column == "'SP_DA'" else 'legendonly',
                visible=visible_default,
                marker=dict(color=color,line=dict(width=0.2, color='black')), 
                legendgroup=column.replace("'", ""),
                meta=type
            )
        )


def create_update_button_SP(fig
                        ,df_histoSP: pd.DataFrame
                         ):
    """
    This will update the SP graph depending of if we want to see the SP of our constraint
    or the SP of all the constraint of the monitored line
    """
    buttons=[]
    for button in ['Main Constraint','All']:
        visibility_update =[]
        for j in range(len(fig.data)):
            if fig.data[j]['meta']==button:
                if fig.data[j]['name']=="'SP_DA'":
                    visibility_update.append(True)
                else:
                    visibility_update.append('legendonly')
                # visibility_update.append(fig.data[j]['visible'])
            else:
                visibility_update.append(False)        

        buttons.append(
             dict(
                 label=button,
                 method="update",
                 args=[{"visible": visibility_update}]
             )
         )
    fig.update_layout(
    updatemenus=[
        dict(
            type="buttons",
            direction="left",
            buttons=buttons,
            active=0,
            x=0.7, y=1.15
        )
    ]
    )

    # scenarios=sorted(df_catego["SCENARIONAME"].unique())
    # buttons=[]
    # for scenario in scenarios:
    #     visibility_update =[]                    #just affect the category and outage graph
    #     scenariotype=scenario.split("_",1)[1]

    #     for j in range(len(fig.data)):
    #         if fig.data[j]['yaxis'] not in ['y3','y4']: #if it's not in the category graph, keep the default visible value
    #             visibility_update.append(fig.data[j]['visible'])
    #         elif fig.data[j]['yaxis']=='y3': #if it's the category graph:
    #             if scenariotype in fig.data[j]['name']: #if the scenario name is in the name make it visible
    #                 visibility_update.append(True)
    #             else:
    #                 visibility_update.append(False)
    #         else: #if it's the outage graph:
    #             if fig.data[j]['meta']==scenario:
    #                 visibility_update.append(True)
    #                 # visibility_update.append(True)

    #             else:
    #                 visibility_update.append(False)


                

    #     buttons.append(
    #         dict(
    #             label=scenario,
    #             method="update",
    #             args=[{"visible": visibility_update}]
    #         )
    #     )

    # fig.update_layout(
    # updatemenus=[
    #     dict(
    #         type="buttons",
    #         direction="left",
    #         buttons=buttons,
    #         x=0.7, y=1.15
    #     )
    # ]
    # )