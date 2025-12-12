import plotly.express as px

# ------------------------
# 1) Helper: Convert $ → float
# ------------------------
def money_to_float(series):
    return (
        series.astype(str)
              .str.replace("$", "", regex=False)
              .str.replace(",", "", regex=False)
              .astype(float)
    )

# ------------------------
# 2) Compute TP differences for any scenario
# ------------------------
def compute_tp_diff(df, sp_col, scenario_col):
    """
    df: dataframe containing SP_DA, scenario column, and constraint column.
    sp_col: day-ahead settlement column (e.g., 'SP_DA')
    scenario_col: scenario being compared (e.g., 'NEPOOL_1MA_Default')
    """
    
    df = df.copy()
    
    # Convert money fields
    df[sp_col] = money_to_float(df[sp_col])
    df[scenario_col] = money_to_float(df[scenario_col])
    
    # TP = both nonzero
    # tp = df[(df[sp_col] != 0) & (df[scenario_col] != 0)].copy()
    tp = df[
        (df[sp_col] != 0)
        & (df[scenario_col] != 0)
        & (df[scenario_col].abs() > 50) # cutoff_SP=50
    ].copy()

    # Absolute difference
    tp["abs_diff"] = (tp[sp_col].abs() - tp[scenario_col].abs()).abs()
    tp["pct_diff"] = tp["abs_diff"] / tp["abs_diff"].sum() * 100

    return tp

# ------------------------
# 3) Plotting function
# ------------------------
def plot_constraint_percent(tp_df, constraint_col, scenario_name):
    fig = px.bar(
        tp_df,
        x=constraint_col,
        y="pct_diff",
        hover_data=["abs_diff"],
        title=f"% de la Contribution par Contrainte — {scenario_name}",
    )

    fig.update_layout(
        xaxis_title="Contrainte",
        yaxis_title="% du total de la Différence Absolue",
        xaxis_tickangle=-45,
        bargap=0.25
    )
    
    fig.show()