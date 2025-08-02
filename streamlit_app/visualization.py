import re
import pandas as pd
import plotly.express as px
from collections import defaultdict
import streamlit as st
from constants import operator_weights  

def extract_operators_and_costs(plan_text):
    operators = []
    costs = []
    pattern = re.compile(r'([A-Za-z ]+?)\s*\(cost=(\d+\.\d+)\.\.(\d+\.\d+).*?\)')
    for line in plan_text.splitlines():
        match = pattern.search(line)
        if match:
            op = match.group(1).strip()
            cost_start = float(match.group(2))
            cost_end = float(match.group(3))
            avg_cost = (cost_start + cost_end) / 2
            operators.append(op)
            costs.append(avg_cost)
    return operators, costs

def plot_cost_heatmap(operators, costs):
    if not operators:
        st.warning("No operator cost data found in the plan.")
        return None
    df = pd.DataFrame({'Operator': operators, 'Cost': costs})
    df['Step'] = range(1, len(df) + 1)
    pivot = df.pivot(index='Operator', columns='Step', values='Cost').fillna(0)
    fig = px.imshow(
        pivot,
        labels=dict(x="Plan Step", y="Operator", color="Avg Cost"),
        x=pivot.columns,
        y=pivot.index,
        color_continuous_scale='Reds'
    )
    fig.update_layout(
        autosize=True,
        width=None,
        height=600,
        margin=dict(l=40, r=40, t=40, b=40),
    )
    return fig

def visualize_hive_operator_weights(plan_text):
    operator_counts = defaultdict(int)
    for op in operator_weights.keys():
        count = len(re.findall(op, plan_text, re.IGNORECASE))
        if count > 0:
            operator_counts[op] += count

    if not operator_counts:
        st.warning("No operators detected in the EXPLAIN plan.")
        return None

    operator_scores = {op: count * operator_weights[op] for op, count in operator_counts.items()}
    total_score = sum(operator_scores.values())
    operator_scores["Total"] = total_score

    df = pd.DataFrame({
        "Operator": list(operator_scores.keys()),
        "Weighted Score": list(operator_scores.values())
    })

    fig = px.bar(
        df, x="Operator", y="Weighted Score",
        color="Operator",
        title="Heuristic Weighted Operator Costs",
        text="Weighted Score"
    )
    fig.update_layout(
        autosize=True,
        width=None,
        height=600,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="Operator Type",
        yaxis_title="Weighted Score",
    )
    return fig
