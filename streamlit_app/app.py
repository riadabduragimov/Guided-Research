import streamlit as st
import psycopg2
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import time
import plotly.express as px  
from rules import explain_plan_text, explain_query_text
from visualization import extract_operators_and_costs, plot_cost_heatmap, visualize_hive_operator_weights
from constants import operator_weights  

st.set_page_config(page_title="Hive & PostgreSQL Query Analyzer", layout="wide")
st.title("Data Query Analyzer: Hive via Spark & PostgreSQL Metastore")

session_defaults = {
    'show_hive_vis': False,
    'show_pg_vis': False,
    'show_hive_cost_scale': False,
    'show_pg_cost_scale': False,
    'pg_vis_fig': None,
    'hive_vis_fig': None,
    'hive_cost_scale_fig': None,
    'pg_cost_scale_fig': None,
    'hive_query_result': None,
    'hive_query_error': None,
    'hive_query_exec_time': None,
    'hive_query_suggestion': None,
    'hive_explain': None,
    'hive_explain_error': None,
    'hive_explain_nl': None,
    'pg_query_result': None,
    'pg_query_error': None,
    'pg_query_exec_time': None,
    'pg_explain': None,
    'pg_explain_error': None
}
for key, default in session_defaults.items():
    st.session_state.setdefault(key, default)

# --- Helper Functions ---

def display_query_results(result_df, exec_time, label):
    st.write(f"### Results from {label}")
    st.dataframe(result_df)
    if exec_time is not None:
        st.write(f"Query Execution Time: {exec_time:.3f} seconds")

def display_explain_plan(plan_text, natural_language=None):
    st.write("### Query Execution Plan")
    st.code(plan_text, language='sql')
    if natural_language:
        st.write("### Natural Language Interpretation")
        for line in natural_language.split("\n"):
            st.markdown(f"- {line}")

def display_query_suggestions(suggestions, title="Query Optimization Suggestions"):
    if suggestions:
        st.write(f"### {title}")
        for line in suggestions.split("\n"):
            st.markdown(f"- {line}")


# --- Sidebar Navigation ---
page = st.sidebar.selectbox("Select Environment", ["Hive Query Inspector", "Metastore Explorer (PostgreSQL)"])

# --- HIVE PAGE ---
if page == "Hive Query Inspector":
    st.header("Explore Hive Tables Using PySpark + Hive Metastore")
    st.caption(f"Using PySpark version: {pyspark.__version__}")

    try:
        spark = (
            SparkSession.builder
            .appName("HiveSparkIntegration")
            .master("spark://spark-master:7077")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
            .enableHiveSupport()
            .getOrCreate()
        )
        st.success("Spark session initialized successfully.")

        hive_query = st.text_area("Enter your SQL query on Hive tables", "SELECT * FROM test_db.customer LIMIT 5;")

        col1, col2, col3, col4, col5 = st.columns(5)  

        with col1:
            if st.button("Run Query"):
                try:
                    start = time.time()
                    result = spark.sql(hive_query).toPandas()
                    elapsed = time.time() - start

                    st.session_state['hive_query_result'] = result
                    st.session_state['hive_query_exec_time'] = elapsed
                    st.session_state['hive_query_suggestion'] = explain_query_text(hive_query)
                    st.session_state['hive_query_error'] = None

                except Exception as e:
                    st.session_state['hive_query_error'] = str(e)
                    st.session_state['hive_query_result'] = None

        with col2:
            if st.button("Explain Plan"):
                try:
                    explain_df = spark.sql(f"EXPLAIN FORMATTED {hive_query}")
                    st.session_state['hive_explain'] = "\n".join(explain_df.toPandas().iloc[:, 0])
                    st.session_state['hive_explain_error'] = None
                except Exception as e:
                    st.session_state['hive_explain_error'] = str(e)
                    st.session_state['hive_explain'] = None

        with col3:
            if st.button("Natural Language Plan"):
                if st.session_state['hive_explain']:
                    st.session_state['hive_explain_nl'] = explain_plan_text(st.session_state['hive_explain'])

        with col4:
            if st.button("Visualize Plan"):
                if st.session_state['hive_explain']:
                    fig = visualize_hive_operator_weights(st.session_state['hive_explain'])
                    if fig:
                        st.session_state['hive_vis_fig'] = fig
                        st.session_state['show_hive_vis'] = True
                    else:
                        st.warning("No visualizable operators found.")

        with col5:
            if st.button("Show Cost Scale"):
                df_scale = pd.DataFrame({
                    "Operator": list(operator_weights.keys()),
                    "Weight": list(operator_weights.values())
                })
                fig_scale = px.bar(
                    df_scale, x="Operator", y="Weight",
                    color="Operator",
                    title="Reference Cost Scale of Spark Execution Operators",
                    text="Weight"
                )
                fig_scale.update_layout(
                    autosize=True,
                    width=None,
                    height=400,
                    margin=dict(l=40, r=40, t=40, b=40),
                    xaxis_title="Operator Type",
                    yaxis_title="Weight"
                )
                st.session_state['hive_cost_scale_fig'] = fig_scale
                st.session_state['show_hive_cost_scale'] = True

        # Results and Visuals
        if st.session_state['hive_query_result'] is not None:
            display_query_results(st.session_state['hive_query_result'], st.session_state['hive_query_exec_time'], "Hive via Spark")

        if st.session_state['hive_query_error']:
            st.error(st.session_state['hive_query_error'])

        display_query_suggestions(st.session_state['hive_query_suggestion'])

        if st.session_state['hive_explain']:
            display_explain_plan(st.session_state['hive_explain'], st.session_state.get('hive_explain_nl'))

        if st.session_state['show_hive_vis']:
            with st.expander("Operator Visualization"):
                st.plotly_chart(st.session_state['hive_vis_fig'], use_container_width=True, key="hive_vis")
                if st.button("Hide Visualization"):
                    st.session_state['show_hive_vis'] = False
                    st.experimental_rerun()

        if st.session_state['show_hive_cost_scale']:
            with st.expander("Operator Cost Scale"):
                st.plotly_chart(st.session_state['hive_cost_scale_fig'], use_container_width=True, key="hive_cost_scale")
                if st.button("Hide Cost Scale"):
                    st.session_state['show_hive_cost_scale'] = False
                    st.experimental_rerun()

    except Exception as e:
        st.error(f"Could not initialize SparkSession: {e}")

# --- POSTGRESQL PAGE ---
elif page == "Metastore Explorer (PostgreSQL)":
    st.header("Analyze Hive Metastore with PostgreSQL")
    host, port, dbname, user, password = "hive-metastore-postgresql", 5432, "metastore", "hive", "hive"

    try:
        conn = psycopg2.connect(host=host, port=port, database=dbname, user=user, password=password)
        st.success("Connected to PostgreSQL Hive Metastore.")

        pg_query = st.text_area("Enter your PostgreSQL query", 'SELECT * FROM "DBS" LIMIT 5;')
        col1, col2, col3, col4 = st.columns(4)  

        with col1:
            if st.button("Run Query"):
                try:
                    start = time.time()
                    df = pd.read_sql(pg_query, conn)
                    elapsed = time.time() - start

                    st.session_state['pg_query_result'] = df
                    st.session_state['pg_query_exec_time'] = elapsed
                    st.session_state['pg_query_error'] = None

                except Exception as e:
                    st.session_state['pg_query_error'] = str(e)
                    st.session_state['pg_query_result'] = None

        with col2:
            if st.button("Explain Plan"):
                try:
                    explain_df = pd.read_sql(f"EXPLAIN {pg_query}", conn)
                    st.session_state['pg_explain'] = "\n".join(explain_df.iloc[:, 0])
                    st.session_state['pg_explain_error'] = None
                except Exception as e:
                    st.session_state['pg_explain_error'] = str(e)

        with col3:
            if st.button("Visualize Cost Plan"):
                if st.session_state['pg_explain']:
                    ops, costs = extract_operators_and_costs(st.session_state['pg_explain'])
                    if ops:
                        fig = plot_cost_heatmap(ops, costs)
                        st.session_state['pg_vis_fig'] = fig
                        st.session_state['show_pg_vis'] = True

        with col4:
            if st.button("Show Cost Scale"):
                if st.session_state['pg_explain']:
                    ops, costs = extract_operators_and_costs(st.session_state['pg_explain'])
                    if ops:
                        fig = plot_cost_heatmap(ops, costs)
                        st.session_state['pg_cost_scale_fig'] = fig
                        st.session_state['show_pg_cost_scale'] = True
                    else:
                        st.warning("No cost data found for scale.")

        if st.session_state['pg_query_result'] is not None:
            display_query_results(st.session_state['pg_query_result'], st.session_state['pg_query_exec_time'], "PostgreSQL Metastore")

        if st.session_state['pg_query_error']:
            st.error(st.session_state['pg_query_error'])

        if st.session_state['pg_explain']:
            display_explain_plan(st.session_state['pg_explain'])

        if st.session_state['pg_explain_error']:
            st.error(st.session_state['pg_explain_error'])

        if st.session_state['show_pg_vis']:
            with st.expander("PostgreSQL Plan Heatmap"):
                st.plotly_chart(st.session_state['pg_vis_fig'], use_container_width=True, key="pg_vis")
                if st.button("Hide PostgreSQL Visualization"):
                    st.session_state['show_pg_vis'] = False
                    st.experimental_rerun()

        if st.session_state['show_pg_cost_scale']:
            with st.expander("PostgreSQL Cost Scale"):
                st.plotly_chart(st.session_state['pg_cost_scale_fig'], use_container_width=True, key="pg_cost_scale")
                if st.button("Hide Cost Scale"):
                    st.session_state['show_pg_cost_scale'] = False
                    st.experimental_rerun()

        conn.close()

    except Exception as e:
        st.error(f"Could not connect to PostgreSQL: {e}")
