import streamlit as st 
import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from rules import explain_plan_text, explain_query_text
import pyspark
import time  # <-- for timing queries

# Import visualization functions from visualization.py
from visualization import (
    extract_operators_and_costs,
    plot_cost_heatmap,
    visualize_hive_operator_weights
)

st.title("Hive Metastore PostgreSQL & Spark Viewer")

# Initialize session state flags & figures if not present
if 'show_hive_vis' not in st.session_state:
    st.session_state['show_hive_vis'] = False
if 'show_pg_vis' not in st.session_state:
    st.session_state['show_pg_vis'] = False
if 'pg_vis_fig' not in st.session_state:
    st.session_state['pg_vis_fig'] = None
if 'hive_vis_fig' not in st.session_state:
    st.session_state['hive_vis_fig'] = None

# Sidebar navigation
page = st.sidebar.selectbox("Select Page", ["Hive", "PostgreSQL"])

if page == "Hive":
    st.header("PySpark Hive Connection")
    st.write(f"PySpark version: {pyspark.__version__}")

    try:
        spark = (
            SparkSession.builder
            .appName("HiveSparkIntegration")
            .master("spark://spark-master:7077")  # Adjust as needed
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
            .enableHiveSupport()
            .getOrCreate()
        )

        st.success("✅ SparkSession connected to Hive Metastore!")

        hive_query = st.text_area("Write your Hive SQL query here:", "SELECT * FROM test_db.customer LIMIT 5;")
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            if st.button("Run Hive Query"):
                # Clear previous explain plan, explanation, and visualization
                st.session_state['hive_explain'] = None
                st.session_state['hive_explain_error'] = None
                st.session_state['hive_explain_nl'] = None
                st.session_state['hive_vis_fig'] = None
                st.session_state['show_hive_vis'] = False
                st.session_state['hive_query_suggestion'] = None  # Clear old suggestions

                try:
                    start_time = time.time()  # start timer
                    hive_df = spark.sql(hive_query)
                    pandas_df = hive_df.toPandas()
                    end_time = time.time()  # end timer
                    elapsed_time = end_time - start_time

                    st.session_state['hive_query_result'] = pandas_df
                    st.session_state['hive_query_error'] = None
                    st.session_state['hive_query_exec_time'] = elapsed_time

                    # Get query text suggestions
                    query_suggestion = explain_query_text(hive_query)
                    st.session_state['hive_query_suggestion'] = query_suggestion

                except Exception as query_err:
                    st.session_state['hive_query_error'] = f"❌ Error executing Hive query: {query_err}"
                    st.session_state['hive_query_result'] = None
                    st.session_state['hive_query_exec_time'] = None
                    st.session_state['hive_query_suggestion'] = None

        with col2:
            if st.button("Explain Hive Query"):
                try:
                    explain_query = f"EXPLAIN FORMATTED {hive_query}"
                    explain_df = spark.sql(explain_query)
                    plan_text = "\n".join(explain_df.toPandas().iloc[:, 0].tolist())
                    st.session_state['hive_explain'] = plan_text
                    st.session_state['hive_explain_error'] = None
                    st.session_state['hive_explain_nl'] = None
                except Exception as explain_err:
                    st.session_state['hive_explain_error'] = f"❌ Error getting EXPLAIN plan: {explain_err}"
                    st.session_state['hive_explain'] = None
                    st.session_state['hive_explain_nl'] = None

        with col3:
            if st.button("Explain EXPLAIN Plan (Natural Language)"):
                if 'hive_explain' in st.session_state and st.session_state['hive_explain']:
                    explanation = explain_plan_text(st.session_state['hive_explain'])
                    st.session_state['hive_explain_nl'] = explanation
                else:
                    st.warning("Run 'Explain Hive Query' first to get the EXPLAIN plan.")

        with col4:
            if st.button("Visualize Hive Operator Weights"):
                if 'hive_explain' in st.session_state and st.session_state['hive_explain']:
                    fig = visualize_hive_operator_weights(st.session_state['hive_explain'])
                    if fig is not None:
                        st.session_state['hive_vis_fig'] = fig
                        st.session_state['show_hive_vis'] = True
                    else:
                        st.warning("No operators detected in the Hive EXPLAIN plan.")
                else:
                    st.warning("Run 'Explain Hive Query' first to visualize operator weights.")

        if st.session_state.get('show_hive_vis', False):
            with st.expander("Hive Operator Weights Visualization (Click to collapse)", expanded=True):
                st.plotly_chart(st.session_state['hive_vis_fig'], use_container_width=True)
                if st.button("Close Hive Visualization"):
                    st.session_state['show_hive_vis'] = False
                    st.experimental_rerun()

        # Display Hive results
        if st.session_state.get('hive_query_result') is not None:
            st.write("### Query Results from Hive via PySpark")
            st.dataframe(st.session_state['hive_query_result'])

            if st.session_state.get('hive_query_exec_time') is not None:
                st.write(f"⏱️ Query execution time: {st.session_state['hive_query_exec_time']:.3f} seconds")

        if st.session_state.get('hive_query_error') is not None:
            st.error(st.session_state['hive_query_error'])

        # Show Query Text Suggestions under results
        if st.session_state.get('hive_query_suggestion') is not None:
            st.write("### Query Text Suggestions")
            for line in st.session_state['hive_query_suggestion'].split("\n"):
                st.markdown(f"- {line}")

        if st.session_state.get('hive_explain') is not None:
            st.write("### EXPLAIN Plan for Hive Query")
            st.code(st.session_state['hive_explain'], language='sql')

        if st.session_state.get('hive_explain_error') is not None:
            st.error(st.session_state['hive_explain_error'])

        if st.session_state.get('hive_explain_nl') is not None:
            st.write("### Natural Language Explanation of EXPLAIN Plan")
            for line in st.session_state['hive_explain_nl'].split("\n"):
                st.markdown(f"- {line}")

            # Also remind query suggestions here for convenience
            if st.session_state.get('hive_query_suggestion') is not None:
                st.write("### Query Text Suggestions (Reminder)")
                for line in st.session_state['hive_query_suggestion'].split("\n"):
                    st.markdown(f"- {line}")

    except Exception as e:
        st.error(f"❌ Error connecting with PySpark: {e}")

elif page == "PostgreSQL":
    st.header("PostgreSQL Hive Metastore Connection")
    host = "hive-metastore-postgresql"
    port = 5432
    dbname = "metastore"
    user = "hive"
    password = "hive"

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password
        )
        st.success("✅ Connected to Hive Metastore PostgreSQL!")

        pg_query = st.text_area("Write your PostgreSQL Metastore query here:", 'SELECT * FROM "DBS" LIMIT 5;')
        col5, col6, col7 = st.columns([1, 1, 1])

        with col5:
            if st.button("Run PostgreSQL Query"):
                try:
                    start_time = time.time()
                    pg_df = pd.read_sql(pg_query, conn)
                    end_time = time.time()
                    elapsed_time = end_time - start_time

                    st.session_state['pg_query_result'] = pg_df
                    st.session_state['pg_query_error'] = None
                    st.session_state['pg_query_exec_time'] = elapsed_time

                except Exception as pg_err:
                    st.session_state['pg_query_error'] = f"❌ Error executing PostgreSQL query: {pg_err}"
                    st.session_state['pg_query_result'] = None
                    st.session_state['pg_query_exec_time'] = None

        with col6:
            if st.button("Explain PostgreSQL Query"):
                try:
                    explain_pg_query = f"EXPLAIN {pg_query}"
                    explain_df = pd.read_sql(explain_pg_query, conn)
                    plan_text = "\n".join(explain_df.iloc[:, 0].tolist())
                    st.session_state['pg_explain'] = plan_text
                    st.session_state['pg_explain_error'] = None
                except Exception as explain_err:
                    st.session_state['pg_explain_error'] = f"❌ Error getting EXPLAIN plan for PostgreSQL: {explain_err}"
                    st.session_state['pg_explain'] = None

        with col7:
            if st.button("Visualize PostgreSQL Plan Costs"):
                if 'pg_explain' in st.session_state and st.session_state['pg_explain']:
                    operators, costs = extract_operators_and_costs(st.session_state['pg_explain'])
                    if operators:
                        fig = plot_cost_heatmap(operators, costs)
                        if fig is not None:
                            st.session_state['pg_vis_fig'] = fig
                            st.session_state['show_pg_vis'] = True
                    else:
                        st.warning("No operator cost data found in the plan.")
                else:
                    st.warning("Run 'Explain PostgreSQL Query' first to get the EXPLAIN plan.")

        if st.session_state.get('show_pg_vis', False):
            with st.expander("PostgreSQL Plan Cost Heatmap (Click to collapse)", expanded=True):
                st.plotly_chart(st.session_state['pg_vis_fig'], use_container_width=True)
                if st.button("Close PostgreSQL Visualization"):
                    st.session_state['show_pg_vis'] = False
                    st.experimental_rerun()

        # Display PostgreSQL results
        if st.session_state.get('pg_query_result') is not None:
            st.write("### Query Results from PostgreSQL Metastore")
            st.dataframe(st.session_state['pg_query_result'])

            if st.session_state.get('pg_query_exec_time') is not None:
                st.write(f"⏱️ Query execution time: {st.session_state['pg_query_exec_time']:.3f} seconds")

        if st.session_state.get('pg_query_error') is not None:
            st.error(st.session_state['pg_query_error'])

        if st.session_state.get('pg_explain') is not None:
            st.write("### EXPLAIN Plan for PostgreSQL Query")
            st.code(st.session_state['pg_explain'], language='sql')

        if st.session_state.get('pg_explain_error') is not None:
            st.error(st.session_state['pg_explain_error'])

        conn.close()

    except Exception as e:
        st.error(f"❌ Error connecting to Hive Metastore PostgreSQL: {e}")
