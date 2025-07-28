import re

def explain_query_text(query_text: str) -> str:
    """
    Rule-based suggestions based on the raw SQL query text.
    Checks for common anti-patterns and performance issues.
    """
    suggestions = []

    # SELECT *
    if re.search(r"select\s+\*", query_text, re.IGNORECASE):
        suggestions.append("⚠️ `SELECT *` used — consider selecting only required columns to reduce I/O.")

    # Missing LIMIT
    if "limit" not in query_text.lower():
        suggestions.append("💡 Consider adding a LIMIT clause to reduce result size if appropriate.")

    # No WHERE clause
    if re.search(r"from\s+\w+", query_text, re.IGNORECASE) and "where" not in query_text.lower():
        suggestions.append("⚠️ No WHERE clause found — query may perform a full table scan.")

    # DISTINCT check
    if re.search(r"\bdistinct\b", query_text, re.IGNORECASE):
        suggestions.append("⚠️ `DISTINCT` used — ensure it's necessary as it adds an expensive sort/shuffle operation.")

    # Aggregation without GROUP BY
    if re.search(r"sum\(|avg\(|count\(", query_text, re.IGNORECASE) and "group by" not in query_text.lower():
        suggestions.append("💡 Aggregation used without `GROUP BY` — check if this is intentional.")

    # Subqueries
    if re.search(r"\(\s*select", query_text, re.IGNORECASE):
        suggestions.append("📌 Subquery detected — consider if a `JOIN` or `WITH` clause could improve readability or performance.")

    # ORDER BY without LIMIT
    if "order by" in query_text.lower() and "limit" not in query_text.lower():
        suggestions.append("⚠️ `ORDER BY` without `LIMIT` — may cause unnecessary full sort on large datasets.")

    # Aliases without AS
    if re.search(r"select\s+.+\s+[a-zA-Z_][a-zA-Z0-9_]*\s*,", query_text, re.IGNORECASE) and " as " not in query_text.lower():
        suggestions.append("💡 Column aliases used without `AS` — consider adding `AS` for clarity.")

    # Cartesian product (JOIN without ON)
    if re.search(r"\bjoin\b", query_text, re.IGNORECASE) and " on " not in query_text.lower():
        suggestions.append("🚨 JOIN without ON clause detected — may produce Cartesian Product.")

    # NOT IN check
    if re.search(r"\bnot\s+in\b", query_text, re.IGNORECASE):
        suggestions.append("⚠️ `NOT IN` used — if the subquery returns NULLs, results may be incorrect. Consider using `NOT EXISTS` instead.")
    
    # Cartesian product (JOIN without ON)
    if re.search(r"\bjoin\b", query_text, re.IGNORECASE) and " on " not in query_text.lower():
        suggestions.append("🚨 JOIN without ON clause detected — may produce Cartesian Product.")
    
    # Aggregation without GROUP BY
    if re.search(r"sum\(|avg\(|count\(", query_text, re.IGNORECASE) and "group by" not in query_text.lower():
        suggestions.append("💡 Aggregation used without `GROUP BY` — check if this is intentional.")
    
    # Suggest partition filtering
    if re.search(r"\bwhere\b", query_text, re.IGNORECASE) and not re.search(r"partition\s*=", query_text, re.IGNORECASE):
        suggestions.append("💡 If working with partitioned tables, filter on partition column(s) for faster access.")

    # FULL OUTER JOIN check
    if "full join" in query_text.lower() or "full outer join" in query_text.lower():
        suggestions.append("ℹ️ FULL OUTER JOIN used — ensure that combining unmatched rows is necessary. Consider performance implications.")

    # CROSS JOIN check
    if "cross join" in query_text.lower():
        suggestions.append("🚨 `CROSS JOIN` detected — produces Cartesian product unless filtered. Use with caution.")

    if not suggestions:
        suggestions.append("✅ Query text looks good — no obvious anti-patterns detected.")

    return "\n".join(suggestions)



def explain_plan_text(plan_text: str) -> str:
    """
    Rule-based explanation of Spark/Hive EXPLAIN FORMATTED plan with join reasoning,
    performance warnings, and heuristics for skew detection.
    """

    explanations = []

    # General anti-patterns and best practices
    if re.search(r"select\s+\*", plan_text, re.IGNORECASE):
        explanations.append("⚠️ `SELECT *` used — consider selecting only required columns to reduce I/O.")

    if "Scan" in plan_text and "Filter" not in plan_text:
        explanations.append("⚠️ Full table scan detected — no filters applied.")
        explanations.append("💡 Add WHERE conditions or partition filters to improve performance.")

    if "Limit" in plan_text:
        explanations.append("✅ LIMIT clause used — good practice to reduce result size.")

    if "Sort" in plan_text and "Global" in plan_text:
        explanations.append("⚠️ Global sort detected — can be expensive on large datasets.")
        explanations.append("💡 Consider sorting after limiting rows, or avoid if not necessary.")

    # Core Operators
    if "CollectLimit" in plan_text:
        explanations.append("🔢 **CollectLimit**: Limits the number of rows collected to the driver.")
    if re.search(r"\bScan hive\b", plan_text, re.IGNORECASE):
        explanations.append("📄 **Scan Hive Table**: Scans a Hive table from storage.")
    if "Filter" in plan_text:
        explanations.append("🔍 **Filter**: Applies conditions to reduce the data processed.")
    if "Project" in plan_text:
        explanations.append("📌 **Project**: Selects specific columns.")
    if "Sort" in plan_text:
        explanations.append("↕️ **Sort**: Sorts data based on specified keys.")
    if "Aggregate" in plan_text:
        explanations.append("∑ **Aggregate**: Aggregates data using functions like SUM, COUNT, AVG.")
    if "Window" in plan_text:
        explanations.append("🪟 **Window Function**: Performs analytics using window functions like `rank()` or `row_number()`.")
    if "Union" in plan_text:
        explanations.append("🔗 **Union**: Combines rows from multiple datasets.")
    if "Repartition" in plan_text:
        explanations.append("🗂 **Repartition**: Changes the number of partitions in the dataset.")
    if "Subquery" in plan_text:
        explanations.append("🔄 **Subquery**: Nested query used within the main query.")

    # Join Patterns
    if "MapJoin" in plan_text:
        explanations.append("🗺 **MapJoin**: Joins data directly on the map side. Efficient when one table is very small.")
    if "BroadcastHashJoin" in plan_text:
        explanations.append("📡 **BroadcastHashJoin**: One table is broadcasted to all nodes. Reduces shuffle cost.")
    if "ShuffleHashJoin" in plan_text:
        explanations.append("🔀 **ShuffleHashJoin**: Requires shuffling both sides of the join. ❗ Can lead to high I/O and memory use.")
    if "SortMergeJoin" in plan_text:
        explanations.append("🧩 **SortMergeJoin**: Requires sorting both sides. Good for large datasets, but costly.")

    # Exchange / Shuffle
    if "Exchange" in plan_text:
        explanations.append("🔄 **Exchange**: Redistributes data across partitions. ❗ Large exchanges may slow performance.")
    if "BroadcastExchange" in plan_text:
        explanations.append("📡 **BroadcastExchange**: Distributes small table across all workers.")
    if "ReusedExchange" in plan_text:
        explanations.append("♻️ **ReusedExchange**: Reuses previous shuffle results for efficiency.")
    if "AdaptiveSparkPlan" in plan_text:
        explanations.append("🧠 **AdaptiveSparkPlan**: Adapts query plan at runtime based on statistics.")

    # Serialization
    if "DeserializeToObject" in plan_text:
        explanations.append("📥 **DeserializeToObject**: Converts Spark SQL rows into JVM objects.")
    if "SerializeFromObject" in plan_text:
        explanations.append("📤 **SerializeFromObject**: Converts JVM objects into Spark SQL rows.")

    # Whole-stage codegen
    if "WholeStageCodegen" in plan_text:
        explanations.append("⚡ **WholeStageCodegen**: Optimizes multiple stages into compiled bytecode.")

    # Caching
    if "InMemoryRelation" in plan_text:
        explanations.append("🗄 **InMemoryTableScan**: Reads from cached data in memory.")

    # Partitioning
    if "HashPartitioning" in plan_text:
        explanations.append("🔢 **HashPartitioning**: Partitions data using hash keys.")
    if "RangePartitioning" in plan_text:
        explanations.append("📈 **RangePartitioning**: Partitions data by sorted value range.")

    # Limits
    if "GlobalLimit" in plan_text:
        explanations.append("🌐 **GlobalLimit**: Applies a limit to the total output rows.")
    if "LocalLimit" in plan_text:
        explanations.append("🏠 **LocalLimit**: Applies a per-partition row limit before global aggregation.")

    # Metadata
    if "SubqueryAlias" in plan_text:
        explanations.append("🏷 **SubqueryAlias**: Assigns alias to a subquery result.")
    if "CommandResult" in plan_text:
        explanations.append("💻 **CommandResult**: Output from commands like `SHOW TABLES`, `DESCRIBE`.")

    # Heuristics
    if "Exchange" in plan_text and "SortMergeJoin" in plan_text:
        explanations.append("⚠️ Warning: Shuffle + SortMergeJoin may indicate data skew.")

    if re.search(r"/\*\+\s*BROADCAST\((.*?)\)", plan_text):
        explanations.append("💡 Broadcast join hint used — planner instructed to broadcast a table.")

    if "CartesianProduct" in plan_text:
        explanations.append("🚨 Cartesian Product detected — joins without keys can explode data size.")

    if not explanations:
        explanations.append("⚠️ No recognizable patterns found in EXPLAIN plan.")

    return "\n".join(explanations)
