import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import json

# ------------------------------------
# Page Config
# ------------------------------------
st.set_page_config(
    page_title="Policy & Control Search",
    layout="wide"
)

st.title("🔍 Policy & Control Search (Cortex Search POC)")

# ------------------------------------
# Snowflake Session
# ------------------------------------
session = get_active_session()

# ------------------------------------
# Sidebar Filters
# ------------------------------------
st.sidebar.header("📂 Search Filters")

lob_list = ["All"] + [
    row[0] for row in session.sql(
        "SELECT DISTINCT LOB FROM DOCUMENT_CHUNKS ORDER BY LOB"
    ).collect()
]

state_list = ["All"] + [
    row[0] for row in session.sql(
        "SELECT DISTINCT STATE FROM DOCUMENT_CHUNKS ORDER BY STATE"
    ).collect()
]

version_list = ["All"] + [
    row[0] for row in session.sql(
        "SELECT DISTINCT VERSION FROM DOCUMENT_CHUNKS ORDER BY VERSION"
    ).collect()
]

lob = st.sidebar.selectbox("LOB", lob_list)
state = st.sidebar.selectbox("State", state_list)
version = st.sidebar.selectbox("Version", version_list)

top_k = st.sidebar.slider("Top Results", 1, 20, 5)

# ------------------------------------
# Search Input
# ------------------------------------
search_text = st.text_input(
    "Enter your search query",
    placeholder="e.g. claim settlement waiting period"
)

# ------------------------------------
# Helper: Build Cortex Filter
# ------------------------------------
def build_filter():
    filters = []

    if lob != "All":
        filters.append(f"'LOB','{lob}'")

    if state != "All":
        filters.append(f"'STATE','{state}'")

    if version != "All":
        filters.append(f"'VERSION','{version}'")

    return ",".join(filters)

# ------------------------------------
# Search Button
# ------------------------------------
if st.button("🔎 Search") and search_text.strip():

    filter_sql = build_filter()

    cortex_sql = f"""
    SELECT
        DOC_ID,
        CHUNK_TEXT,
        LOB,
        STATE,
        VERSION,
        SCORE
    FROM TABLE(
        CORTEX.SEARCH(
            'POLICY_SEARCH_SVC',
            '{search_text}',
            OBJECT_CONSTRUCT_KEEP_NULL({filter_sql}),
            {top_k}
        )
    )
    """

    # Execute Cortex Search
    result_df = session.sql(cortex_sql).to_pandas()

    # ------------------------------------
    # Display Results
    # ------------------------------------
    if result_df.empty:
        st.warning("No results found.")
    else:
        st.success(f"Found {len(result_df)} results")

        for idx, row in result_df.iterrows():
            with st.expander(f"📄 Document: {row['DOC_ID']} (Score: {row['SCORE']:.4f})"):
                st.markdown(f"**LOB:** {row['LOB']}")
                st.markdown(f"**State:** {row['STATE']}")
                st.markdown(f"**Version:** {row['VERSION']}")
                st.markdown("---")
                st.write(row["CHUNK_TEXT"])

    # ------------------------------------
    # Audit Logging
    # ------------------------------------
    audit_sql = """
    INSERT INTO POLICY_SEARCH_AUDIT
    (
        SEARCH_TEXT,
        LOB,
        STATE,
        VERSION,
        QUERY_TEXT,
        QUERY_OUTPUT,
        RESULT_COUNT,
        USER_NAME,
        ROLE_NAME,
        SEARCH_TS
    )
    SELECT
        %s,
        %s,
        %s,
        %s,
        %s,
        PARSE_JSON(%s),
        %s,
        CURRENT_USER(),
        CURRENT_ROLE(),
        CURRENT_TIMESTAMP()
    """

    session.sql(
        audit_sql,
        params=[
            search_text,
            lob if lob != "All" else None,
            state if state != "All" else None,
            version if version != "All" else None,
            cortex_sql,
            json.dumps(result_df.to_dict(orient="records")),
            len(result_df)
        ]
    ).collect()
