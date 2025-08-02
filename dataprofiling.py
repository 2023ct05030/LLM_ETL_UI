"""
LangGraph Agent Script: Identify candidate primary/business key columns and SCD2 date columns from a CSV in S3.
Uses LangGraph for orchestration and a FastAPI-exposed fine-tuned model endpoint (ETL_GEN).
"""

import os
import boto3
import pandas as pd
import requests
from typing import TypedDict, List
from langgraph.graph import StateGraph, START, END
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

# Define the state structure for LangGraph
class GraphState(TypedDict, total=False):
    bucket: str
    key: str
    df: pd.DataFrame
    primary_key_candidates: List[str]
    date_columns: List[str]
    report: str

# Node 1: Load CSV data from S3 into DataFrame
def load_csv_node(state: GraphState) -> GraphState:
    bucket = state.get("bucket")
    key = state.get("key")
    if not bucket or not key:
        raise ValueError("S3 bucket and key must be provided in state.")
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        raise RuntimeError(f"Error fetching S3 object: {e}")
    try:
        df = pd.read_csv(obj['Body'])
    except Exception as e:
        raise RuntimeError(f"Error reading CSV: {e}")
    state["df"] = df
    return state

# Node 2: Identify candidate primary/business key columns
def find_keys_node(state: GraphState) -> GraphState:
    df = state.get("df")
    if df is None:
        raise ValueError("DataFrame is not loaded in state.")
    n = len(df)
    candidates = []
    for col in df.columns:
        unique_count = df[col].nunique(dropna=True)
        null_count = df[col].isnull().sum()
        if unique_count == n and null_count == 0:
            candidates.append(col)
    state["primary_key_candidates"] = candidates
    return state

# Node 3: Identify date/time columns for SCD2
def find_dates_node(state: GraphState) -> GraphState:
    df = state.get("df")
    if df is None:
        raise ValueError("DataFrame is not loaded in state.")
    candidates = []
    for col in df.columns:
        if is_datetime64_any_dtype(df[col]):
            if df[col].nunique(dropna=True) > 1:
                candidates.append(col)
            continue
        if is_numeric_dtype(df[col]):
            continue
        parsed = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
        if parsed.notna().sum() >= 2 and parsed.nunique(dropna=True) > 1:
            candidates.append(col)
        if col.endswith("dt") or col.endswith("date"):
            if col not in candidates:
                candidates.append(col)
    state["date_columns"] = candidates
    return state

# Node 4: Use external LLM API (FastAPI + ngrok) to generate summary
def generate_report_node(state: GraphState) -> GraphState:
    keys = state.get("primary_key_candidates", [])
    dates = state.get("date_columns", [])
    prompt = (
        f"The analysis found these columns:\n"
        f"- Primary/business key candidates: {keys}\n"
        f"- SCD2 date candidates: {dates}\n\n"
        f"Explain why these columns were chosen. "
        f"If no SCD2 date is found, at least return one date candidate."
    )
    try:
        response = requests.post(
            "https://c95ef4c35ed0.ngrok-free.app/predict",  # Replace with your current ngrok URL
            json={"prompt": prompt},
            timeout=60
        )
        response.raise_for_status()
        result = response.json()
        state["report"] = result.get("response", "").strip()
    except Exception as e:
        state["report"] = f"Error during API call: {e}"
    return state

# Build the LangGraph workflow
workflow = StateGraph(GraphState)
workflow.add_node("load_csv", load_csv_node)
workflow.add_node("find_keys", find_keys_node)
workflow.add_node("find_dates", find_dates_node)
workflow.add_node("summarize", generate_report_node)
workflow.add_edge(START, "load_csv")
workflow.add_edge("load_csv", "find_keys")
workflow.add_edge("find_keys", "find_dates")
workflow.add_edge("find_dates", "summarize")
workflow.add_edge("summarize", END)
agent_graph = workflow.compile()

# Main execution
if __name__ == "__main__":
    bucket = "pkn-aws-genai"
    key = "sample_sales_data.csv"
    initial_state: GraphState = {"bucket": bucket, "key": key}
    final_state = {} # Initialize final_state
    try:
        final_state = agent_graph.invoke(initial_state)
    except Exception as e:
        print(f"Error running agent: {e}")

    keys = final_state.get("primary_key_candidates", [])
    dates = final_state.get("date_columns", [])
    report = final_state.get("report", "")

    print("\nCandidate Primary/Business Key Columns:", keys)
    print("Candidate SCD2 Date Columns:", dates)
    print("\nAnalysis Summary:")
    print(report)