#!/usr/bin/env python3
"""
Turbopuffer MCP Server

An MCP server that provides access to Turbopuffer's vector search and filtering capabilities.
Supports vector search, full-text search, hybrid search, and complex filtering operations.

This server integrates directly with Claude Desktop via the Model Context Protocol.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
from typing import List, Optional
import turbopuffer as tpuf
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

# Initialize the FastMCP server
mcp = FastMCP("turbopuffer")


def setup_turbopuffer_client():
    """Setup the turbopuffer client with API key from environment."""
    api_key = os.getenv("TURBOPUFFER_API_KEY")
    if not api_key:
        raise ValueError("TURBOPUFFER_API_KEY environment variable is required")
    tpuf.api_key = api_key

    base_url = os.getenv("TURBOPUFFER_BASE_URL")
    if not base_url:
        raise ValueError("TURBOPUFFER_BASE_URL environment variable is required")
    tpuf.api_base_url = base_url


def format_query_results(results) -> str:
    """Format query results into a readable string."""
    if hasattr(results, "rows") and results.rows:
        df = pd.DataFrame([row.attributes for row in results.rows])
        return tabulate(df, headers="keys", tablefmt="psql", showindex=True)
    return results


@mcp.tool()
async def search_for_journey(
    namespace: str,
    stop_name: str,
    direction: str,
) -> str:
    """
    Perform full-text search using BM25 scoring in a Turbopuffer namespace.

    Args:
        namespace: The namespace to search in
        stop_name: The name of the stop to search for
        direction: Direction to filter results (e.g. "North" for northbound, "South" for southbound)
    """
    try:
        setup_turbopuffer_client()

        time_now = datetime.now().time().strftime("%H:%M:%S")
        query_time_now = pd.to_datetime(time_now, format="%H:%M:%S")
        time_plus = (datetime.now() + timedelta(minutes=30)).time().strftime("%H:%M:%S")
        query_time_plus = pd.to_datetime(time_plus, format="%H:%M:%S")

        weekday = datetime.now().strftime("%A")
        if weekday == "Sunday":
            service_id = "Sunday"
        elif weekday == "Saturday":
            service_id = "Saturday"
        else:
            service_id = "Weekday"

        if direction.lower() == "northbound":
            direction = "North"
        elif direction.lower() == "southbound":
            direction = "South"

        ns = tpuf.Namespace(namespace)
        results = ns.query(
            rank_by=["stop_name", "BM25", stop_name],
            filters=[
                "And",
                [
                    ["direction", "Eq", direction],
                    ["service_id", "Eq", service_id],
                    ["arrival_time_epoch", "Gt", query_time_now.value],
                    ["arrival_time_epoch", "Lt", query_time_plus.value],
                ],
            ],
            top_k=20,
            include_attributes=[
                "route_name",
                "direction",
                "trip_headsign",
                "stop_name",
                "stop_sequence",
                "arrival_time",
                "next_stop_name",
            ],
        )
        return format_query_results(results)

    except Exception as e:
        return f"Full-text search failed: {str(e)}"


@mcp.tool()
async def list_namespaces() -> str:
    """
    List all available namespaces in your Turbopuffer account.
    """
    try:
        setup_turbopuffer_client()
        namespaces = tpuf.namespaces()

        if not namespaces:
            return "No namespaces found in your account"

        output = ["Available Namespaces:"]
        output.append("=" * 40)
        for namespace in namespaces:
            output.append(f"- {namespace.name}")
        return "\n".join(output)

    except Exception as e:
        return f"Failed to list namespaces: {str(e)}"


@mcp.tool()
async def get_namespace_schema(namespace: str) -> str:
    """
    Get the schema information for a specific namespace.

    Args:
        namespace: The namespace to get schema information for
    """
    try:
        setup_turbopuffer_client()
        ns = tpuf.Namespace(namespace)
        schema = ns.schema()
        output = []
        for k, v in schema.items():
            output.append(f"{k}: {v.as_dict()}")
        return "\n".join(output)
    except Exception as e:
        return f"Failed to get schema for namespace '{namespace}': {str(e)}"


@mcp.tool()
async def get_time() -> str:
    return datetime.now().strftime("%H:%M:%S")


if __name__ == "__main__":
    load_dotenv()
    setup_turbopuffer_client()
    try:
        mcp.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
        exit(0)
