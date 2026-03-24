#!/usr/bin/env python3
"""
Test script for WorkflowSDK - demonstrates minimal overhead workflow creation.
"""

import json
from forge.workflow_sdk import WorkflowSDK

# Sample forge config
forge_config = {
    "name": "test_workflow",
    "id": "test",
    "environment": "dev",
    "active_profile": "dev",
    "compute": {"type": "serverless"},
    "profiles": {
        "dev": {
            "platform": "databricks",
            "databricks_profile": "DEFAULT"
        }
    }
}

# Sample graph (simplified)
graph = {
    "contracts": {
        "raw_customers": {
            "dataset": {"name": "raw_customers", "type": "table"},
            "tags": ["ingest"]
        },
        "stg_customers": {
            "dataset": {"name": "stg_customers", "type": "table"},
            "tags": ["stage"]
        },
        "customer_clean": {
            "dataset": {"name": "customer_clean", "type": "table"},
            "tags": ["clean"]
        }
    },
    "edges": [
        {"from": "raw_customers", "to": "stg_customers"},
        {"from": "stg_customers", "to": "customer_clean"}
    ]
}

def test_workflow_sdk():
    """Test the WorkflowSDK functionality."""
    print("Testing WorkflowSDK...")

    # Create workflow
    wf = WorkflowSDK(forge_config, graph)

    print(f"Created workflow: {wf.name}")
    print(f"Number of tasks: {len(wf.tasks)}")

    for task in wf.tasks:
        print(f"  - {task.name}: {task.stage} ({len(task.models)} models)")

    # Test job settings creation
    try:
        job_settings = wf._create_job_settings()
        print(f"Job settings created successfully: {job_settings.name}")
        print(f"Number of job tasks: {len(job_settings.tasks)}")

        # Test job creation (commented out to avoid actual API calls)
        # result = wf.create_or_update_job()
        # print(f"Job operation result: {result}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    print("WorkflowSDK test completed!")

if __name__ == "__main__":
    test_workflow_sdk()