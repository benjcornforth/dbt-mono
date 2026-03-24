#!/usr/bin/env python3
"""
Parity test for WorkflowSDK vs original workflow.py
Ensures both approaches generate equivalent job configurations.
"""

import yaml

def create_test_data():
    """Create test forge config and graph data."""
    forge_config = {
        "name": "test_workflow",
        "id": "test",
        "environment": "dev",
        "active_profile": "dev",
        "compute": {"type": "serverless"},
        "catalog": "main",
        "schema": "default",
        "profiles": {
            "dev": {
                "platform": "databricks",
                "databricks_profile": "DEFAULT"
            }
        }
    }

    # Sample graph with realistic dbt models
    graph = {
        "contracts": {
            "raw_customers": {
                "dataset": {"name": "raw_customers", "type": "table"},
                "tags": ["ingest"]
            },
            "raw_orders": {
                "dataset": {"name": "raw_orders", "type": "table"},
                "tags": ["ingest"]
            },
            "stg_customers": {
                "dataset": {"name": "stg_customers", "type": "table"},
                "tags": ["stage"]
            },
            "stg_orders": {
                "dataset": {"name": "stg_orders", "type": "table"},
                "tags": ["stage"]
            },
            "customer_clean": {
                "dataset": {"name": "customer_clean", "type": "table"},
                "tags": ["clean"]
            },
            "customer_orders": {
                "dataset": {"name": "customer_orders", "type": "table"},
                "tags": ["enrich"]
            },
            "customer_summary": {
                "dataset": {"name": "customer_summary", "type": "table"},
                "tags": ["serve"]
            }
        },
        "edges": [
            {"from": "raw_customers", "to": "stg_customers"},
            {"from": "raw_orders", "to": "stg_orders"},
            {"from": "stg_customers", "to": "customer_clean"},
            {"from": "stg_orders", "to": "customer_orders"},
            {"from": "customer_clean", "to": "customer_orders"},
            {"from": "customer_orders", "to": "customer_summary"}
        ]
    }

    return forge_config, graph

def run_parity_test():
    """Run the parity test."""
    print("=== PARITY TEST: Workflow vs WorkflowSDK ===\n")

    try:
        # Import here to avoid issues
        import sys
        sys.path.insert(0, 'src')

        from forge.workflow import build_workflow
        from forge.workflow_sdk import WorkflowSDK

        # Create test data
        forge_config, graph = create_test_data()

        print("Generating original workflow...")
        original_wf = build_workflow(forge_config, graph)
        original_yml = original_wf.to_databricks_yml()

        print("Generating SDK workflow...")
        sdk_wf = WorkflowSDK(forge_config, graph, workspace_client=None)
        sdk_settings = sdk_wf._create_job_settings()

        # Parse YAML from original workflow
        original_data = yaml.safe_load(original_yml)
        original_job = original_data['resources']['jobs']
        job_name = list(original_job.keys())[0]
        original_tasks = original_job[job_name]['tasks']

        print(f"Job Name: {job_name}")
        print(f"Original tasks: {len(original_tasks)}")
        print(f"SDK tasks: {len(sdk_settings.tasks)}")

        if len(original_tasks) != len(sdk_settings.tasks):
            print(f"❌ Different number of tasks: {len(original_tasks)} vs {len(sdk_settings.tasks)}")
            print("Original tasks:", [t['task_key'] for t in original_tasks])
            print("SDK tasks:", [t.task_key for t in sdk_settings.tasks])
            return False

        # Compare tasks
        original_task_map = {t['task_key']: t for t in original_tasks}

        for sdk_task in sdk_settings.tasks:
            task_key = sdk_task.task_key
            if task_key not in original_task_map:
                print(f"❌ Task {task_key} missing in original")
                return False

            orig_task = original_task_map[task_key]

            # Compare environment keys
            orig_env_key = orig_task.get('environment_key')
            sdk_env_key = getattr(sdk_task, 'environment_key', None)
            if orig_env_key != sdk_env_key:
                print(f"❌ Environment key mismatch for {task_key}: {orig_env_key} vs {sdk_env_key}")
                return False

            # Compare dependencies
            orig_deps = [d['task_key'] for d in orig_task.get('depends_on', [])]
            sdk_deps = [d.task_key for d in getattr(sdk_task, 'depends_on', [])]
            if set(orig_deps) != set(sdk_deps):
                print(f"❌ Dependencies mismatch for {task_key}: {orig_deps} vs {sdk_deps}")
                return False

            # Compare DBT commands
            if 'dbt_task' in orig_task and sdk_task.dbt_task:
                orig_cmd = orig_task['dbt_task']['commands'][0]
                sdk_cmd = sdk_task.dbt_task.commands[0]
                if orig_cmd != sdk_cmd:
                    print(f"❌ DBT command mismatch for {task_key}: {orig_cmd} vs {sdk_cmd}")
                    return False

            print(f"✅ Task {task_key} matches")

        print("\n🎉 PARITY TEST PASSED! Both approaches generate equivalent configurations.")
        return True

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_parity_test()
    exit(0 if success else 1)