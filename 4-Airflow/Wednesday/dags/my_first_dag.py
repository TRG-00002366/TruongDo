"""
My First DAG
============
Complete this file to create your first Airflow DAG.

Your DAG should:
1. Print a start message (BashOperator)
2. Process some data (PythonOperator)
3. Generate a report (BashOperator)
4. Print an end message (PythonOperator)
"""

# TODO: Import the required modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# YOUR IMPORTS HERE


# ============================================================
# Python Functions
# ============================================================

def process_data():
    """
    TODO: Implement this function.
    
    It should:
    1. Print "Processing data..."
    2. Simulate processing by printing record counts
    3. Return a dictionary with status information
    
    Example return: {"records_processed": 100, "status": "success"}
    """
    # YOUR CODE HERE
    print("Processing data...")
    print("Reading 100 records...")
    print("Transforming 100 records...")
    print("Writing 100 records...")
    return {"records_processed": 100, "status": "success"}

def generate_summary():
    """
    TODO: Implement this function.
    
    It should:
    1. Print a summary message like "Pipeline execution complete!"
    2. Print the current timestamp
    3. Return a success message
    """
    # YOUR CODE HERE
    print("Pipeline execution complete!")
    print(f"Completed at: {datetime.now()}")
    return "Pipeline finished successfully"

# ============================================================
# DAG Definition
# ============================================================

# TODO: Create the DAG using a context manager (with statement)
# 
# Required parameters:
#   - dag_id: "my_first_pipeline"
#   - start_date: datetime(2024, 1, 1)
#   - schedule: None
#   - catchup: False
#   - tags: ["exercise", "beginner"]
#
with DAG(
    dag_id="my_first_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "beginner"],
) as dag:

# YOUR DAG DEFINITION HERE


    # ============================================================
    # Tasks
    # ============================================================
    
    # TODO: Create the 'start' task
    # Use BashOperator to echo "Pipeline starting at $(date)"
    
    start = BashOperator(
        task_id="start",
        bash_command='echo "Pipeline starting at $(date)"'
    )
    
    
    # TODO: Create the 'process' task
    # Use PythonOperator to call the process_data function
    
    process = PythonOperator(
        task_id="process",
        python_callable=process_data
    )
    
    
    # TODO: Create the 'report' task
    # Use BashOperator to echo "Generating report..."
    
    report = BashOperator(
        task_id="report",
        bash_command='echo "Generating report..."'
    )
    
    
    # TODO: Create the 'end' task
    # Use PythonOperator to call the generate_summary function
    
    end = PythonOperator(
        task_id="end",
        python_callable=generate_summary
    )
    
    
    # ============================================================
    # Dependencies
    # ============================================================
    
    # TODO: Define the task dependencies
    # The order should be: start -> process -> report -> end
    # Use the >> operator
    
    start >> process >> report >> end