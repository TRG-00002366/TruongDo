# Execution Log - My First DAG

## DAG Information

**DAG ID:** my_first_pipeline  
**Execution Date:**  
**Status:** 

---

## Screenshots

### 1. DAG Grid View (Successful Run)

[Paste your screenshot here or describe what you see]
It tells me how many total runs are displayed which is 1 and how many total success
with First run start and Last run start 
First - 2026-03-14, 00:07:40 UTC
Last - 	2026-03-14, 00:07:40 UTC
with run durations all at 3 seconds
### 2. Task Logs - Process Task

[Paste the relevant log output showing your print statements]
***   * /opt/airflow/logs/dag_id=my_first_pipeline/run_id=manual__2026-03-14T00:07:40.002783+00:00/task_id=start/attempt=1.log
[2026-03-14, 00:07:41 UTC] {taskinstance.py:1159} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: my_first_pipeline.start manual__2026-03-14T00:07:40.002783+00:00 [queued]>
[2026-03-14, 00:07:41 UTC] {taskinstance.py:1159} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: my_first_pipeline.start manual__2026-03-14T00:07:40.002783+00:00 [queued]>
[2026-03-14, 00:07:41 UTC] {taskinstance.py:1361} INFO - Starting attempt 1 of 1
[2026-03-14, 00:07:41 UTC] {taskinstance.py:1382} INFO - Executing <Task(BashOperator): start> on 2026-03-14 00:07:40.002783+00:00
[2026-03-14, 00:07:41 UTC] {standard_task_runner.py:57} INFO - Started process 355 to run task
[2026-03-14, 00:07:41 UTC] {standard_task_runner.py:84} INFO - Running: ['***', 'tasks', 'run', 'my_first_pipeline', 'start', 'manual__2026-03-14T00:07:40.002783+00:00', '--job-id', '3', '--raw', '--subdir', 'DAGS_FOLDER/my_first_dag.py', '--cfg-path', '/tmp/tmpd9yqhbpp']
[2026-03-14, 00:07:41 UTC] {standard_task_runner.py:85} INFO - Job 3: Subtask start
[2026-03-14, 00:07:41 UTC] {task_command.py:416} INFO - Running <TaskInstance: my_first_pipeline.start manual__2026-03-14T00:07:40.002783+00:00 [running]> on host d316c86a4e19
[2026-03-14, 00:07:41 UTC] {taskinstance.py:1662} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='***' AIRFLOW_CTX_DAG_ID='my_first_pipeline' AIRFLOW_CTX_TASK_ID='start' AIRFLOW_CTX_EXECUTION_DATE='2026-03-14T00:07:40.002783+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-03-14T00:07:40.002783+00:00'
[2026-03-14, 00:07:41 UTC] {subprocess.py:63} INFO - Tmp dir root location: /tmp
[2026-03-14, 00:07:41 UTC] {subprocess.py:75} INFO - Running command: ['/bin/bash', '-c', 'echo "Pipeline starting at $(date)"']
[2026-03-14, 00:07:41 UTC] {subprocess.py:86} INFO - Output:
[2026-03-14, 00:07:41 UTC] {subprocess.py:93} INFO - Pipeline starting at Sat Mar 14 00:07:41 UTC 2026
[2026-03-14, 00:07:41 UTC] {subprocess.py:97} INFO - Command exited with return code 0
[2026-03-14, 00:07:41 UTC] {taskinstance.py:1400} INFO - Marking task as SUCCESS. dag_id=my_first_pipeline, task_id=start, execution_date=20260314T000740, start_date=20260314T000741, end_date=20260314T000741
[2026-03-14, 00:07:41 UTC] {local_task_job_runner.py:228} INFO - Task exited with return code 0
[2026-03-14, 00:07:41 UTC] {taskinstance.py:2778} INFO - 1 downstream tasks scheduled from follow-on schedule check

---

## Reflection Questions

### Question 1
What is the difference between an Operator and a Task?

**Your Answer:**
An operator defines the type of work, while task is the unit of work that airflow schedules and executes 

### Question 2
Why did we set `catchup=False`? What would happen if it was True?

**Your Answer:**
we set up false so it only run the latest scheduled Dag, if true it would executes all missed dags runs since the start date

### Question 3
What does the `>>` operator do in Airflow?

**Your Answer:**
tells which task should run before another task 

### Question 4
If the `process` task failed, what would happen to the `report` and `end` tasks?

**Your Answer:**
if 'process' failed then the 'report' and 'end' task will not run 

---

## Task Execution Times

| Task | Duration | Status |
|------|----------|--------|
| start | | |
| process | | |
| report | | |
| end | | |

---

## Issues Encountered

Describe any problems you faced and how you solved them:
no problem

---

## Key Learnings

What are the 3 most important things you learned from this exercise?

1. what dags are
2. how dags work
3. how to set up a dag with airflow