# Task 1: Observations
- What databases exist by default?
    - DEV_DB, SNOWFLAKE, SNOWFLAKE_LEARNING_DB, SNOWFLAKE_SAMPLE_DATA, USER$TRUONGDO

- What is the name of the default virtual warehouse?
    - COMPUTE_WH

- What role are you currently using?
    - ACCOUNTADMIN

# Task 3: Observations
- Record the execution time of your COUNT(*) query.
    - 76ms

## 1. Architecture Diagram
- Cloud Services Layer: 
    - The Cloud Services layer is the controls, this is where it manages everything except for storage and query execution 
- Query Processing Layer (Virtual Warehouses): 
    - The Query Processing layer is the computte layer. this is where queries are being executed. 
- Database Storage Layer: 
    - The Database Storage layer is the data layer, where all the data resides. 

## 2. Observation Table:
| Component | What You Observed | Purpose |
| :--- | :--- | :--- |
| Virtual Warehouse | A compute resource with a name, size, and status | Executes queries and performs data processing. |
| Database | Logical container that holds schemas and data. | Organizes and stores data. |
| Schema | Sub-container inside a database. | Organizing and stores data. |
| Table | Structured data with rows and columns. | Store actual data used for analysis, transformation, and reportings. |
| Role | Security entity | Manage permissions and enforce access control. |

## 3. Cost Control Setting
- AUTO_SUSPEND automatically pauses a virtual warehouse after period of inactivity, this can help minimize compute costs.