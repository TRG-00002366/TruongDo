# Security Analysis - Connections Exercise

## Question 1: Hardcoded Credentials

Why is it bad to hardcode passwords in DAG files?

**Your Answer:**
anyone would be able to see the password if they have access to the DAG file



## Question 2: Alternative Secret Storage

What other methods could you use to store secrets besides Airflow Connections?

List at least 3 alternatives:

1. Enviroment Variables

2. Secrets Mangers

3. Kubernetes Secrets


## Question 3: Connection Deletion

What would happen if someone accidentally deleted the connection from Airflow?

**Your Answer:**
If any connection from that airflow rely on that dag then the run will fail 



## Question 4: Production Best Practices

Research and describe one production-grade secrets management solution.

**Your Answer:**
HashiCorp Vault, this management system securely stores and controls access to sensitive credentials. 
it provides strong encryption, detailed audit logging and has an automatic secret rotation. 


---

## Reflection

What is the most important security lesson from this exercise?
Never store credentials in your code. 