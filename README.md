🛡️ Anti-Money Laundering (AML) System in Microsoft Fabric
This project demonstrates a comprehensive, end-to-end data pipeline for Anti-Money Laundering (AML) detection using Microsoft Fabric. It implements a Medallion Lakehouse Architecture (Bronze, Silver, Gold, Platinum) to transform raw transaction data, engineer behavioral features, apply rule-based flagging, and utilize a Machine Learning model for advanced anomaly scoring.

Project Summary
The platform uses Spark (PySpark) within Fabric Notebooks to process three core datasets (accounts, transactions, alerts). It moves data through four layers:

Silver: Cleaned, structured Delta tables.

Gold: Feature-enriched data with rule-based flags (e.g., velocity).

Platinum: Final data, scored by a trained Logistic Regression model.

Consumption: Power BI dashboard for compliance analyst investigation.

Prerequisites
To deploy this solution, you need:

Microsoft Fabric Workspace: Access to a workspace with capacity.

Fabric Lakehouse: A Lakehouse created within the workspace.

Source Data Files: The following three CSV files must be uploaded to the Files section of your Lakehouse:

accounts.csv

transactions.csv

alerts.csv

Fabric Notebook: A new notebook created and attached to the Lakehouse, set as the Default context.

Data Architecture
The project follows the Medallion Architecture to ensure data quality and structure throughout the pipeline.
Layer,Input,Process,Output,Purpose
Bronze,Raw CSVs,Ingestion (Initial Upload),/Files/ (CSV),"Raw, immutable data storage."
Silver,Bronze,"Read CSV, Infer Schema, Clean",silver_* Delta Tables,"Structured, cleaned tables ready for joining."
Gold,Silver,"Joining, Window Functions, Rules",gold_aml_transactions,"Feature engineering, rule-based flagging (IS_SUSPICIOUS)."
Platinum,Gold,ML Training & Scoring,platinum_aml_transactions,Final data with predictive ML_FRAUD_SCORE.
