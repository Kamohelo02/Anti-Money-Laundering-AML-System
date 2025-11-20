🛡️ Anti-Money Laundering (AML) System using Microsoft Fabric
This repository hosts a comprehensive, end-to-end solution for detecting potential money laundering activities. The platform is architected within Microsoft Fabric and leverages its unified data and analytics capabilities, including Lakehouse, PySpark Notebooks, Spark MLlib, MLflow, and Power BI.

The solution implements a dual detection strategy: Rule-Based Flagging and Machine Learning-Based Anomaly Scoring.

1. Project Overview and Architecture
The platform processes three core datasets (accounts, transactions, alerts) through a four-stage Medallion Architecture to ensure data quality, structure, and enrichment at every phase.
<img width="703" height="270" alt="Screenshot (577)" src="https://github.com/user-attachments/assets/abbc5ada-f12a-4a1a-8ae0-eabc8199bd15" />


2. Prerequisites
Before deploying the pipeline, ensure the following are in place:

Microsoft Fabric Capacity: Access to a Fabric Workspace.

Fabric Lakehouse: A Lakehouse created within the workspace.

Data Upload: The source CSV files (accounts.csv, transactions.csv, alerts.csv) must be uploaded to the Files section of the Lakehouse.

Notebook Setup: A new Fabric Notebook must be created and explicitly attached to the Lakehouse, setting it as the Default storage context.
3. Deployment Steps (PySpark Code)
The following steps are executed sequentially in separate cells within the Fabric Notebook.

3.1 Step 1: Data Transformation (Bronze to Silver)
This script reads the raw CSV files and persists them as optimized Delta tables, forming the Silver Layer.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("AML_Ingestion").getOrCreate()

# Load and Transform ACCOUNTS Data
df_accounts = spark.read.csv("/lakehouse/default/Files/accounts.csv", header=True, inferSchema=True)
df_accounts.write.format("delta").mode("overwrite").saveAsTable("silver_accounts")

# Load and Transform TRANSACTIONS Data
df_transactions = spark.read.csv("/lakehouse/default/Files/transactions.csv", header=True, inferSchema=True)
df_transactions.write.format("delta").mode("overwrite").saveAsTable("silver_transactions")

# Load and Transform ALERTS Data (selecting only key columns for the join later)
df_alerts = spark.read.csv("/lakehouse/default/Files/alerts.csv", header=True, inferSchema=True).select("TX_ID", "ALERT_ID", "ALERT_TYPE")
df_alerts.write.format("delta").mode("overwrite").saveAsTable("silver_alerts")
print("Silver layer tables created successfully.")

3.2 Step 2: Feature Engineering and Rule Logic (Silver to Gold)
This script performs data enrichment, calculates behavioral features using Spark Window Functions, and applies conditional rule-based flags.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("AML_Feature_Engineering").getOrCreate()

df_tx = spark.read.table("silver_transactions")
df_accounts = spark.read.table("silver_accounts")
df_alerts = spark.read.table("silver_alerts")

# Joining Account Details (Sender and Receiver)
sender_cols = ["ACCOUNT_ID", "INIT_BALANCE", "IS_FRAUD", "COUNTRY"]
df_sender_details = df_accounts.select([F.col(c).alias("SENDER_" + c) for c in sender_cols]).withColumnRenamed("SENDER_ACCOUNT_ID", "ACCOUNT_ID")
receiver_cols = ["ACCOUNT_ID", "INIT_BALANCE", "IS_FRAUD", "COUNTRY"]
df_receiver_details = df_accounts.select([F.col(c).alias("RECEIVER_" + c) for c in receiver_cols]).withColumnRenamed("RECEIVER_ACCOUNT_ID", "ACCOUNT_ID")

df_enriched = df_tx.alias("t").join(df_sender_details.alias("s"), F.col("t.SENDER_ACCOUNT_ID") == F.col("s.SENDER_ACCOUNT_ID"), "left")
df_enriched = df_enriched.join(df_receiver_details.alias("r"), F.col("t.RECEIVER_ACCOUNT_ID") == F.col("r.RECEIVER_ACCOUNT_ID"), "left")

# Behavioral Profiling (Velocity and Deviation)
sender_window_spec = Window.partitionBy("SENDER_ACCOUNT_ID").orderBy("TIMESTAMP").rowsBetween(-10, 0)

df_features = df_enriched.withColumn(
    "SENDER_TX_VELOCITY_10STEP", F.count("TX_ID").over(sender_window_spec)
).withColumn(
    "SENDER_AVG_TX_AMOUNT_10STEP", F.avg("TX_AMOUNT").over(sender_window_spec)
).withColumn(
    "SENDER_TX_AMOUNT_DEVIATION", F.col("TX_AMOUNT") - F.col("SENDER_AVG_TX_AMOUNT_10STEP")
)

# Alert History Join
df_features = df_features.join(
    df_alerts.select("TX_ID", F.col("ALERT_ID").alias("EXISTING_ALERT_ID")), on="TX_ID", how="left"
).withColumn(
    "HAS_PREVIOUS_ALERT", F.when(F.col("EXISTING_ALERT_ID").isNotNull(), F.lit(True)).otherwise(F.lit(False))
).drop("EXISTING_ALERT_ID")

# Rule-Based Alerting
LARGE_TX_THRESHOLD = 500.00
HIGH_VELOCITY_THRESHOLD = 5
df_gold = df_features.withColumn("IS_SUSPICIOUS", F.lit(False))

# Rule 1: Large Transaction
df_gold = df_gold.withColumn(
    "ALERT_Large_Transaction", F.when(F.col("TX_AMOUNT") > LARGE_TX_THRESHOLD, F.lit(True)).otherwise(F.lit(False))
).withColumn("IS_SUSPICIOUS", F.when(F.col("ALERT_Large_Transaction"), F.lit(True)).otherwise(F.col("IS_SUSPICIOUS")))

# Rule 2: High Velocity
df_gold = df_gold.withColumn(
    "ALERT_High_Velocity", F.when(F.col("SENDER_TX_VELOCITY_10STEP") > HIGH_VELOCITY_THRESHOLD, F.lit(True)).otherwise(F.lit(False))
).withColumn("IS_SUSPICIOUS", F.when(F.col("ALERT_High_Velocity"), F.lit(True)).otherwise(F.col("IS_SUSPICIOUS")))

# Rule 3: Fraud Account Counterparty
df_gold = df_gold.withColumn(
    "ALERT_Fraud_Counterparty", F.when(F.col("RECEIVER_IS_FRAUD") == True, F.lit(True)).otherwise(F.lit(False))
).withColumn("IS_SUSPICIOUS", F.when(F.col("ALERT_Fraud_Counterparty"), F.lit(True)).otherwise(F.col("IS_SUSPICIOUS")))

# Persist Gold Layer
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_aml_transactions")
print("GOLD LAYER 'gold_aml_transactions' PERSISTED SUCCESSFULLY.")

3.3 Step 3: Machine Learning & Scoring (Gold to Platinum)
This step trains a Logistic Regression model on the engineered features and assigns a continuous ML_FRAUD_SCORE (probability of fraud) to every transaction, forming the Platinum Layer.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vector
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import mlflow

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("AML_ML_Training_Final").getOrCreate()

# UDF FIX: Function to safely extract the probability of the positive class (index 1)
def extract_prob(v):
    # Spark's probability column is a VectorUDT; this uses Python logic to safely access the score.
    if isinstance(v, Vector) and v.size > 1:
        return float(v[1])
    return 0.0

extract_prob_udf = udf(extract_prob, FloatType())

# Load Gold data and prepare features
df_gold = spark.read.table("gold_aml_transactions")
LABEL_COL = "SENDER_IS_FRAUD" 
FEATURE_COLS = ["TX_AMOUNT", "SENDER_INIT_BALANCE", "SENDER_TX_VELOCITY_10STEP", "SENDER_TX_AMOUNT_DEVIATION", "HAS_PREVIOUS_ALERT"]

df_ml = df_gold.withColumn("label", F.when(F.col(LABEL_COL) == True, F.lit(1.0)).otherwise(F.lit(0.0)))
df_ml = df_ml.withColumn("HAS_PREVIOUS_ALERT", F.col("HAS_PREVIOUS_ALERT").cast("double"))
df_ml = df_ml.na.fill(0, subset=FEATURE_COLS)

assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
df_model_input = assembler.transform(df_ml)
(training_data, test_data) = df_model_input.randomSplit([0.7, 0.3], seed=42)

# Model Training and MLflow Tracking
mlflow.set_experiment("AML_Fraud_Detection_Model")
with mlflow.start_run():
    lr = LogisticRegression(featuresCol="features", labelCol="label", family="binomial", maxIter=10)
    lr_model = lr.fit(training_data)
    
    # Register model in Fabric Model Registry
    mlflow.spark.log_model(spark_model=lr_model, artifact_path="lr_aml_model", registered_model_name="aml_fraud_lr")

# Scoring and Platinum Layer Creation
df_scored = lr_model.transform(df_model_input)

# Apply UDF to extract the ML_FRAUD_SCORE
df_scored = df_scored.withColumn("ML_FRAUD_SCORE", extract_prob_udf(F.col("probability")))

# Persist Platinum Layer
temp_cols_to_drop = ["features", "rawPrediction", "probability", "prediction", "label"]
df_platinum = df_scored.select(*[col for col in df_scored.columns if col not in temp_cols_to_drop])
df_platinum.write.format("delta").mode("overwrite").saveAsTable("platinum_aml_transactions")

print("PLATINUM LAYER TABLE 'platinum_aml_transactions' PERSISTED SUCCESSFULLY.")

4. Reporting and Visualization (Power BI)
The final step is to create the interactive dashboard using the rich platinum_aml_transactions table.

4.1 Connect to Data
In the Fabric UI, navigate to the Lakehouse.

Select the platinum_aml_transactions table.

Initiate report creation by selecting "New Power BI Report".

4.2 Creating the Alert Source Calculated Column (DAX)
To categorize alerts from multiple flags and the ML score into a single chart dimension, create this DAX column:

In the Power BI editor, switch to the Table View (Data View icon).

Click New Column and enter the following DAX:
Alert Source = 
IF('platinum_aml_transactions'[ALERT_Large_Transaction] = TRUE(), "Rule: Large Tx", 
IF('platinum_aml_transactions'[ALERT_High_Velocity] = TRUE(), "Rule: High Velocity",
IF('platinum_aml_transactions'[ALERT_Fraud_Counterparty] = TRUE(), "Rule: Fraud Counterparty",
IF('platinum_aml_transactions'[ML_FRAUD_SCORE] >= 0.8, "ML: High Score Anomaly", "Other/ML Alert"))))

4.3 Key Dashboard Visuals
<img width="703" height="331" alt="Screenshot (579)" src="https://github.com/user-attachments/assets/c927dff9-cd7b-44da-8f92-b85d42868e29" />
<img width="1920" height="1080" alt="Screenshot (580)" src="https://github.com/user-attachments/assets/0e3204d8-dd47-4292-9b22-7e78469a82b1" />
<img width="1920" height="1080" alt="Screenshot (574)" src="https://github.com/user-attachments/assets/b7201be8-833c-47a4-a103-f413ee730f3d" />



<img width="1920" height="1080" alt="Screenshot (580)" src="https://github.com/user-attachments/assets/d5e02735-e350-4f13-8e8d-2d44a614c4a2" />
<img width="1920" height="1080" alt="Screenshot (574)" src="https://github.com/user-attachments/assets/1b0edfb5-05d6-4406-bfc2-edb6fc4bb577" />



