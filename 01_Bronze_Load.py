#!/usr/bin/env python
# coding: utf-8

# ## 01_Bronze_Load
# 
# New notebook

# In[3]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
# Read the raw CSV file from Files section
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/raw_transactions/*.csv")

# Add ingestion metadata columns for audit trail
from pyspark.sql.functions import current_timestamp, lit

df_with_metadata = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("SAML-D_dataset"))

# Display first few rows to verify
display(df_with_metadata.limit(10))

# Save as Delta table in Bronze layer
df_with_metadata.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_transactions")


# Check row count and schema
bronze_df = spark.sql("SELECT * FROM bronze_transactions")
print(f"Total rows in bronze: {bronze_df.count()}")
bronze_df.printSchema()

# Check for the 0.1039% suspicious transactions baseline
# Note: Column name is 'Is_laundering' not 'Is Suspicious'
suspicious_count = bronze_df.filter(bronze_df["Is_laundering"] == 1).count()  # Using 1 instead of True for integer column
total_count = bronze_df.count()
percentage = (suspicious_count / total_count) * 100
print(f"Suspicious transactions: {percentage:.4f}% (baseline target: 0.1039%)")

# Also check the distribution of laundering types (typologies)
print("\nLaundering Type Distribution:")
typology_dist = bronze_df.groupBy("Laundering_type").count().orderBy("count", ascending=False)
typology_dist.show(30)  # Show all 28 typologies

# Check for the 28 typologies (11 normal, 17 suspicious)
unique_typologies = bronze_df.select("Laundering_type").distinct().count()
print(f"\nTotal unique typologies: {unique_typologies}")

