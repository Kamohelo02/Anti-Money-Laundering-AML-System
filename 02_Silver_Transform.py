#!/usr/bin/env python
# coding: utf-8

# ## 02_Silver_Transform
# 
# New notebook

# In[4]:


# CORRECTED SILVER LAYER - Using Time column as timestamp
print("=" * 60)
print("RECREATING SILVER TRANSACTIONS WITH CORRECT TIMESTAMP HANDLING")
print("=" * 60)

from pyspark.sql.functions import col, when, upper, trim, to_timestamp
from pyspark.sql.types import TimestampType

# Read from bronze
bronze_df = spark.sql("SELECT * FROM bronze_transactions")

# Check the schema to see what type Time column is
print("\n1. Checking bronze table schema:")
bronze_df.printSchema()

# Check the actual values
print("\n2. Sample of Time column values:")
bronze_df.select("Time").show(10, truncate=False)

# Check if Time is already a timestamp or string
from pyspark.sql.types import TimestampType, StringType

# Convert Time to timestamp if it's not already
if 'Time' in bronze_df.columns:
    # Try to cast Time directly to timestamp
    bronze_df = bronze_df.withColumn(
        "transaction_datetime", 
        to_timestamp(col("Time"))
    )
    
    # Count valid timestamps
    valid_count = bronze_df.filter(col("transaction_datetime").isNotNull()).count()
    total_count = bronze_df.count()
    print(f"\n3. Direct timestamp conversion from Time column:")
    print(f"   Valid: {valid_count:,} out of {total_count:,}")
    
    if valid_count == 0:
        # If that fails, try a different approach - maybe Time needs a format
        print("\n4. Trying with specific format...")
        bronze_df = bronze_df.withColumn(
            "transaction_datetime", 
            to_timestamp(col("Time"), "yyyy-MM-dd HH:mm:ss")
        )
        
        valid_count = bronze_df.filter(col("transaction_datetime").isNotNull()).count()
        print(f"   Valid with format: {valid_count:,}")
    
    if valid_count > 0:
        # Use the timestamp directly without combining with Date
        # The Time column already contains the full date and time
        
        # Remove duplicates
        silver_df = bronze_df.dropDuplicates(["transaction_datetime", "Sender_account", 
                                              "Receiver_account", "Amount"])
        
        # Filter invalid amounts
        silver_df = silver_df.filter(col("Amount") > 0)
        
        # Rename columns
        silver_df = silver_df \
            .withColumnRenamed("transaction_datetime", "transaction_time") \
            .withColumnRenamed("Sender_account", "sender_account") \
            .withColumnRenamed("Receiver_account", "receiver_account") \
            .withColumnRenamed("Amount", "amount") \
            .withColumnRenamed("Payment_type", "payment_type") \
            .withColumnRenamed("Sender_bank_location", "sender_location") \
            .withColumnRenamed("Receiver_bank_location", "receiver_location") \
            .withColumnRenamed("Payment_currency", "payment_currency") \
            .withColumnRenamed("Received_currency", "receiver_currency") \
            .withColumnRenamed("Is_laundering", "is_suspicious") \
            .withColumnRenamed("Laundering_type", "typology")
        
        # Add validation flags
        high_risk_locations = ["MEXICO", "TURKEY", "MOROCCO", "UAE"]
        
        silver_df = silver_df \
            .withColumn("sender_location_upper", upper(trim(col("sender_location")))) \
            .withColumn("receiver_location_upper", upper(trim(col("receiver_location"))))
        
        silver_df = silver_df \
            .withColumn("sender_high_risk", 
                        when(col("sender_location_upper").isin(high_risk_locations), 1).otherwise(0)) \
            .withColumn("receiver_high_risk", 
                        when(col("receiver_location_upper").isin(high_risk_locations), 1).otherwise(0)) \
            .withColumn("currency_mismatch", 
                        when(col("payment_currency") != col("receiver_currency"), 1).otherwise(0))
        
        # Standardize text fields
        silver_df = silver_df \
            .withColumn("payment_type", upper(trim(col("payment_type")))) \
            .withColumn("sender_location", upper(trim(col("sender_location")))) \
            .withColumn("receiver_location", upper(trim(col("receiver_location")))) \
            .withColumn("payment_currency", upper(trim(col("payment_currency")))) \
            .withColumn("receiver_currency", upper(trim(col("receiver_currency"))))
        
        # Drop temporary columns
        silver_df = silver_df.drop("sender_location_upper", "receiver_location_upper", "Date")
        # Keep Time? No, we renamed it to transaction_time
        
        # Save silver table
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("silver_transactions")
        
        print("\n✅ Silver layer table 'silver_transactions' created successfully!")
        
        # Verify with correct integer comparison
        print("\n5. Verifying silver_transactions:")
        
        # Check date range
        date_check = spark.sql("""
            SELECT 
                MIN(transaction_time) as min_date,
                MAX(transaction_time) as max_date,
                COUNT(DISTINCT DATE(transaction_time)) as unique_dates
            FROM silver_transactions
        """).collect()[0]
        
        print(f"   Date range: {date_check['min_date']} to {date_check['max_date']}")
        print(f"   Unique dates: {date_check['unique_dates']}")
        
        # Check suspicious count
        susp_count = spark.sql("SELECT COUNT(*) FROM silver_transactions WHERE is_suspicious = 1").collect()[0][0]
        total_count = spark.sql("SELECT COUNT(*) FROM silver_transactions").collect()[0][0]
        print(f"   Suspicious transactions: {susp_count:,} out of {total_count:,} ({susp_count/total_count*100:.4f}%)")
        
        # Show sample
        print("\n6. Sample of silver_transactions:")
        display(spark.sql("""
            SELECT transaction_time, sender_account, receiver_account, 
                   amount, is_suspicious, typology 
            FROM silver_transactions 
            LIMIT 5
        """))
    else:
        print("❌ Could not convert Time column to timestamp")
else:
    print("❌ Time column not found in bronze table")




print("\n" + "=" * 60)
print("CONTINUING SILVER LAYER CREATION")
print("=" * 60)

from pyspark.sql.functions import col, when, upper, trim

# Read from bronze again
bronze_df = spark.sql("SELECT * FROM bronze_transactions")

# Use Time directly as transaction_time (it's already a timestamp)
# Remove duplicates based on transaction details
silver_df = bronze_df.dropDuplicates(["Time", "Sender_account", 
                                      "Receiver_account", "Amount"])

# Filter out invalid amounts (must be positive)
silver_df = silver_df.filter(col("Amount") > 0)

# Rename columns to consistent naming convention
silver_df = silver_df \
    .withColumnRenamed("Time", "transaction_time") \
    .withColumnRenamed("Sender_account", "sender_account") \
    .withColumnRenamed("Receiver_account", "receiver_account") \
    .withColumnRenamed("Amount", "amount") \
    .withColumnRenamed("Payment_type", "payment_type") \
    .withColumnRenamed("Sender_bank_location", "sender_location") \
    .withColumnRenamed("Receiver_bank_location", "receiver_location") \
    .withColumnRenamed("Payment_currency", "payment_currency") \
    .withColumnRenamed("Received_currency", "receiver_currency") \
    .withColumnRenamed("Is_laundering", "is_suspicious") \
    .withColumnRenamed("Laundering_type", "typology")

# Add validation flags for AML monitoring
high_risk_locations = ["MEXICO", "TURKEY", "MOROCCO", "UAE"]

# Convert locations to uppercase for consistent comparison
silver_df = silver_df \
    .withColumn("sender_location_upper", upper(trim(col("sender_location")))) \
    .withColumn("receiver_location_upper", upper(trim(col("receiver_location"))))

# Use integer 1/0 for flags (not boolean)
silver_df = silver_df \
    .withColumn("sender_high_risk", 
                when(col("sender_location_upper").isin(high_risk_locations), 1).otherwise(0)) \
    .withColumn("receiver_high_risk", 
                when(col("receiver_location_upper").isin(high_risk_locations), 1).otherwise(0)) \
    .withColumn("currency_mismatch", 
                when(col("payment_currency") != col("receiver_currency"), 1).otherwise(0))

# Standardize text fields (uppercase for consistency)
silver_df = silver_df \
    .withColumn("payment_type", upper(trim(col("payment_type")))) \
    .withColumn("sender_location", upper(trim(col("sender_location")))) \
    .withColumn("receiver_location", upper(trim(col("receiver_location")))) \
    .withColumn("payment_currency", upper(trim(col("payment_currency")))) \
    .withColumn("receiver_currency", upper(trim(col("receiver_currency"))))

# Drop temporary columns and original Date (since we're using Time as transaction_time)
silver_df = silver_df.drop("sender_location_upper", "receiver_location_upper", "Date")

# Display first few rows to verify
print("\nSample of cleaned silver data:")
display(silver_df.limit(10))

# Show schema to verify column names
print("\nSilver layer schema:")
silver_df.printSchema()

# Save as Silver Delta table
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_transactions")

print("\n✅ Silver layer table 'silver_transactions' created successfully!")

# Verify it was saved
print("\nVerifying table was created:")
spark.sql("SHOW TABLES").show()

# Verify data quality with correct integer comparison (= 1 not = TRUE)
print("\nData Quality Validation:")
validation_df = spark.sql("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) as invalid_amounts,
        COUNT(DISTINCT sender_account) as unique_senders,
        COUNT(DISTINCT receiver_account) as unique_receivers,
        SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_count,
        SUM(CASE WHEN sender_high_risk = 1 THEN 1 ELSE 0 END) as high_risk_senders,
        SUM(CASE WHEN currency_mismatch = 1 THEN 1 ELSE 0 END) as currency_mismatches
    FROM silver_transactions
""")
display(validation_df)

# Check date range
print("\nDate Range:")
date_check = spark.sql("""
    SELECT 
        MIN(transaction_time) as min_date,
        MAX(transaction_time) as max_date,
        COUNT(DISTINCT DATE(transaction_time)) as unique_dates
    FROM silver_transactions
""").collect()[0]
print(f"Min date: {date_check['min_date']}")
print(f"Max date: {date_check['max_date']}")
print(f"Unique dates: {date_check['unique_dates']}")

# Check typology distribution
print("\nTypology Distribution (Top 10):")
typology_dist = spark.sql("""
    SELECT typology, 
           COUNT(*) as frequency,
           SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_in_typology
    FROM silver_transactions
    GROUP BY typology
    ORDER BY frequency DESC
    LIMIT 10
""")
display(typology_dist)

# Calculate suspicious percentage
susp_count = spark.sql("SELECT COUNT(*) FROM silver_transactions WHERE is_suspicious = 1").collect()[0][0]
total_count = spark.sql("SELECT COUNT(*) FROM silver_transactions").collect()[0][0]
susp_pct = (susp_count / total_count * 100) if total_count > 0 else 0
print(f"\nSuspicious transactions: {susp_count:,} out of {total_count:,} ({susp_pct:.4f}%)")
print(f"Target baseline: 0.1039%")


# In[ ]:




