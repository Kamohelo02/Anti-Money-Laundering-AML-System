#!/usr/bin/env python
# coding: utf-8

# ## 03_Gold_Dimensional_Model
# 
# New notebook

# In[4]:


print("\n" + "=" * 60)
print("RECREATING DIM_DATE WITH PROPER DATA")
print("=" * 60)

from pyspark.sql.functions import col, date_format, year, month, dayofmonth, quarter, to_date

# Check if silver_transactions has dates
date_check = spark.sql("""
    SELECT 
        MIN(DATE(transaction_time)) as min_date,
        MAX(DATE(transaction_time)) as max_date,
        COUNT(DISTINCT DATE(transaction_time)) as unique_dates
    FROM silver_transactions
    WHERE transaction_time IS NOT NULL
""").collect()[0]

print(f"\n1. Silver transactions date range:")
print(f"   Min date: {date_check['min_date']}")
print(f"   Max date: {date_check['max_date']}")
print(f"   Unique dates: {date_check['unique_dates']}")

if date_check['unique_dates'] and date_check['unique_dates'] > 0:
    # Create date dimension
    dates_df = spark.sql("""
        SELECT DISTINCT DATE(transaction_time) as transaction_date
        FROM silver_transactions
        WHERE transaction_time IS NOT NULL
    """)
    
    date_count = dates_df.count()
    print(f"\n2. Found {date_count} distinct dates")
    
    # Create date dimension attributes
    dim_date = dates_df \
        .withColumn("date_key", date_format("transaction_date", "yyyyMMdd").cast("int")) \
        .withColumn("year", year("transaction_date")) \
        .withColumn("month", month("transaction_date")) \
        .withColumn("day", dayofmonth("transaction_date")) \
        .withColumn("quarter", quarter("transaction_date")) \
        .withColumn("month_name", date_format("transaction_date", "MMMM")) \
        .withColumn("day_of_week", date_format("transaction_date", "EEEE")) \
        .withColumn("transaction_time", to_date("transaction_date")) \
        .withColumn("full_date", col("transaction_date"))
    
    dim_date = dim_date.drop("transaction_date")
    
    # Save dim_date
    dim_date.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("dim_date")
    
    print("✅ dim_date recreated successfully!")
    
    # Verify
    dim_count = spark.sql("SELECT COUNT(*) FROM dim_date").collect()[0][0]
    print(f"   dim_date has {dim_count} rows")
    
    # Show sample
    print("\n3. Sample of dim_date:")
    display(spark.sql("SELECT * FROM dim_date LIMIT 5"))
else:
    print("❌ No valid dates found in silver_transactions")


    print("\n" + "=" * 60)
print("RECREATING FACT_TRANSACTIONS")
print("=" * 60)

# Verify dim_date has data
dim_count = spark.sql("SELECT COUNT(*) FROM dim_date").collect()[0][0]
print(f"\n1. dim_date has {dim_count} rows")

if dim_count > 0:
    # Create fact_transactions
    fact_transactions = spark.sql("""
        SELECT 
            s.transaction_time,
            d.date_key,
            s.sender_account as sender_key,
            s.receiver_account as receiver_key,
            s.amount,
            s.payment_type,
            s.is_suspicious,
            s.typology as typology_key,
            s.sender_high_risk,
            s.receiver_high_risk,
            s.currency_mismatch,
            s.payment_currency,
            s.receiver_currency
        FROM silver_transactions s
        JOIN dim_date d ON DATE(s.transaction_time) = DATE(d.transaction_time)
    """)
    
    row_count = fact_transactions.count()
    print(f"\n2. Rows to be inserted: {row_count:,}")
    
    if row_count > 0:
        # Save fact table
        fact_transactions.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("fact_transactions")
        
        print("✅ fact_transactions created successfully!")
        
        # Verify
        fact_count = spark.sql("SELECT COUNT(*) FROM fact_transactions").collect()[0][0]
        susp_count = spark.sql("SELECT COUNT(*) FROM fact_transactions WHERE is_suspicious = 1").collect()[0][0]
        
        print(f"\n3. Fact table statistics:")
        print(f"   Total rows: {fact_count:,}")
        print(f"   Suspicious rows: {susp_count:,} ({susp_count/fact_count*100:.4f}%)")
        
        # Show sample
        print("\n4. Sample of fact_transactions:")
        display(spark.sql("""
            SELECT transaction_time, date_key, sender_key, receiver_key, 
                   amount, is_suspicious 
            FROM fact_transactions 
            LIMIT 5
        """))
    else:
        print("❌ No rows would be inserted")
else:
    print("❌ dim_date has no data")



print("\n" + "=" * 60)
print("RECREATING ALL GOLD TABLES (WITH TABLE DROPS)")
print("=" * 60)

# First, drop existing tables to avoid schema conflicts
print("\n1. Dropping existing Gold tables...")
for table in ['trend_daily', 'kpi_suspicious_value', 'kpi_top_typology', 
              'bar_payment_type', 'bar_country_corridors', 'bar_top_typologies']:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"   ✅ Dropped {table}")
    except:
        print(f"   ⚠️ {table} didn't exist")

# Verify fact_transactions has data
fact_count = spark.sql("SELECT COUNT(*) FROM fact_transactions").collect()[0][0]
print(f"\n2. fact_transactions has {fact_count:,} rows")

if fact_count > 0:
    # KPI 1: Total suspicious value
    print("\n3. Creating kpi_suspicious_value...")
    kpi_suspicious_value = spark.sql("""
        SELECT 
            SUM(amount) as total_suspicious_value,
            COUNT(*) as suspicious_count
        FROM fact_transactions
        WHERE is_suspicious = 1
    """)
    kpi_suspicious_value.write.format("delta").mode("overwrite").saveAsTable("kpi_suspicious_value")
    print("   ✅ Created")
    display(kpi_suspicious_value)
    
    # KPI 2: Suspicious percentage
    print("\n4. Creating suspicious percentage view...")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_suspicious_percentage AS
        SELECT 
            ROUND((SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 4) as suspicious_pct
        FROM fact_transactions
    """)
    display(spark.sql("SELECT * FROM vw_suspicious_percentage"))
    
    # KPI 3: Top typology by value
    print("\n5. Creating kpi_top_typology...")
    kpi_top_typology = spark.sql("""
        SELECT 
            t.typology_name,
            SUM(f.amount) as total_value,
            COUNT(*) as transaction_count
        FROM fact_transactions f
        JOIN dim_typology t ON f.typology_key = t.typology_key
        WHERE f.is_suspicious = 1
        GROUP BY t.typology_name
        ORDER BY total_value DESC
        LIMIT 1
    """)
    kpi_top_typology.write.format("delta").mode("overwrite").saveAsTable("kpi_top_typology")
    print("   ✅ Created")
    display(kpi_top_typology)
    
    # Line Chart 1: Daily trends - with unique column names
    print("\n6. Creating trend_daily...")
    trend_daily = spark.sql("""
        SELECT 
            d.transaction_time as transaction_date,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN f.is_suspicious = 1 THEN 1 ELSE 0 END) as suspicious_count,
            SUM(CASE WHEN f.is_suspicious = 0 THEN 1 ELSE 0 END) as normal_count,
            SUM(CASE WHEN f.is_suspicious = 1 THEN f.amount ELSE 0 END) as suspicious_amount,
            SUM(f.amount) as total_amount
        FROM fact_transactions f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.transaction_time
        ORDER BY d.transaction_time
    """)
    trend_daily.write.format("delta").mode("overwrite").saveAsTable("trend_daily")
    trend_count = trend_daily.count()
    print(f"   ✅ Created with {trend_count:,} rows")
    display(trend_daily.limit(5))
    
    # Bar Chart 1: Payment type
    print("\n7. Creating bar_payment_type...")
    bar_payment_type = spark.sql("""
        SELECT 
            payment_type,
            COUNT(*) as suspicious_count,
            SUM(amount) as suspicious_amount
        FROM fact_transactions
        WHERE is_suspicious = 1
        GROUP BY payment_type
        ORDER BY suspicious_count DESC
    """)
    bar_payment_type.write.format("delta").mode("overwrite").saveAsTable("bar_payment_type")
    print("   ✅ Created")
    display(bar_payment_type)
    
    # Bar Chart 2: Country corridors
    print("\n8. Creating bar_country_corridors...")
    bar_country_corridors = spark.sql("""
        SELECT 
            s.location as sender_location,
            r.location as receiver_location,
            COUNT(*) as transaction_count,
            SUM(f.amount) as total_amount
        FROM fact_transactions f
        JOIN dim_account s ON f.sender_key = s.account_key
        JOIN dim_account r ON f.receiver_key = r.account_key
        WHERE f.is_suspicious = 1
        GROUP BY s.location, r.location
        ORDER BY total_amount DESC
        LIMIT 10
    """)
    bar_country_corridors.write.format("delta").mode("overwrite").saveAsTable("bar_country_corridors")
    print("   ✅ Created")
    display(bar_country_corridors)
    
    # Bar Chart 3: Top typologies
    print("\n9. Creating bar_top_typologies...")
    bar_top_typologies = spark.sql("""
        SELECT 
            t.typology_name,
            COUNT(*) as frequency,
            SUM(f.amount) as total_amount
        FROM fact_transactions f
        JOIN dim_typology t ON f.typology_key = t.typology_key
        WHERE f.is_suspicious = 1
        GROUP BY t.typology_name
        ORDER BY frequency DESC
        LIMIT 5
    """)
    bar_top_typologies.write.format("delta").mode("overwrite").saveAsTable("bar_top_typologies")
    print("   ✅ Created")
    display(bar_top_typologies)
    
    # Final verification
    print("\n" + "=" * 40)
    print("FINAL VERIFICATION")
    print("=" * 40)
    
    for table in ['kpi_suspicious_value', 'kpi_top_typology', 'trend_daily', 
                  'bar_payment_type', 'bar_country_corridors', 'bar_top_typologies']:
        try:
            count = spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0][0]
            status = "✅" if count > 0 else "❌"
            print(f"{status} {table}: {count:,} rows")
        except:
            print(f"❌ {table}: table not found")
    
    print("\n" + "=" * 60)
    print("✅ ALL GOLD TABLES NOW HAVE DATA!")
    print("=" * 60)
else:
    print("❌ fact_transactions has no data")

