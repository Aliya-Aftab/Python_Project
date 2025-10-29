# clean_data.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, try_to_timestamp, when, upper, trim, coalesce, lit

# --- THIS IS THE FIX ---
# We tell Python to set the HADOOP_HOME variable before Spark starts
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
# ---------------------

def main():
    spark = SparkSession.builder \
        .appName("TrafficDataCleaning") \
        .master("local[*]") \
        .getOrCreate()
        
    print("SparkSession created. Reading messy data...")

    file_path = "traffic_violations.csv"

    # --- Step 1: Read the CSV data ---
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    print("Raw data sample:")
    df.show(5, truncate=False)
    print("Raw data schema:")
    df.printSchema()

    # --- Step 2: Clean and Preprocess Data ---

    # 2a. Handle Missing/Null Values
    columns_to_clean = ['Location', 'Vehicle_Type']
    
    df_cleaned = df
    for col_name in columns_to_clean:
        df_cleaned = df_cleaned.withColumn(col_name,
            when(col(col_name).isNull(), "Unknown")
            .when(col(col_name) == "NULL", "Unknown")
            .when(col(col_name) == "N/A", "Unknown")
            .otherwise(col(col_name))
        )

    print("Data after handling nulls:")
    df_cleaned.show(5, truncate=False)

    # 2b. Standardize Timestamps
    df_cleaned = df_cleaned.withColumn("Timestamp",
        coalesce(
            try_to_timestamp(col("Timestamp"), lit("yyyy-MM-dd'T'HH:mm:ss")),
            try_to_timestamp(col("Timestamp"), lit("dd/MM/yyyy HH:mm"))
        )
    )

    print("Data after standardizing timestamps:")
    df_cleaned.show(5, truncate=False)
    
    # 2c. Standardize Categorical Fields (Violation_Type, Vehicle_Type)
    df_cleaned = df_cleaned.withColumn("Violation_Type", upper(trim(col("Violation_Type"))))
    df_cleaned = df_cleaned.withColumn("Vehicle_Type", upper(trim(col("Vehicle_Type"))))

    # 2d. Validate Violation Types
    valid_violations = ["SPEEDING", "RED LIGHT", "ILLEGAL U-TURN", "NO HELMET"]
    df_cleaned = df_cleaned.filter(col("Violation_Type").isin(valid_violations))

    # --- Step 3: Show Final Cleaned Data and Schema ---
    print("--- FINAL CLEANED DATA ---")
    df_cleaned.show(10, truncate=False)
    
    print("--- FINAL CLEANED SCHEMA ---")
    df_cleaned.printSchema()

    # --- Step 4: Save Cleaned Data into Parquet Format ---
    output_directory = "cleaned_traffic_data.parquet"
    
    try:
        df_cleaned.write \
            .mode("overwrite") \
            .parquet(output_directory)
        
        # We should NOT see the error message anymore
        print(f"\nSuccessfully saved cleaned data to {output_directory}")
        
    except Exception as e:
        print(f"Error saving Parquet file: {e}")

    spark.stop()

if __name__ == "__main__":
    main()