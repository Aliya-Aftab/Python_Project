# advanced_analysis.py
import os
from pyspark.sql import SparkSession
# We need 'when' for our IF/ELSE logic, and 'dayofweek'/'hour'
from pyspark.sql.functions import col, hour, dayofweek, when, desc

# --- HADOOP_HOME Fix ---
# We always need this when running on Windows
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
# ---------------------

def main():
    spark = SparkSession.builder \
        .appName("AdvancedPatternAnalysis") \
        .master("local[*]") \
        .getOrCreate()
        
    print("SparkSession created. Reading clean data...")

    input_path = "cleaned_traffic_data.parquet"

    # --- Step 1: Read the Clean Data ---
    try:
        df_clean = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        spark.stop()
        return

    print("Successfully read clean data.")

    # --- Step 2: Feature Engineering (Weekday/Weekend) ---
    # We create a new column 'day_type'
    # dayofweek() returns 1 for Sunday, 2 for Monday... 7 for Saturday
    # So, we'll say 1 (Sun) and 7 (Sat) are 'Weekend'
    
    df_with_features = df_clean.withColumn("dayofweek", dayofweek(col("Timestamp")))
    
    df_with_features = df_with_features.withColumn("day_type",
        when(col("dayofweek").isin([1, 7]), "Weekend")  # IF day is 1 or 7
        .otherwise("Weekday")                          # ELSE
    )

    # --- Step 3: Feature Engineering (3-Hour Windows) ---
    # We'll use a chain of 'when' statements. This is the clearest way.
    
    df_with_features = df_with_features.withColumn("hour", hour(col("Timestamp")))
    
    df_with_features = df_with_features.withColumn("time_window",
        when(col("hour").between(0, 2),   "00:00 - 02:59")
        .when(col("hour").between(3, 5),   "03:00 - 05:59")
        .when(col("hour").between(6, 8),   "06:00 - 08:59")
        .when(col("hour").between(9, 11),  "09:00 - 11:59")
        .when(col("hour").between(12, 14), "12:00 - 14:59")
        .when(col("hour").between(15, 17), "15:00 - 17:59")
        .when(col("hour").between(18, 20), "18:00 - 20:59")
        .otherwise("21:00 - 23:59")
    )
    
    print("Data with advanced features (day_type, time_window):")
    df_with_features.show(5, truncate=False)

    # --- Step 4: Run Advanced Aggregations ---

    print("--- Analysis 7: Violations by Day Type (Weekday/Weekend) ---")
    violations_by_day_type = df_with_features.groupBy("day_type") \
                                             .count() \
                                             .orderBy(desc("count"))
    violations_by_day_type.show()

    print("--- Analysis 8: Violations by 3-Hour Time Window ---")
    violations_by_window = df_with_features.groupBy("time_window") \
                                           .count() \
                                           .orderBy("time_window")
    violations_by_window.show()

    print("--- Analysis 9 (Correlation): Violation Type by Day Type ---")
    # This shows us IF some violations are more common on weekends
    type_by_day_type = df_with_features.groupBy("Violation_Type", "day_type") \
                                       .count() \
                                       .orderBy("Violation_Type", desc("count"))
    type_by_day_type.show(truncate=False)

    # --- Step 5: Save Results ---
    output_base_dir = "analysis_results"
    
    print(f"Saving new analysis tables to '{output_base_dir}'...")
    
    try:
        violations_by_day_type.write.mode("overwrite").parquet(f"{output_base_dir}/violations_by_day_type")
        violations_by_window.write.mode("overwrite").parquet(f"{output_base_dir}/violations_by_window")
        type_by_day_type.write.mode("overwrite").parquet(f"{output_base_dir}/type_by_day_type")
        
        print("Advanced analysis results saved successfully!")
    
    except Exception as e:
        print(f"Error saving analysis results: {e}")

    spark.stop()
    print("SparkSession stopped. Milestone 3 complete.")

if __name__ == "__main__":
    main()