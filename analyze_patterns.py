# analyze_patterns.py
import os
from pyspark.sql import SparkSession
# We'll need lots of functions for this one!
from pyspark.sql.functions import col, hour, dayofweek, month, year, date_format, desc

# --- THIS IS THE FIX ---
# We still need this for Windows to work correctly
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
# ---------------------

def main():
    spark = SparkSession.builder \
        .appName("TrafficPatternAnalysis") \
        .master("local[*]") \
        .getOrCreate()
        
    print("SparkSession created. Reading clean data...")

    # Define the input path (our clean data)
    input_path = "cleaned_traffic_data.parquet"

    # --- Step 1: Read the Clean Parquet Data ---
    try:
        df_clean = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        print(f"Please make sure the '{input_path}' folder exists and is not empty.")
        spark.stop()
        return

    print("Successfully read clean data:")
    df_clean.show(5, truncate=False)
    df_clean.printSchema()

    # --- Step 2: Week 3 - Derive Time-Based Features ---
    # We add new columns based on the 'Timestamp'
    df_with_features = df_clean.withColumn("hour", hour(col("Timestamp"))) \
                               .withColumn("dayofweek", dayofweek(col("Timestamp"))) \
                               .withColumn("day_name", date_format(col("Timestamp"), "E")) \
                               .withColumn("month", month(col("Timestamp"))) \
                               .withColumn("year", year(col("Timestamp")))
    
    print("Data with new time features added:")
    df_with_features.show(5, truncate=False)

    # --- Step 3: Week 3 - Aggregations ---
    
    print("--- Analysis 1: Violations per Hour of Day ---")
    violations_per_hour = df_with_features.groupBy("hour") \
                                          .count() \
                                          .orderBy("hour")
    violations_per_hour.show(24)

    print("--- Analysis 2: Violations per Day of Week ---")
    violations_per_day = df_with_features.groupBy("dayofweek", "day_name") \
                                         .count() \
                                         .orderBy("dayofweek")
    violations_per_day.show(7)

    print("--- Analysis 3: Violations by Type of Offense ---")
    violations_by_type = df_with_features.groupBy("Violation_Type") \
                                         .count() \
                                         .orderBy(desc("count"))
    violations_by_type.show(truncate=False)

    print("--- Analysis 4: Cross-Tab: Violation Type x Hour of Day ---")
    # This shows what types of violations happen at what hour
    violations_crosstab = df_with_features.crosstab("Violation_Type", "hour")
    violations_crosstab.show(truncate=False)

    # --- Step 4: Week 4 - Location-Based Aggregations ---

    print("--- Analysis 5: Total Violations per Location ---")
    violations_per_location = df_with_features.groupBy("Location") \
                                              .count() \
                                              .orderBy(desc("count"))
    violations_per_location.show(truncate=False)

    print("--- Analysis 6: Top 3 High-Violation Locations ---")
    top_3_locations = violations_per_location.limit(3)
    top_3_locations.show(truncate=False)

    # --- Step 5: Save All Results ---
    # We save each analysis into its own Parquet file
    output_base_dir = "analysis_results"
    
    print(f"Saving all analysis tables to '{output_base_dir}'...")
    
    try:
        violations_per_hour.write.mode("overwrite").parquet(f"{output_base_dir}/violations_per_hour")
        violations_per_day.write.mode("overwrite").parquet(f"{output_base_dir}/violations_per_day")
        violations_by_type.write.mode("overwrite").parquet(f"{output_base_dir}/violations_by_type")
        violations_crosstab.write.mode("overwrite").parquet(f"{output_base_dir}/violations_crosstab")
        violations_per_location.write.mode("overwrite").parquet(f"{output_base_dir}/violations_per_location")
        top_3_locations.write.mode("overwrite").parquet(f"{output_base_dir}/top_3_locations")
        
        print("All analysis results saved successfully!")
    
    except Exception as e:
        print(f"Error saving analysis results: {e}")

    spark.stop()
    print("SparkSession stopped. Milestone 2 complete.")

if __name__ == "__main__":
    main()