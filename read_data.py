# read_data.py
from pyspark.sql import SparkSession

def main():
    # --- Step 1: Create a SparkSession ---
    # This is the entry point to all PySpark functionality
    spark = SparkSession.builder \
        .appName("TrafficDataIngestion") \
        .master("local[*]") \
        .getOrCreate()
        
    print("SparkSession created successfully.")

    # Define the file path
    file_path = "traffic_violations.csv"

    # --- Step 2: Read the CSV data into a Spark DataFrame ---
    # header=True: Tells Spark the first row is the column names
    # inferSchema=True: Tells Spark to *guess* the data type (we'll fix this later)
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        print(f"Successfully read data from {file_path}")

        # --- Step 3: Show the data and schema ---
        # printSchema() shows us what data types Spark *inferred*
        print("Data Schema (What Spark *thinks* the data is):")
        df.printSchema()
        
        # .show(10) displays the first 10 rows of the DataFrame
        print("Sample of the raw, messy data:")
        df.show(10, truncate=False) # truncate=False stops it from cutting off long text

    except Exception as e:
        print(f"Error reading the file: {e}")
        print("Please make sure 'traffic_violations.csv' exists in the same folder.")

    # --- Step 4: Stop the SparkSession ---
    # Always good practice to stop the session when you're done
    spark.stop()

if __name__ == "__main__":
    main()