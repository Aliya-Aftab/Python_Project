# generate_data.py
import csv
import random
from datetime import datetime, timedelta

# --- This is our Schema Definition ---
# Violation_ID: Unique ID for the violation
# Timestamp: When it happened
# Location: Which intersection
# Violation_Type: What was the violation
# Vehicle_Type: What kind of vehicle
# Severity: How bad was it

# --- Lists to pull random data from ---
locations = ['Intersection_A', 'Intersection_B', 'Intersection_C', 'Intersection_D', None, 'N/A'] # Added null/bad values
violation_types = ['Speeding', 'Red Light', 'speeding', 'Illegal U-Turn', 'No Helmet', 'RED LIGHT'] # Added inconsistent values
vehicle_types = ['Car', 'Motorcycle', 'Truck', 'Bus', 'car', None] # Added inconsistent and null values
severities = ['Low', 'Medium', 'High', 'Medium']

# --- Main function to generate the file ---
def generate_messy_data(filename='traffic_violations.csv', num_rows=500):
    print(f"Generating {num_rows} rows of messy data into {filename}...")
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write the header row (our schema)
        writer.writerow([
            'Violation_ID', 
            'Timestamp', 
            'Location', 
            'Violation_Type', 
            'Vehicle_Type', 
            'Severity'
        ])
        
        start_time = datetime(2025, 10, 27, 8, 0, 0) # Start time for simulation
        
        for i in range(1, num_rows + 1):
            row_time = start_time + timedelta(minutes=i * random.randint(1, 5))
            
            # --- Introduce Intentional Errors ---
            
            # 1. Malformed Timestamps
            if random.random() < 0.1: # 10% chance
                # Write a bad date format
                timestamp = row_time.strftime('%d/%m/%Y %H:%M') 
            else:
                timestamp = row_time.isoformat()

            # 2. Missing Values (represented by None)
            loc = random.choice(locations)
            v_type = random.choice(violation_types)
            veh_type = random.choice(vehicle_types)

            # 3. Create the row
            row = [
                i,  # Violation_ID
                timestamp,
                loc,
                v_type,
                veh_type,
                random.choice(severities)
            ]
            
            writer.writerow(row)
            
    print("Data generation complete!")

# --- Run the function ---
if __name__ == "__main__":
    generate_messy_data()