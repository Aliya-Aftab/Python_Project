# generate_data.py (7-Day Version)
import csv
import random
from datetime import datetime, timedelta

locations = ['Intersection_A', 'Intersection_B', 'Intersection_C', 'Intersection_D', None, 'N/A']
violation_types = ['Speeding', 'Red Light', 'speeding', 'Illegal U-Turn', 'No Helmet', 'RED LIGHT']
vehicle_types = ['Car', 'Motorcycle', 'Truck', 'Bus', 'car', None]
severities = ['Low', 'Medium', 'High', 'Medium']

def generate_messy_data(filename='traffic_violations.csv', num_rows=500):
    print(f"Generating {num_rows} rows of messy data (7-day range) into {filename}...")
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        writer.writerow([
            'Violation_ID', 
            'Timestamp', 
            'Location', 
            'Violation_Type', 
            'Vehicle_Type', 
            'Severity'
        ])
        
        start_time = datetime(2025, 10, 27, 8, 0, 0)
        
        for i in range(1, num_rows + 1):
            # This line adds random days (0 to 6) to make it a full week
            row_time = start_time + timedelta(days=random.randint(0, 6), minutes=i * random.randint(1, 5))
            
            if random.random() < 0.1:
                timestamp = row_time.strftime('%d/%m/%Y %H:%M') 
            else:
                timestamp = row_time.isoformat()

            loc = random.choice(locations)
            v_type = random.choice(violation_types)
            veh_type = random.choice(vehicle_types)

            row = [
                i,
                timestamp,
                loc,
                v_type,
                veh_type,
                random.choice(severities)
            ]
            
            writer.writerow(row)
            
    print("Data generation complete!")

if __name__ == "__main__":
    generate_messy_data()