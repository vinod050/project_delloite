import json
from datetime import datetime
import os

def load_json_file(filename):
    """Helper function to load JSON data from file"""
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"File {filename} not found")
        return None
    except json.JSONDecodeError:
        print(f"Invalid JSON in {filename}")
        return None

def iso_to_milliseconds(iso_timestamp):
    """Convert ISO timestamp to milliseconds since epoch"""
    try:
        # Parse ISO timestamp (e.g., "2023-10-15T14:30:00.000Z")
        dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
        # Convert to milliseconds since epoch
        return int(dt.timestamp() * 1000)
    except ValueError as e:
        print(f"Error converting timestamp {iso_timestamp}: {e}")
        return None

def convert_format_1_to_unified(data):
    """
    IMPLEMENT: Convert data format 1 to unified format
    
    This function should take telemetry data in format 1 and convert it
    to the unified target format. Based on the hint, this format likely
    already uses millisecond timestamps.
    """
    if not data:
        return []
    
    unified_data = []
    
    # Assuming format 1 structure (adjust based on actual data-1.json)
    for record in data:
        unified_record = {
            "deviceId": record.get("device_id", ""),
            "timestamp": record.get("timestamp", 0),  # Already in milliseconds
            "temperature": record.get("temp", 0.0),
            "pressure": record.get("pressure", 0.0),
            "humidity": record.get("humidity", 0.0),
            "status": record.get("status", "unknown")
        }
        unified_data.append(unified_record)
    
    return unified_data

def convert_format_2_to_unified(data):
    """
    IMPLEMENT: Convert data format 2 to unified format
    
    This function should take telemetry data in format 2 and convert it
    to the unified target format. Based on the hint, this format likely
    uses ISO timestamps that need to be converted to milliseconds.
    """
    if not data:
        return []
    
    unified_data = []
    
    # Assuming format 2 structure with ISO timestamps (adjust based on actual data-2.json)
    for record in data:
        # Convert ISO timestamp to milliseconds
        iso_timestamp = record.get("timestamp", "")
        millisecond_timestamp = iso_to_milliseconds(iso_timestamp)
        
        if millisecond_timestamp is None:
            continue  # Skip records with invalid timestamps
        
        unified_record = {
            "deviceId": record.get("deviceId", ""),
            "timestamp": millisecond_timestamp,
            "temperature": record.get("temperature", 0.0),
            "pressure": record.get("pressure", 0.0),
            "humidity": record.get("humidity", 0.0),
            "status": record.get("status", "unknown")
        }
        unified_data.append(unified_record)
    
    return unified_data

def combine_and_sort_data(data1, data2):
    """Combine both datasets and sort by timestamp"""
    combined_data = data1 + data2
    # Sort by timestamp (ascending order)
    return sorted(combined_data, key=lambda x: x['timestamp'])

def main():
    """Main function to process and combine telemetry data"""
    print("Daikibo Industrials - IIoT Data Integration")
    print("=" * 50)
    
    # Load the two different data formats
    format1_data = load_json_file("data-1.json")
    format2_data = load_json_file("data-2.json")
    
    if format1_data is None or format2_data is None:
        print("Error: Could not load input data files")
        return
    
    print(f"Loaded {len(format1_data)} records from format 1")
    print(f"Loaded {len(format2_data)} records from format 2")
    
    # Convert both formats to unified format
    unified_data1 = convert_format_1_to_unified(format1_data)
    unified_data2 = convert_format_2_to_unified(format2_data)
    
    print(f"Converted {len(unified_data1)} records from format 1")
    print(f"Converted {len(unified_data2)} records from format 2")
    
    # Combine and sort the data
    final_data = combine_and_sort_data(unified_data1, unified_data2)
    
    print(f"Combined total: {len(final_data)} records")
    
    # Save the result
    try:
        with open("output-result.json", "w") as file:
            json.dump(final_data, file, indent=2)
        print("✅ Successfully saved unified data to output-result.json")
    except Exception as e:
        print(f"❌ Error saving result: {e}")
    
    # Display first few records for verification
    if final_data:
        print("\nFirst 3 unified records:")
        for i, record in enumerate(final_data[:3]):
            print(f"Record {i+1}: {record}")

def run_tests():
    """Simple test function to verify the implementation"""
    print("\nRunning tests...")
    
    # Test ISO timestamp conversion
    test_iso = "2023-10-15T14:30:00.000Z"
    result = iso_to_milliseconds(test_iso)
    print(f"ISO conversion test: {test_iso} -> {result}")
    
    # Test with sample data
    sample_format1 = [
        {"device_id": "DEV001", "timestamp": 1697374200000, "temp": 25.5, "pressure": 1013.2, "humidity": 60.0, "status": "active"}
    ]
    
    sample_format2 = [
        {"deviceId": "DEV002", "timestamp": "2023-10-15T14:30:00.000Z", "temperature": 26.0, "pressure": 1012.8, "humidity": 58.5, "status": "active"}
    ]
    
    unified1 = convert_format_1_to_unified(sample_format1)
    unified2 = convert_format_2_to_unified(sample_format2)
    
    print(f"✅ Format 1 conversion: {len(unified1)} records")
    print(f"✅ Format 2 conversion: {len(unified2)} records")
    
    if unified1 and unified2:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")

if __name__ == "__main__":
    # Run the main processing
    main()
    
    # Run tests
    run_tests()
