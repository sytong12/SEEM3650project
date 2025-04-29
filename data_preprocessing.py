import xml.etree.ElementTree as ET
import csv
import os
from collections import defaultdict

def process_single_file(xml_file, road_detectors, hourly_data):
    """
    Processes a single XML file and aggregates data for each lane in each road.

    Args:
        xml_file (str): Path to the XML file.
        road_detectors (dict): Dictionary mapping road names to detector IDs.
        hourly_data (dict): Dictionary to store aggregated data.
    """
    try:
        # Parse the XML file
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Extract the date
        date = root.find('date').text if root.find('date') is not None else ''

        # Iterate through periods and detectors
        for period in root.findall('.//period'):
            period_from = period.find('period_from').text if period.find('period_from') is not None else ''
            hour = period_from.split(':')[0]  # Extract the hour

            for detector in period.findall('./detectors/detector'):
                detector_id = detector.find('detector_id').text if detector.find('detector_id') is not None else ''

                # Identify the road for the detector_id
                road_name = None
                for road, detectors in road_detectors.items():
                    if detector_id in detectors:
                        road_name = road
                        break

                # Skip if detector_id is not in the specified list
                if not road_name:
                    continue

                direction = detector.find('direction').text if detector.find('direction') is not None else ''

                # Process lane data
                for lane in detector.findall('./lanes/lane'):
                    lane_id = lane.find('lane_id').text if lane.find('lane_id') is not None else ''
                    speed = float(lane.find('speed').text) if lane.find('speed') is not None else 0.0
                    occupancy = float(lane.find('occupancy').text) if lane.find('occupancy') is not None else 0.0
                    volume = int(lane.find('volume').text) if lane.find('volume') is not None else 0
                    valid = lane.find('valid').text if lane.find('valid') is not None else ''

                    # Aggregate data
                    key = (road_name, lane_id, hour, direction, valid, date)
                    if key not in hourly_data:
                        hourly_data[key] = {'total_speed': 0.0, 'total_occupancy': 0.0, 'total_volume': 0, 'count': 0}
                    
                    hourly_data[key]['total_speed'] += speed
                    hourly_data[key]['total_occupancy'] += occupancy
                    hourly_data[key]['total_volume'] += volume
                    hourly_data[key]['count'] += 1

    except Exception as e:
        print(f"Error processing file {xml_file}: {e}")


def write_aggregated_data(hourly_data, output_csv):
    """
    Writes aggregated hourly data to a CSV file.

    Args:
        hourly_data (dict): Dictionary containing aggregated data.
        output_csv (str): Path to the CSV file.
    """
    with open(output_csv, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for (road_name, lane_id, hour, direction, valid, date), data in hourly_data.items():
            avg_speed = data['total_speed'] / data['count']
            avg_occupancy = data['total_occupancy'] / data['count']
            total_volume = data['total_volume']
            writer.writerow([road_name, lane_id, hour, direction, valid, date, avg_speed, avg_occupancy, total_volume])


def aggregate_hourly_data(file_list, road_detectors, output_csv, batch_size=1000):
    """
    Aggregates data for multiple XML files and writes results to a CSV file.

    Args:
        file_list (list): List of XML file paths.
        road_detectors (dict): Dictionary mapping road names to detector IDs.
        output_csv (str): Path to the output CSV file.
        batch_size (int): Number of files to process before writing to CSV.
    """
    hourly_data = defaultdict(dict)

    # Initialize CSV with headers
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Road", "Lane", "Hour", "Direction", "Valid", "Date", "Average_Speed", "Average_Occupancy", "Total_Volume"])

    # Process files in batches
    for i, xml_file in enumerate(file_list):
        process_single_file(xml_file, road_detectors, hourly_data)

        # Write to CSV in batches
        if (i + 1) % batch_size == 0 or (i + 1) == len(file_list):
            write_aggregated_data(hourly_data, output_csv)
            hourly_data.clear()  # Clear memory

        print(f"Processed {i + 1}/{len(file_list)} files.")


# Main execution
if _name_ == "_main_":
    # Path to the directory containing XML files
    xml_directory = "./202503"  # Replace with your directory path
    output_csv_path = "aggregated_hourly_data.csv"

    # Dictionary mapping road names to detector IDs
    road_detectors = {
        "Kwun Tong Road Westbound": ["AID07108", "AID07109", "AID07110", "AID07111", "AID07113", "AID07114"],
        "Kwun Tong Road Eastbound": ["AID07219", "AID07220", "AID07221", "AID07222", "AID07223", "AID07224", "AID07225", "AID07226"],
        "New Clear Water Bay Road Eastbound": ["TDSNCWBR10001", "TDSNCWBR10002", "TDSNCWBR10003", "TDSNCWBR10004"],
        "New Clear Water Bay Road Westbound": ["TDSNCWBR20001", "TDSNCWBR20002", "TDSNCWBR20003", "TDSNCWBR20004"],
        "Prince Edward Road Northeastbound": ["TDSPERE10001", "TDSPERE10002"],
        "Prince Edward Road Southeastbound": ["TDSPERE20001", "TDSPERE20002"]
    }

    # Collect all XML files in the directory
    xml_files = [os.path.join(xml_directory, f) for f in os.listdir(xml_directory) if f.endswith('.xml')]

    # Perform aggregation
    aggregate_hourly_data(xml_files, road_detectors, output_csv_path)
