#!/usr/bin/env python3
import csv
import math
import sys
import os

def convert_csv(input_file, output_file):
    """
    Converts the input CSV file by reading 'Azimuth/deg' and 'Zenith/deg' columns,
    converting them to radians, and writing a new CSV with columns:
    Time/s (sequential integer), Pitch/rad, Yaw/rad.

    Pitch is calculated as radians(90 - zenith_deg).
    Yaw is calculated as radians(azimuth_deg).
    """
    with open(input_file, newline='') as csvfile_in, open(output_file, 'w', newline='') as csvfile_out:
        reader = csv.DictReader(csvfile_in) 
        writer = csv.writer(csvfile_out)
        writer.writerow(["Time/s", "Pitch/rad", "Yaw/rad"])

        for i, row in enumerate(reader, start=1):
            azimuth_deg = float(row["Azimuth/deg"])
            zenith_deg = float(row["Zenith/deg"])

            yaw_rad = math.radians(azimuth_deg)
            pitch_rad = math.radians(90 - zenith_deg)
            

            writer.writerow([i, round(pitch_rad, 6), round(yaw_rad, 6)])

    print(f"CSV converted successfully and saved to: {output_file}")

if __name__ == "__main__":
    # Check if exactly two command-line arguments are provided: input and output filenames
    if len(sys.argv) != 3:
        print(f"Usage: {os.path.basename(sys.argv[0])} <input_file.csv> <output_file.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Verify input file exists before processing
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)

    # Call the conversion function
    convert_csv(input_file, output_file)
