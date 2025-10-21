"""
Example usage of the flight data processor.

This script demonstrates how to use the process_flight_data module
to extract specific flights from large CSV files.
"""

from process_flight_data import process_flight_csvs
import pandas as pd


def example_basic_usage():
    """Basic example with default settings."""
    print("Example 1: Basic Usage")
    print("=" * 70)

    result = process_flight_csvs(
        csv_pattern="flights_2024_*.csv",  # All CSV files for 2024
        target_types=["Type1", "Type2"],   # Aircraft types to extract
        flights_per_type=3,                # 3 flights per type
        chunk_size=100000,                 # 100k rows per chunk
        output_file="filtered_flights.csv"
    )

    return result


def example_single_day():
    """Example processing a single day's CSV file."""
    print("\nExample 2: Single Day")
    print("=" * 70)

    result = process_flight_csvs(
        csv_pattern="flights_2024_01_15.csv",  # Single day
        target_types=["Boeing 737", "Airbus A320"],
        flights_per_type=5,
        chunk_size=50000,
        output_file="flights_single_day.csv"
    )

    return result


def example_multiple_types():
    """Example with multiple aircraft types."""
    print("\nExample 3: Multiple Aircraft Types")
    print("=" * 70)

    # List of aircraft types you're interested in
    aircraft_types = [
        "Boeing 737",
        "Boeing 777",
        "Airbus A320",
        "Airbus A380",
        "Cessna 172"
    ]

    result = process_flight_csvs(
        csv_pattern="flights_*.csv",
        target_types=aircraft_types,
        flights_per_type=2,  # 2 flights per type = 10 flights total
        chunk_size=100000,
        output_file="multiple_types.csv"
    )

    return result


def example_with_analysis():
    """Example that includes post-processing analysis."""
    print("\nExample 4: With Analysis")
    print("=" * 70)

    # Process the data
    result = process_flight_csvs(
        csv_pattern="flights_*.csv",
        target_types=["Type1", "Type2"],
        flights_per_type=3,
        chunk_size=100000,
        output_file="analyzed_flights.csv"
    )

    if result.empty:
        print("No data to analyze")
        return

    # Perform some analysis
    print("\n" + "=" * 70)
    print("ANALYSIS RESULTS")
    print("=" * 70)

    # Analysis 1: Track points per flight
    print("\n1. Track Points per Flight:")
    for flight_id in result['flight_id'].unique():
        flight_data = result[result['flight_id'] == flight_id]
        aircraft_type = flight_data['type'].iloc[0]
        n_points = len(flight_data)
        print(f"   {flight_id} ({aircraft_type}): {n_points} track points")

    # Analysis 2: Statistics by type
    print("\n2. Statistics by Type:")
    for aircraft_type in result['type'].unique():
        type_data = result[result['type'] == aircraft_type]
        print(f"\n   {aircraft_type}:")
        print(f"     - Total flights: {type_data['flight_id'].nunique()}")
        print(f"     - Total track points: {len(type_data)}")
        print(f"     - Avg points per flight: {len(type_data) / type_data['flight_id'].nunique():.1f}")

        # If you have altitude column
        if 'altitude' in type_data.columns:
            print(f"     - Altitude range: {type_data['altitude'].min():.0f} - {type_data['altitude'].max():.0f}")

        # If you have timestamp column
        if 'timestamp' in type_data.columns:
            print(f"     - Time range: {type_data['timestamp'].min()} to {type_data['timestamp'].max()}")

    return result


def example_large_dataset():
    """Example for processing very large datasets (adjust chunk size)."""
    print("\nExample 5: Large Dataset (Memory Optimized)")
    print("=" * 70)

    # For very large files, use smaller chunks
    result = process_flight_csvs(
        csv_pattern="flights_*.csv",
        target_types=["Type1", "Type2", "Type3"],
        flights_per_type=3,
        chunk_size=50000,  # Smaller chunks for memory efficiency
        output_file="large_dataset_filtered.csv"
    )

    return result


def example_specific_date_range():
    """Example for processing specific date range."""
    print("\nExample 6: Specific Date Range")
    print("=" * 70)

    # If your files are named like flights_YYYY_MM_DD.csv
    # You can use a more specific pattern
    result = process_flight_csvs(
        csv_pattern="flights_2024_01_*.csv",  # All of January 2024
        target_types=["Type1", "Type2"],
        flights_per_type=5,
        chunk_size=100000,
        output_file="january_flights.csv"
    )

    return result


if __name__ == "__main__":
    # Run the example you want
    # Uncomment the one you'd like to try:

    # result = example_basic_usage()
    # result = example_single_day()
    # result = example_multiple_types()
    result = example_with_analysis()
    # result = example_large_dataset()
    # result = example_specific_date_range()

    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)
