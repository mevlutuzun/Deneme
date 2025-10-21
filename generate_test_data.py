"""
Generate sample flight CSV data for testing the flight processor.

This creates realistic test data with multiple flights and track points.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random


def generate_flight_track(flight_id, aircraft_type, n_points=50, start_time=None):
    """
    Generate a realistic flight track with multiple state records.

    Args:
        flight_id: Unique flight identifier
        aircraft_type: Type of aircraft
        n_points: Number of track points to generate
        start_time: Starting timestamp

    Returns:
        DataFrame with flight track data
    """
    if start_time is None:
        start_time = datetime.now()

    # Random flight parameters
    start_lat = random.uniform(30, 50)  # Latitude
    start_lon = random.uniform(-120, -70)  # Longitude
    start_alt = random.uniform(0, 1000)  # Starting altitude
    cruise_alt = random.uniform(30000, 40000)  # Cruise altitude

    data = []

    for i in range(n_points):
        # Create realistic flight profile (takeoff -> cruise -> landing)
        progress = i / n_points

        # Altitude profile
        if progress < 0.2:  # Takeoff
            altitude = start_alt + (cruise_alt - start_alt) * (progress / 0.2)
        elif progress > 0.8:  # Landing
            altitude = cruise_alt - (cruise_alt - start_alt) * ((progress - 0.8) / 0.2)
        else:  # Cruise
            altitude = cruise_alt + random.uniform(-500, 500)

        # Position (simple linear movement with noise)
        latitude = start_lat + progress * random.uniform(5, 15) + random.uniform(-0.1, 0.1)
        longitude = start_lon + progress * random.uniform(5, 15) + random.uniform(-0.1, 0.1)

        # Speed (realistic for different flight phases)
        if progress < 0.2 or progress > 0.8:
            speed = random.uniform(150, 250)  # Takeoff/landing
        else:
            speed = random.uniform(450, 550)  # Cruise

        # Heading (with slight variations)
        heading = 45 + random.uniform(-5, 5)

        # Timestamp
        timestamp = start_time + timedelta(seconds=i * 30)  # 30 second intervals

        data.append({
            'flight_id': flight_id,
            'type': aircraft_type,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'latitude': round(latitude, 6),
            'longitude': round(longitude, 6),
            'altitude': round(altitude, 1),
            'speed': round(speed, 1),
            'heading': round(heading, 1)
        })

    return pd.DataFrame(data)


def generate_daily_csv(
    date,
    n_flights_per_type,
    aircraft_types,
    points_per_flight=(30, 100),
    output_dir="."
):
    """
    Generate a CSV file for one day with multiple flights.

    Args:
        date: Date for this CSV file
        n_flights_per_type: Number of flights to generate per aircraft type
        aircraft_types: Dictionary of aircraft types and their frequency
        points_per_flight: Tuple of (min, max) track points per flight
        output_dir: Directory to save CSV file

    Returns:
        Path to generated CSV file
    """
    all_flights = []
    flight_counter = 1

    # Generate flights for each type
    for aircraft_type, count in aircraft_types.items():
        for _ in range(count):
            # Random number of track points
            n_points = random.randint(points_per_flight[0], points_per_flight[1])

            # Random start time during the day
            start_time = datetime.combine(date, datetime.min.time())
            start_time += timedelta(seconds=random.randint(0, 86400))

            # Generate flight ID
            flight_id = f"FL{date.strftime('%Y%m%d')}{flight_counter:04d}"
            flight_counter += 1

            # Generate flight track
            flight_track = generate_flight_track(
                flight_id, aircraft_type, n_points, start_time
            )
            all_flights.append(flight_track)

    # Combine all flights
    day_data = pd.concat(all_flights, ignore_index=True)

    # Shuffle rows to simulate unsorted data
    day_data = day_data.sample(frac=1).reset_index(drop=True)

    # Save to CSV
    filename = f"{output_dir}/flights_{date.strftime('%Y_%m_%d')}.csv"
    day_data.to_csv(filename, index=False)

    print(f"Generated: {filename}")
    print(f"  - Total rows: {len(day_data)}")
    print(f"  - Unique flights: {day_data['flight_id'].nunique()}")

    return filename


def generate_test_dataset(
    n_days=7,
    target_types_count=3,  # Flights of types we're interested in
    other_types_count=20,  # Flights of other types (noise)
    output_dir="test_data"
):
    """
    Generate a complete test dataset spanning multiple days.

    Args:
        n_days: Number of days to generate
        target_types_count: Flights per day for types we'll filter for
        other_types_count: Flights per day for other types (noise)
        output_dir: Directory to save CSV files
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 70)
    print("GENERATING TEST FLIGHT DATA")
    print("=" * 70)

    # Define aircraft types
    target_types = {
        "Type1": target_types_count,
        "Type2": target_types_count,
    }

    # Other types (noise in the data)
    other_types = {
        "Type3": other_types_count // 4,
        "Type4": other_types_count // 4,
        "Type5": other_types_count // 4,
        "Type6": other_types_count // 4,
    }

    # Combine all types
    all_types = {**target_types, **other_types}

    print(f"\nConfiguration:")
    print(f"  Days: {n_days}")
    print(f"  Target types: {list(target_types.keys())} ({target_types_count} flights/day each)")
    print(f"  Other types: {list(other_types.keys())} ({other_types_count} flights/day total)")
    print(f"  Output directory: {output_dir}")
    print()

    # Generate CSV for each day
    start_date = datetime(2024, 1, 1)
    generated_files = []

    for day in range(n_days):
        date = start_date + timedelta(days=day)
        filename = generate_daily_csv(
            date=date,
            n_flights_per_type=n_days,
            aircraft_types=all_types,
            points_per_flight=(30, 100),
            output_dir=output_dir
        )
        generated_files.append(filename)

    print("\n" + "=" * 70)
    print("TEST DATA GENERATION COMPLETE")
    print("=" * 70)
    print(f"\nGenerated {len(generated_files)} CSV files in '{output_dir}/' directory")
    print("\nYou can now test with:")
    print(f'  process_flight_csvs(csv_pattern="{output_dir}/flights_*.csv", ...)')

    return generated_files


def generate_small_test():
    """Generate a small test dataset for quick testing."""
    print("Generating small test dataset (3 days, fewer flights)...\n")
    return generate_test_dataset(
        n_days=3,
        target_types_count=5,  # 5 flights per target type per day
        other_types_count=10,  # 10 other flights per day
        output_dir="test_data_small"
    )


def generate_large_test():
    """Generate a large test dataset to simulate real conditions."""
    print("Generating large test dataset (30 days, many flights)...\n")
    return generate_test_dataset(
        n_days=30,
        target_types_count=100,  # 100 flights per target type per day
        other_types_count=500,   # 500 other flights per day
        output_dir="test_data_large"
    )


if __name__ == "__main__":
    # Choose which test dataset to generate:

    # Small dataset for quick testing
    generate_small_test()

    # Large dataset for realistic testing
    # generate_large_test()

    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print("\n1. Run the processor:")
    print("   python process_flight_data.py")
    print("\n2. Or run examples:")
    print("   python example_usage.py")
    print("\n3. Adjust the CSV_PATTERN in those files to match your test data directory")
