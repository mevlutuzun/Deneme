import pandas as pd
import glob
from pathlib import Path
from typing import List, Dict, Set
from collections import defaultdict


def find_target_flight_ids(
    csv_files: List[str],
    target_types: List[str],
    flights_per_type: int = 3,
    chunk_size: int = 100000
) -> Dict[str, Set[str]]:
    """
    First pass: Scan through CSV files to identify flight_ids for target types.

    Args:
        csv_files: List of CSV file paths
        target_types: List of aircraft types to filter (e.g., ["Type1", "Type2"])
        flights_per_type: Number of flights to collect per type
        chunk_size: Number of rows to read per chunk

    Returns:
        Dictionary mapping type to set of flight_ids
    """
    # Track flight_ids found for each type
    found_flights = defaultdict(set)

    # Track which types still need more flights
    types_needed = set(target_types)

    print("Pass 1: Scanning for flight IDs...")

    for csv_file in csv_files:
        if not types_needed:
            break  # Found enough flights for all types

        print(f"  Scanning: {Path(csv_file).name}")

        try:
            # Read CSV in chunks
            for chunk_num, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
                # Filter for rows with target types that still need flights
                mask = chunk['type'].isin(types_needed)
                filtered_chunk = chunk[mask]

                if filtered_chunk.empty:
                    continue

                # Group by type and collect unique flight_ids
                for aircraft_type in types_needed.copy():
                    type_rows = filtered_chunk[filtered_chunk['type'] == aircraft_type]

                    if not type_rows.empty:
                        # Get unique flight_ids from this chunk
                        new_flight_ids = set(type_rows['flight_id'].unique())
                        found_flights[aircraft_type].update(new_flight_ids)

                        # Check if we have enough flights for this type
                        if len(found_flights[aircraft_type]) >= flights_per_type:
                            # Limit to exact number needed
                            found_flights[aircraft_type] = set(
                                list(found_flights[aircraft_type])[:flights_per_type]
                            )
                            types_needed.discard(aircraft_type)
                            print(f"    Found {flights_per_type} flights for {aircraft_type}")

        except Exception as e:
            print(f"  Error reading {csv_file}: {e}")
            continue

    # Print summary
    print("\nFlights found:")
    for aircraft_type in target_types:
        count = len(found_flights.get(aircraft_type, set()))
        print(f"  {aircraft_type}: {count} flights")

    return dict(found_flights)


def collect_flight_data(
    csv_files: List[str],
    target_flight_ids: Dict[str, Set[str]],
    chunk_size: int = 100000
) -> pd.DataFrame:
    """
    Second pass: Collect all rows for the target flight_ids.

    Args:
        csv_files: List of CSV file paths
        target_flight_ids: Dictionary mapping type to set of flight_ids
        chunk_size: Number of rows to read per chunk

    Returns:
        DataFrame containing all rows for target flights
    """
    # Flatten all target flight_ids
    all_target_ids = set()
    for flight_ids in target_flight_ids.values():
        all_target_ids.update(flight_ids)

    if not all_target_ids:
        print("No flight IDs to collect!")
        return pd.DataFrame()

    print(f"\nPass 2: Collecting data for {len(all_target_ids)} flights...")

    collected_data = []

    for csv_file in csv_files:
        print(f"  Processing: {Path(csv_file).name}")

        try:
            # Read CSV in chunks
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
                # Filter for rows with target flight_ids
                mask = chunk['flight_id'].isin(all_target_ids)
                filtered_chunk = chunk[mask]

                if not filtered_chunk.empty:
                    collected_data.append(filtered_chunk)
                    print(f"    Collected {len(filtered_chunk)} rows")

        except Exception as e:
            print(f"  Error reading {csv_file}: {e}")
            continue

    if not collected_data:
        print("No data collected!")
        return pd.DataFrame()

    # Combine all collected data
    result_df = pd.concat(collected_data, ignore_index=True)

    # Sort by type and flight_id for better organization
    result_df = result_df.sort_values(['type', 'flight_id']).reset_index(drop=True)

    print(f"\nTotal rows collected: {len(result_df)}")
    print(f"Unique flights: {result_df['flight_id'].nunique()}")

    return result_df


def process_flight_csvs(
    csv_pattern: str,
    target_types: List[str],
    flights_per_type: int = 3,
    chunk_size: int = 100000,
    output_file: str = "filtered_flights.csv"
) -> pd.DataFrame:
    """
    Main function to process flight CSV files.

    Args:
        csv_pattern: Glob pattern for CSV files (e.g., "flight_data_*.csv")
        target_types: List of aircraft types to filter
        flights_per_type: Number of flights to collect per type
        chunk_size: Number of rows to read per chunk
        output_file: Path to save the filtered data

    Returns:
        DataFrame containing filtered flight data
    """
    # Find all CSV files matching the pattern
    csv_files = sorted(glob.glob(csv_pattern))

    if not csv_files:
        print(f"No CSV files found matching pattern: {csv_pattern}")
        return pd.DataFrame()

    print(f"Found {len(csv_files)} CSV files")
    print(f"Target types: {target_types}")
    print(f"Flights per type: {flights_per_type}")
    print(f"Chunk size: {chunk_size:,} rows\n")

    # Pass 1: Find target flight_ids
    target_flight_ids = find_target_flight_ids(
        csv_files, target_types, flights_per_type, chunk_size
    )

    # Pass 2: Collect all data for target flights
    result_df = collect_flight_data(csv_files, target_flight_ids, chunk_size)

    # Save to output file
    if not result_df.empty and output_file:
        result_df.to_csv(output_file, index=False)
        print(f"\nData saved to: {output_file}")

        # Print summary statistics
        print("\nSummary by type:")
        for aircraft_type in target_types:
            type_data = result_df[result_df['type'] == aircraft_type]
            n_flights = type_data['flight_id'].nunique()
            n_rows = len(type_data)
            print(f"  {aircraft_type}: {n_flights} flights, {n_rows} track points")

    return result_df


if __name__ == "__main__":
    # Example usage

    # Configure your settings here
    CSV_PATTERN = "flight_data_*.csv"  # Pattern to match your CSV files
    TARGET_TYPES = ["Type1", "Type2"]  # Aircraft types to filter
    FLIGHTS_PER_TYPE = 3               # Number of flights per type
    CHUNK_SIZE = 100000                # Rows per chunk (adjust based on memory)
    OUTPUT_FILE = "filtered_flights.csv"

    # Process the data
    result = process_flight_csvs(
        csv_pattern=CSV_PATTERN,
        target_types=TARGET_TYPES,
        flights_per_type=FLIGHTS_PER_TYPE,
        chunk_size=CHUNK_SIZE,
        output_file=OUTPUT_FILE
    )

    # Display sample of results
    if not result.empty:
        print("\n" + "="*60)
        print("Sample of collected data:")
        print("="*60)
        print(result.head(10))

        print("\n" + "="*60)
        print("Flight IDs collected:")
        print("="*60)
        for aircraft_type in TARGET_TYPES:
            type_flights = result[result['type'] == aircraft_type]['flight_id'].unique()
            print(f"\n{aircraft_type}:")
            for flight_id in type_flights:
                n_points = len(result[result['flight_id'] == flight_id])
                print(f"  - {flight_id}: {n_points} track points")
