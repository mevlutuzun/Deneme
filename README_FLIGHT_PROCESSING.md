# Flight CSV Data Processor

A memory-efficient Python solution for extracting specific flights from large CSV datasets.

## Problem

You have:
- Multiple CSV files (one per day, spanning a month)
- Millions of flight records
- Each row represents an aircraft state (track point)
- Multiple rows per flight (identified by `flight_id`)
- Data is NOT sorted by `flight_id`
- Cannot load all data into memory at once

You want:
- Filter by specific aircraft types
- Extract complete flight tracks (all points for selected flights)
- Get N flights per aircraft type (e.g., 3 flights each)

## Solution Approach

The script uses a **two-pass strategy** to handle large datasets efficiently:

### Pass 1: Identify Target Flight IDs
- Reads CSV files in chunks
- Scans for flight_ids matching target aircraft types
- Collects N flight_ids per type
- Stops early once enough flight_ids are found

### Pass 2: Collect Complete Flight Data
- Reads CSV files in chunks again
- Extracts ALL rows for the identified flight_ids
- Ensures complete flight tracks are captured
- Combines and sorts the results

## Files

- `process_flight_data.py` - Main processing module
- `example_usage.py` - Example usage patterns
- `README_FLIGHT_PROCESSING.md` - This file

## Usage

### Basic Example

```python
from process_flight_data import process_flight_csvs

result = process_flight_csvs(
    csv_pattern="flights_*.csv",           # Your CSV files
    target_types=["Type1", "Type2"],       # Aircraft types to extract
    flights_per_type=3,                    # 3 flights per type
    chunk_size=100000,                     # Adjust based on available RAM
    output_file="filtered_flights.csv"     # Output file
)
```

### Required CSV Structure

Your CSV files should have at least these columns:
- `flight_id` - Unique identifier for each flight
- `type` - Aircraft type
- Other columns (altitude, timestamp, latitude, longitude, etc.)

### Parameters

- **csv_pattern**: Glob pattern to match your CSV files
  - Example: `"flights_*.csv"` matches all files starting with "flights_"
  - Example: `"data/2024_01_*.csv"` matches specific month

- **target_types**: List of aircraft types to filter
  - Example: `["Boeing 737", "Airbus A320"]`

- **flights_per_type**: Number of complete flights to extract per type
  - Default: 3

- **chunk_size**: Number of rows to read at once
  - Larger = faster but uses more memory
  - Smaller = slower but more memory efficient
  - Default: 100,000 rows
  - Adjust based on your RAM: 8GB RAM → 50k-100k, 16GB RAM → 100k-200k

- **output_file**: Where to save the filtered data
  - Default: `"filtered_flights.csv"`

## Performance Tips

### Memory Optimization
- Reduce `chunk_size` if you encounter memory errors
- Process fewer days at a time
- Use specific date patterns instead of wildcards

### Speed Optimization
- Increase `chunk_size` if you have available RAM
- Use SSD storage for faster I/O
- Process files in parallel (advanced users)

### Example: Memory-Constrained Environment
```python
result = process_flight_csvs(
    csv_pattern="flights_2024_01_01.csv",  # Single day
    target_types=["Type1"],                 # One type
    flights_per_type=3,
    chunk_size=50000,                       # Smaller chunks
    output_file="output.csv"
)
```

### Example: High-Performance Environment
```python
result = process_flight_csvs(
    csv_pattern="flights_*.csv",            # All files
    target_types=["Type1", "Type2", "Type3"],
    flights_per_type=10,
    chunk_size=500000,                      # Larger chunks
    output_file="output.csv"
)
```

## Output

The script produces:
1. **Console output** showing progress and statistics
2. **CSV file** with filtered flight data, sorted by type and flight_id
3. **DataFrame** returned for further analysis in Python

### Output Statistics

The script displays:
- Number of CSV files processed
- Flight IDs found per type
- Total rows collected
- Track points per flight
- Summary by aircraft type

## Advanced Usage

### Post-Processing Analysis

```python
result = process_flight_csvs(...)

# Analyze track lengths
for flight_id in result['flight_id'].unique():
    flight_data = result[result['flight_id'] == flight_id]
    print(f"{flight_id}: {len(flight_data)} points")

# Filter by additional criteria
high_altitude = result[result['altitude'] > 30000]

# Export by type
for aircraft_type in result['type'].unique():
    type_data = result[result['type'] == aircraft_type]
    type_data.to_csv(f"{aircraft_type}_flights.csv", index=False)
```

### Processing Specific Date Range

```python
import glob
from datetime import datetime, timedelta

# Get files for specific week
start_date = datetime(2024, 1, 1)
date_list = [start_date + timedelta(days=x) for x in range(7)]
csv_files = [f"flights_{d.strftime('%Y_%m_%d')}.csv" for d in date_list]

# Process only those files
# (Modify the function to accept list of files instead of pattern)
```

## Troubleshooting

### "No CSV files found"
- Check the csv_pattern matches your file names
- Verify you're in the correct directory
- Use absolute paths if needed

### "MemoryError"
- Reduce chunk_size
- Process fewer files at once
- Close other applications

### "No data collected"
- Verify column names match (case-sensitive)
- Check that target_types exist in your data
- Ensure flight_id column has valid values

### Slow Performance
- Increase chunk_size if you have RAM available
- Process fewer files per run
- Use SSD storage
- Check for disk I/O bottlenecks

## Example Scenarios

### Scenario 1: Research Analysis
Extract small sample for analysis:
```python
process_flight_csvs(
    csv_pattern="flights_*.csv",
    target_types=["Commercial", "Cargo", "Private"],
    flights_per_type=5,
    output_file="sample_flights.csv"
)
```

### Scenario 2: Type Comparison
Compare different aircraft models:
```python
process_flight_csvs(
    csv_pattern="flights_*.csv",
    target_types=["Boeing 737", "Airbus A320", "Boeing 787"],
    flights_per_type=10,
    output_file="aircraft_comparison.csv"
)
```

### Scenario 3: Quality Check
Extract flights for validation:
```python
process_flight_csvs(
    csv_pattern="flights_2024_01_*.csv",
    target_types=["Type1"],
    flights_per_type=20,
    output_file="validation_sample.csv"
)
```

## Requirements

```
pandas>=1.3.0
```

Install with:
```bash
pip install pandas
```

## License

This code is provided as-is for your use.
