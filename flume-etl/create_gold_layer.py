#!/usr/bin/env python3
"""
PROJECT FLUME - Create Gold Layer (Enhanced with Hydrometric Data)
Aggregated tables optimized for dashboard queries
"""
import pandas as pd
import boto3
import io
from datetime import datetime

S3_BUCKET = "project-nimbus-raw-data-lake-12345-raj"
s3_client = boto3.client('s3')

def read_all_parquet(prefix):
    """Read all Parquet files from S3 prefix into single DataFrame"""
    print(f"Reading files from {prefix}...")
    
    # List all parquet files
    paginator = s3_client.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        if 'Contents' in page:
            files.extend([obj['Key'] for obj in page['Contents'] if obj['Key'].endswith('.parquet')])
    
    print(f"  Found {len(files)} Parquet files")
    
    if len(files) == 0:
        print(f"  ⚠️  No files found in {prefix}")
        return pd.DataFrame()
    
    # Read all files into DataFrames
    dfs = []
    for file_key in files:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        dfs.append(df)
    
    # Combine all DataFrames
    combined = pd.concat(dfs, ignore_index=True)
    print(f"  Total records: {len(combined):,}")
    
    return combined

def write_parquet_to_s3(df, key):
    """Write DataFrame to S3 as Parquet"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"✓ Wrote {len(df):,} records to s3://{S3_BUCKET}/{key}")

# ============================================================
# WEATHER GOLD TABLES
# ============================================================

def create_station_latest():
    """Create table with latest weather reading per station"""
    print("\n" + "="*60)
    print("Creating: weather_station_latest")
    print("="*60)
    
    # Read SWOB silver data
    swob_df = read_all_parquet("swob_silver")
    
    if swob_df.empty:
        print("  ⚠️  No SWOB data available")
        return pd.DataFrame()
    
    # Get latest record per station
    latest = swob_df.sort_values('utc_timestamp').groupby('msc_id').tail(1)
    
    # Select columns for dashboard
    dashboard_cols = [
        'station_name', 'msc_id', 'latitude', 'longitude', 'elevation',
        'utc_timestamp', 'air_temp', 'feels_like_temp', 'rel_hum',
        'wind_speed', 'wind_direction', 'stn_pres', 'precip_amount_1hr',
        'snow_depth'
    ]
    
    latest = latest[dashboard_cols].copy()
    
    # Add data freshness indicator (minutes since last update)
    now = pd.Timestamp.now(tz='UTC')
    latest['minutes_since_update'] = (now - latest['utc_timestamp']).dt.total_seconds() / 60
    
    print(f"\nLatest weather conditions for {len(latest):,} stations")
    print(f"Timestamp range: {latest['utc_timestamp'].min()} to {latest['utc_timestamp'].max()}")
    
    # Write to Gold layer
    write_parquet_to_s3(latest, "gold/weather_station_latest/weather_station_latest.parquet")
    
    return latest

def create_hourly_summary():
    """Create hourly aggregated time series per station"""
    print("\n" + "="*60)
    print("Creating: weather_hourly_summary")
    print("="*60)
    
    # Read SWOB silver data
    swob_df = read_all_parquet("swob_silver")
    
    if swob_df.empty:
        print("  ⚠️  No SWOB data available")
        return pd.DataFrame()
    
    # Create hour column
    swob_df['hour'] = swob_df['utc_timestamp'].dt.floor('h')
    
    # Aggregate by station + hour
    hourly = swob_df.groupby(['msc_id', 'station_name', 'hour']).agg({
        'air_temp': ['mean', 'min', 'max'],
        'feels_like_temp': 'mean',
        'rel_hum': 'mean',
        'wind_speed': 'mean',
        'wind_direction': 'mean',
        'stn_pres': 'mean',
        'precip_amount_1hr': 'sum',
        'latitude': 'first',
        'longitude': 'first'
    }).reset_index()
    
    # Flatten column names
    hourly.columns = [
        'msc_id', 'station_name', 'hour',
        'temp_mean', 'temp_min', 'temp_max',
        'feels_like_mean', 'humidity_mean', 'wind_speed_mean',
        'wind_dir_mean', 'pressure_mean', 'precip_total',
        'latitude', 'longitude'
    ]
    
    print(f"\nHourly weather summaries: {len(hourly):,} records")
    print(f"Stations: {hourly['msc_id'].nunique():,}")
    print(f"Time range: {hourly['hour'].min()} to {hourly['hour'].max()}")
    
    # Write to Gold layer
    write_parquet_to_s3(hourly, "gold/weather_hourly_summary/weather_hourly_summary.parquet")
    
    return hourly

def create_station_metadata():
    """Create weather station metadata table"""
    print("\n" + "="*60)
    print("Creating: weather_station_metadata")
    print("="*60)
    
    # Read SWOB silver data
    swob_df = read_all_parquet("swob_silver")
    
    if swob_df.empty:
        print("  ⚠️  No SWOB data available")
        return pd.DataFrame()
    
    # Get unique stations with metadata
    metadata = swob_df.groupby('msc_id').agg({
        'station_name': 'first',
        'station_id': 'first',
        'climate_id': 'first',
        'latitude': 'first',
        'longitude': 'first',
        'elevation': 'first',
        'utc_timestamp': ['min', 'max', 'count']
    }).reset_index()
    
    # Flatten columns
    metadata.columns = [
        'msc_id', 'station_name', 'station_id', 'climate_id',
        'latitude', 'longitude', 'elevation',
        'first_observation', 'last_observation', 'total_observations'
    ]
    
    print(f"\nWeather station metadata: {len(metadata):,} stations")
    
    # Write to Gold layer
    write_parquet_to_s3(metadata, "gold/weather_station_metadata/weather_station_metadata.parquet")
    
    return metadata

# ============================================================
# HYDROMETRIC GOLD TABLES (NEW!)
# ============================================================

def create_hydro_station_latest():
    """Create table with latest water level reading per station"""
    print("\n" + "="*60)
    print("Creating: hydro_station_latest")
    print("="*60)
    
    # Read hydrometric silver data
    hydro_df = read_all_parquet("hydrometric_silver")
    
    if hydro_df.empty:
        print("  ⚠️  No hydrometric data available")
        return pd.DataFrame()
    
    # Get latest record per station
    latest = hydro_df.sort_values('utc_timestamp').groupby('station_number').tail(1)
    
    # Select columns for dashboard
    dashboard_cols = [
        'station_number', 'station_name', 'province',
        'latitude', 'longitude', 'utc_timestamp',
        'water_level', 'discharge'
    ]
    
    latest = latest[dashboard_cols].copy()
    
    # Add data freshness indicator
    now = pd.Timestamp.now(tz='UTC')
    latest['minutes_since_update'] = (now - latest['utc_timestamp']).dt.total_seconds() / 60
    
    print(f"\nLatest water levels for {len(latest):,} stations")
    print(f"Timestamp range: {latest['utc_timestamp'].min()} to {latest['utc_timestamp'].max()}")
    
    # Write to Gold layer
    write_parquet_to_s3(latest, "gold/hydro_station_latest/hydro_station_latest.parquet")
    
    return latest

def create_hydro_hourly_summary():
    """Create hourly aggregated water level time series"""
    print("\n" + "="*60)
    print("Creating: hydro_hourly_summary")
    print("="*60)
    
    # Read hydrometric silver data
    hydro_df = read_all_parquet("hydrometric_silver")
    
    if hydro_df.empty:
        print("  ⚠️  No hydrometric data available")
        return pd.DataFrame()
    
    # Create hour column
    hydro_df['hour'] = hydro_df['utc_timestamp'].dt.floor('h')
    
    # Aggregate by station + hour
    hourly = hydro_df.groupby(['station_number', 'station_name', 'hour']).agg({
        'water_level': ['mean', 'min', 'max'],
        'discharge': ['mean', 'min', 'max'],
        'latitude': 'first',
        'longitude': 'first',
        'province': 'first'
    }).reset_index()
    
    # Flatten column names
    hourly.columns = [
        'station_number', 'station_name', 'hour',
        'water_level_mean', 'water_level_min', 'water_level_max',
        'discharge_mean', 'discharge_min', 'discharge_max',
        'latitude', 'longitude', 'province'
    ]
    
    print(f"\nHourly water level summaries: {len(hourly):,} records")
    print(f"Stations: {hourly['station_number'].nunique():,}")
    print(f"Time range: {hourly['hour'].min()} to {hourly['hour'].max()}")
    
    # Write to Gold layer
    write_parquet_to_s3(hourly, "gold/hydro_hourly_summary/hydro_hourly_summary.parquet")
    
    return hourly

def create_hydro_metadata():
    """Create hydrometric station metadata table"""
    print("\n" + "="*60)
    print("Creating: hydro_station_metadata")
    print("="*60)
    
    # Read hydrometric silver data
    hydro_df = read_all_parquet("hydrometric_silver")
    
    if hydro_df.empty:
        print("  ⚠️  No hydrometric data available")
        return pd.DataFrame()
    
    # Get unique stations with metadata
    metadata = hydro_df.groupby('station_number').agg({
        'station_name': 'first',
        'province': 'first',
        'latitude': 'first',
        'longitude': 'first',
        'utc_timestamp': ['min', 'max', 'count']
    }).reset_index()
    
    # Flatten columns
    metadata.columns = [
        'station_number', 'station_name', 'province',
        'latitude', 'longitude',
        'first_observation', 'last_observation', 'total_observations'
    ]
    
    print(f"\nHydrometric station metadata: {len(metadata):,} stations")
    
    # Write to Gold layer
    write_parquet_to_s3(metadata, "gold/hydro_station_metadata/hydro_station_metadata.parquet")
    
    return metadata

# ============================================================
# MAIN
# ============================================================

def main():
    print("="*60)
    print("PROJECT FLUME - Gold Layer Creation (Weather + Water)")
    print("="*60)
    
    # Create weather gold tables
    print("\n🌦️  WEATHER DATA")
    weather_latest = create_station_latest()
    weather_hourly = create_hourly_summary()
    weather_metadata = create_station_metadata()
    
    # Create hydrometric gold tables
    print("\n💧 HYDROMETRIC DATA")
    hydro_latest = create_hydro_station_latest()
    hydro_hourly = create_hydro_hourly_summary()
    hydro_metadata = create_hydro_metadata()
    
    print("\n" + "="*60)
    print("✓ GOLD LAYER COMPLETE")
    print("="*60)
    print("\n🌦️  Weather Tables:")
    if not weather_latest.empty:
        print(f"  1. weather_station_latest  - {len(weather_latest):,} stations")
        print(f"  2. weather_hourly_summary  - {len(weather_hourly):,} records")
        print(f"  3. weather_station_metadata - {len(weather_metadata):,} stations")
    else:
        print("  ⚠️  No weather data available")
    
    print("\n💧 Hydrometric Tables:")
    if not hydro_latest.empty:
        print(f"  4. hydro_station_latest    - {len(hydro_latest):,} stations")
        print(f"  5. hydro_hourly_summary    - {len(hydro_hourly):,} records")
        print(f"  6. hydro_station_metadata  - {len(hydro_metadata):,} stations")
    else:
        print("  ⚠️  No hydrometric data available")
    
    print(f"\nLocation: s3://{S3_BUCKET}/gold/")

if __name__ == "__main__":
    main()