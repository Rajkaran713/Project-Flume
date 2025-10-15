#!/usr/bin/env python3
"""
PROJECT FLUME - Bronze to Silver Transformation
Cleans raw JSON and converts to Parquet with derived metrics
"""
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import boto3
import io

# Configuration
S3_BUCKET = "project-nimbus-raw-data-lake-12345-raj"
BRONZE_PREFIX = {
    "swob": "swob_raw",
    "climate": "climate_hourly_raw",
    "hydro": "hydrometric_raw"
}
SILVER_PREFIX = {
    "swob": "swob_silver",
    "climate": "climate_hourly_silver",
    "hydro": "hydrometric_silver"
}

s3_client = boto3.client('s3')

# ============================================================
# DERIVED METRICS CALCULATIONS
# ============================================================

def calculate_feels_like(temp_c, humidity, wind_speed_kmh):
    """
    Calculate feels-like temperature using wind chill or heat index
    """
    if temp_c is None or pd.isna(temp_c):
        return None
    
    # If cold (<=10°C) and windy, use wind chill
    if temp_c <= 10 and wind_speed_kmh and wind_speed_kmh > 4.8:
        return calculate_wind_chill(temp_c, wind_speed_kmh)
    
    # If hot (>=27°C) and humid, use heat index
    if temp_c >= 27 and humidity:
        return calculate_heat_index(temp_c, humidity)
    
    # Otherwise feels-like = actual temp
    return temp_c

def calculate_wind_chill(temp_c, wind_speed_kmh):
    """
    Wind chill index (Environment Canada formula)
    Valid for temps <= 10°C and wind > 4.8 km/h
    """
    if temp_c is None or wind_speed_kmh is None:
        return None
    if temp_c > 10 or wind_speed_kmh <= 4.8:
        return temp_c
    
    wc = 13.12 + 0.6215 * temp_c - 11.37 * (wind_speed_kmh ** 0.16) + \
         0.3965 * temp_c * (wind_speed_kmh ** 0.16)
    return round(wc, 1)

def calculate_heat_index(temp_c, humidity):
    """
    Heat index (US formula adapted for Celsius)
    Valid for temps >= 27°C
    """
    if temp_c is None or humidity is None:
        return None
    if temp_c < 27:
        return temp_c
    
    # Convert to Fahrenheit for formula
    temp_f = temp_c * 9/5 + 32
    
    hi = -42.379 + 2.04901523 * temp_f + 10.14333127 * humidity \
         - 0.22475541 * temp_f * humidity - 0.00683783 * temp_f**2 \
         - 0.05481717 * humidity**2 + 0.00122874 * temp_f**2 * humidity \
         + 0.00085282 * temp_f * humidity**2 - 0.00000199 * temp_f**2 * humidity**2
    
    # Convert back to Celsius
    hi_c = (hi - 32) * 5/9
    return round(hi_c, 1)

# ============================================================
# CLIMATE-HOURLY TRANSFORMATION
# ============================================================

def transform_climate_hourly(features):
    """Transform Climate-Hourly raw JSON to clean DataFrame"""
    records = []
    
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [None, None])
        
        # Extract core fields
        record = {
            'station_name': props.get('STATION_NAME'),
            'climate_identifier': props.get('CLIMATE_IDENTIFIER'),
            'province_code': props.get('PROVINCE_CODE'),
            'utc_timestamp': props.get('UTC_DATE'),
            'local_timestamp': props.get('LOCAL_DATE'),
            'longitude': coords[0],
            'latitude': coords[1],
            'temperature': props.get('TEMP'),
            'dew_point_temp': props.get('DEW_POINT_TEMP'),
            'relative_humidity': props.get('RELATIVE_HUMIDITY'),
            'station_pressure': props.get('STATION_PRESSURE'),
            'wind_speed': props.get('WIND_SPEED'),
            'wind_direction': props.get('WIND_DIRECTION'),
            'precip_amount': props.get('PRECIP_AMOUNT'),
            'visibility': props.get('VISIBILITY'),
            'humidex': props.get('HUMIDEX'),
            'windchill': props.get('WINDCHILL'),
        }
        
        # Calculate feels-like temperature
        record['feels_like_temp'] = calculate_feels_like(
            record['temperature'],
            record['relative_humidity'],
            record['wind_speed']
        )
        
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Convert timestamps
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], utc=True)
    df['local_timestamp'] = pd.to_datetime(df['local_timestamp'])
    
    # Add partition columns
    df['year'] = df['utc_timestamp'].dt.year
    df['month'] = df['utc_timestamp'].dt.month
    df['day'] = df['utc_timestamp'].dt.day
    
    return df

# ============================================================
# SWOB TRANSFORMATION
# ============================================================

def transform_swob(features):
    """Transform SWOB raw JSON to clean DataFrame"""
    records = []
    
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [None, None, None])
        
        # Extract core fields (drop aggregates like pst1hr, pst10mts)
        record = {
            'station_name': props.get('stn_nam-value'),
            'station_id': props.get('tc_id-value'),
            'msc_id': props.get('msc_id-value'),
            'climate_id': props.get('clim_id-value'),
            'utc_timestamp': props.get('date_tm-value'),
            'longitude': coords[0],
            'latitude': coords[1],
            'elevation': coords[2] if len(coords) > 2 else None,
            'air_temp': props.get('air_temp'),
            'air_temp_qa': props.get('air_temp-qa'),
            'rel_hum': props.get('rel_hum'),
            'rel_hum_qa': props.get('rel_hum-qa'),
            'stn_pres': props.get('stn_pres'),
            'stn_pres_qa': props.get('stn_pres-qa'),
            'wind_speed': props.get('avg_wnd_spd_10m_pst1mt'),
            'wind_speed_qa': props.get('avg_wnd_spd_10m_pst1mt-qa'),
            'wind_direction': props.get('avg_wnd_dir_10m_pst1mt'),
            'dew_point_temp': props.get('dwpt_temp'),
            'precip_amount_1hr': props.get('pcpn_amt_pst1hr'),
            'snow_depth': props.get('snw_dpth'),
        }
        
        # Calculate derived metrics
        record['feels_like_temp'] = calculate_feels_like(
            record['air_temp'],
            record['rel_hum'],
            record['wind_speed']
        )
        
        record['wind_chill'] = calculate_wind_chill(
            record['air_temp'],
            record['wind_speed']
        )
        
        record['heat_index'] = calculate_heat_index(
            record['air_temp'],
            record['rel_hum']
        )
        
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Convert timestamps
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], utc=True)
    
    # Add partition columns
    df['year'] = df['utc_timestamp'].dt.year
    df['month'] = df['utc_timestamp'].dt.month
    df['day'] = df['utc_timestamp'].dt.day
    
    return df

# ============================================================
# HYDROMETRIC TRANSFORMATION
# ============================================================

def transform_hydrometric(features):
    """Transform Hydrometric raw JSON to clean DataFrame"""
    records = []
    
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [None, None])
        
        record = {
            'station_number': props.get('STATION_NUMBER'),
            'station_name': props.get('STATION_NAME'),
            'province': props.get('PROV_TERR_STATE_LOC'),
            'utc_timestamp': props.get('DATETIME'),
            'local_timestamp': props.get('DATETIME_LST'),
            'longitude': coords[0],
            'latitude': coords[1],
            'water_level': props.get('LEVEL'),
            'discharge': props.get('DISCHARGE'),
            'level_quality_symbol': props.get('LEVEL_SYMBOL_EN'),
            'discharge_quality_symbol': props.get('DISCHARGE_SYMBOL_EN'),
        }
        
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Convert timestamps
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], utc=True)
    df['local_timestamp'] = pd.to_datetime(df['local_timestamp'])
    
    # Add partition columns
    df['year'] = df['utc_timestamp'].dt.year
    df['month'] = df['utc_timestamp'].dt.month
    df['day'] = df['utc_timestamp'].dt.day
    
    return df

# ============================================================
# S3 I/O FUNCTIONS
# ============================================================

def read_json_from_s3(bucket, key):
    """Read JSON file from S3"""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    data = json.loads(obj['Body'].read())
    return data['features']

def write_parquet_to_s3(df, bucket, key):
    """Write DataFrame to S3 as Parquet"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy', engine='pyarrow')
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"✓ Wrote {len(df)} records to s3://{bucket}/{key}")

def list_files_in_s3(bucket, prefix, extension='.json'):
    """List all files with given extension in S3 prefix"""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith(extension):
                files.append(key)
    
    return files

# ============================================================
# MAIN PROCESSING FUNCTION
# ============================================================

def process_dataset(dataset_name, transform_func):
    """Process all files for a given dataset"""
    print(f"\n{'='*60}")
    print(f"Processing: {dataset_name.upper()}")
    print(f"{'='*60}")
    
    bronze_prefix = BRONZE_PREFIX[dataset_name]
    silver_prefix = SILVER_PREFIX[dataset_name]
    
    # List all raw JSON files
    files = list_files_in_s3(S3_BUCKET, bronze_prefix)
    print(f"Found {len(files)} files to process")
    
    if not files:
        print(f"⚠️  No files found in s3://{S3_BUCKET}/{bronze_prefix}")
        return
    
    # Process each file
    for i, file_key in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] Processing: {file_key}")
        
        try:
            # Read raw JSON
            features = read_json_from_s3(S3_BUCKET, file_key)
            print(f"  Loaded {len(features)} features")
            
            # Transform to DataFrame
            df = transform_func(features)
            print(f"  Transformed to {len(df)} records with {len(df.columns)} columns")
            
            # Generate output path
            # Example: swob_raw/year=2025/month=10/day=04/swob_delta_20251004.json
            # Becomes: swob_silver/year=2025/month=10/day=04/swob_20251004.parquet
            parts = file_key.split('/')
            filename = parts[-1].replace('.json', '.parquet').replace('_delta', '')
            output_key = f"{silver_prefix}/{'/'.join(parts[-4:-1])}/{filename}"
            
            # Write Parquet to S3
            write_parquet_to_s3(df, S3_BUCKET, output_key)
            
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            continue
    
    print(f"\n✓ Completed {dataset_name.upper()} transformation")

# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main():
    print("="*60)
    print("PROJECT FLUME - Bronze → Silver Transformation")
    print("="*60)
    
    # Process each dataset
    process_dataset("climate", transform_climate_hourly)
    process_dataset("swob", transform_swob)
    process_dataset("hydro", transform_hydrometric)
    
    print("\n" + "="*60)
    print("✓ ALL TRANSFORMATIONS COMPLETE")
    print("="*60)
    print(f"\nSilver layer data available in:")
    print(f"  - s3://{S3_BUCKET}/{SILVER_PREFIX['climate']}/")
    print(f"  - s3://{S3_BUCKET}/{SILVER_PREFIX['swob']}/")
    print(f"  - s3://{S3_BUCKET}/{SILVER_PREFIX['hydro']}/")

if __name__ == "__main__":
    main()
