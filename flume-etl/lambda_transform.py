import json
import pandas as pd
import numpy as np
from datetime import datetime
import boto3
import io

# S3 client
s3_client = boto3.client('s3')

# ============================================================
# DERIVED METRICS (Same as before)
# ============================================================

def calculate_feels_like(temp_c, humidity, wind_speed_kmh):
    if temp_c is None or pd.isna(temp_c):
        return None
    
    if temp_c <= 10 and wind_speed_kmh and wind_speed_kmh > 4.8:
        return calculate_wind_chill(temp_c, wind_speed_kmh)
    
    if temp_c >= 27 and humidity:
        return calculate_heat_index(temp_c, humidity)
    
    return temp_c

def calculate_wind_chill(temp_c, wind_speed_kmh):
    if temp_c is None or wind_speed_kmh is None:
        return None
    if temp_c > 10 or wind_speed_kmh <= 4.8:
        return temp_c
    
    wc = 13.12 + 0.6215 * temp_c - 11.37 * (wind_speed_kmh ** 0.16) + \
         0.3965 * temp_c * (wind_speed_kmh ** 0.16)
    return round(wc, 1)

def calculate_heat_index(temp_c, humidity):
    if temp_c is None or humidity is None:
        return None
    if temp_c < 27:
        return temp_c
    
    temp_f = temp_c * 9/5 + 32
    
    hi = -42.379 + 2.04901523 * temp_f + 10.14333127 * humidity \
         - 0.22475541 * temp_f * humidity - 0.00683783 * temp_f**2 \
         - 0.05481717 * humidity**2 + 0.00122874 * temp_f**2 * humidity \
         + 0.00085282 * temp_f * humidity**2 - 0.00000199 * temp_f**2 * humidity**2
    
    hi_c = (hi - 32) * 5/9
    return round(hi_c, 1)

# ============================================================
# TRANSFORMATION FUNCTIONS (Same as before)
# ============================================================

def transform_climate_hourly(features):
    records = []
    
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [None, None])
        
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
        
        record['feels_like_temp'] = calculate_feels_like(
            record['temperature'],
            record['relative_humidity'],
            record['wind_speed']
        )
        
        records.append(record)
    
    df = pd.DataFrame(records)
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], utc=True)
    df['local_timestamp'] = pd.to_datetime(df['local_timestamp'])
    df['year'] = df['utc_timestamp'].dt.year
    df['month'] = df['utc_timestamp'].dt.month
    df['day'] = df['utc_timestamp'].dt.day
    
    return df

def transform_swob(features):
    records = []
    
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [None, None, None])
        
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
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], utc=True)
    df['year'] = df['utc_timestamp'].dt.year
    df['month'] = df['utc_timestamp'].dt.month
    df['day'] = df['utc_timestamp'].dt.day
    
    return df

def transform_hydrometric(features):
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
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], utc=True)
    df['local_timestamp'] = pd.to_datetime(df['local_timestamp'])
    df['year'] = df['utc_timestamp'].dt.year
    df['month'] = df['utc_timestamp'].dt.month
    df['day'] = df['utc_timestamp'].dt.day
    
    return df

# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    Lambda entry point - triggered by S3 file upload
    """
    import urllib.parse  # Add this import
    
    print("Lambda triggered!")
    print(f"Event: {json.dumps(event)}")
    
    # Get bucket and file key from S3 event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    input_key = urllib.parse.unquote_plus(record['s3']['object']['key'])  # Decode URL
    
    print(f"Processing: s3://{bucket}/{input_key}")
    
    # Determine dataset type from path
    if 'swob_raw' in input_key:
        dataset_type = 'swob'
        transform_func = transform_swob
        output_prefix = 'swob_silver'
    elif 'climate_hourly_raw' in input_key:
        dataset_type = 'climate_hourly'
        transform_func = transform_climate_hourly
        output_prefix = 'climate_hourly_silver'
    elif 'hydrometric_raw' in input_key:
        dataset_type = 'hydrometric'
        transform_func = transform_hydrometric
        output_prefix = 'hydrometric_silver'
    else:
        print(f"Unknown dataset type in path: {input_key}")
        return {'statusCode': 400, 'body': 'Unknown dataset'}
    
    try:
        # Read JSON from S3
        obj = s3_client.get_object(Bucket=bucket, Key=input_key)
        data = json.loads(obj['Body'].read())
        features = data['features']
        print(f"Loaded {len(features)} features")
        
        # Transform
        df = transform_func(features)
        print(f"Transformed to {len(df)} records with {len(df.columns)} columns")
        
        # Generate output key
        parts = input_key.split('/')
        filename = parts[-1].replace('.json', '.parquet').replace('_delta', '')
        output_key = f"{output_prefix}/{'/'.join(parts[-4:-1])}/{filename}"
        
        # Write Parquet to S3
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression='snappy', engine='pyarrow')
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=output_key, Body=buffer.getvalue())
        
        print(f"✓ Wrote {len(df)} records to s3://{bucket}/{output_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transformation successful',
                'input': input_key,
                'output': output_key,
                'records': len(df)
            })
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise
