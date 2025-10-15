#!/usr/bin/env python3
"""
PROJECT FLUME - Water Level Forecasting
Predict water levels 6-24 hours ahead using time-series models
"""
import pandas as pd
import numpy as np
import boto3
import io
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

S3_BUCKET = "project-nimbus-raw-data-lake-12345-raj"
s3_client = boto3.client('s3')

def read_parquet_from_s3(key):
    """Read Parquet file from S3"""
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(obj['Body'].read()))
    except Exception as e:
        print(f"Error reading {key}: {e}")
        return pd.DataFrame()

def write_parquet_to_s3(df, key):
    """Write DataFrame to S3 as Parquet"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"✓ Wrote {len(df):,} records to s3://{S3_BUCKET}/{key}")

def create_time_features(df):
    """Create time-based features for forecasting"""
    df = df.copy()
    df['hour_of_day'] = df['hour'].dt.hour
    df['day_of_week'] = df['hour'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    return df

def forecast_station(station_data, forecast_hours=24):
    """
    Forecast water levels for a single station
    Uses linear regression with time features
    """
    # Sort by time
    station_data = station_data.sort_values('hour')
    
    # Need at least 3 data points for forecasting
    if len(station_data) < 3:
        return None
    
    # Create features
    station_data = create_time_features(station_data)
    
    # Prepare training data
    X = station_data[['hour_of_day', 'day_of_week']].values
    y = station_data['water_level_mean'].values
    
    # Add time index as feature (hours since first observation)
    time_index = np.arange(len(station_data)).reshape(-1, 1)
    X = np.hstack([X, time_index])
    
    # Train simple linear model
    model = LinearRegression()
    model.fit(X, y)
    
    # Generate future timestamps
    last_time = station_data['hour'].max()
    future_times = [last_time + timedelta(hours=i) for i in range(1, forecast_hours + 1)]
    
    # Create features for future predictions
    future_df = pd.DataFrame({'hour': future_times})
    future_df = create_time_features(future_df)
    
    # Add future time indices
    last_index = len(station_data)
    future_time_indices = np.arange(last_index, last_index + forecast_hours).reshape(-1, 1)
    
    X_future = future_df[['hour_of_day', 'day_of_week']].values
    X_future = np.hstack([X_future, future_time_indices])
    
    # Make predictions
    predictions = model.predict(X_future)
    
    # Calculate confidence interval (simple estimate based on historical variance)
    residuals = y - model.predict(X)
    std_error = np.std(residuals)
    
    # Create forecast dataframe
    forecast_df = pd.DataFrame({
        'hour': future_times,
        'predicted_water_level': predictions,
        'confidence_lower': predictions - 1.96 * std_error,
        'confidence_upper': predictions + 1.96 * std_error
    })
    
    return forecast_df

def generate_all_forecasts():
    """Generate forecasts for all water monitoring stations"""
    print("="*60)
    print("PROJECT FLUME - Water Level Forecasting")
    print("="*60)
    
    # Read hydrometric hourly data
    print("\nReading hydrometric hourly data...")
    hydro_hourly = read_parquet_from_s3('gold/hydro_hourly_summary/hydro_hourly_summary.parquet')
    
    if hydro_hourly.empty:
        print("No hydrometric data available for forecasting")
        return
    
    print(f"Loaded {len(hydro_hourly):,} hourly records for {hydro_hourly['station_number'].nunique():,} stations")
    
    # Convert hour to datetime
    hydro_hourly['hour'] = pd.to_datetime(hydro_hourly['hour'])
    
    # Generate forecasts for each station
    all_forecasts = []
    stations_forecasted = 0
    stations_skipped = 0
    
    print("\nGenerating forecasts...")
    
    for station_num, station_data in hydro_hourly.groupby('station_number'):
        # Only forecast if we have valid water level data
        if station_data['water_level_mean'].isna().all():
            stations_skipped += 1
            continue
        
        # Remove NaN values
        station_data = station_data.dropna(subset=['water_level_mean'])
        
        if len(station_data) < 3:
            stations_skipped += 1
            continue
        
        # Generate forecast
        forecast = forecast_station(station_data, forecast_hours=24)
        
        if forecast is not None:
            forecast['station_number'] = station_num
            forecast['station_name'] = station_data['station_name'].iloc[0]
            forecast['province'] = station_data['province'].iloc[0]
            forecast['latitude'] = station_data['latitude'].iloc[0]
            forecast['longitude'] = station_data['longitude'].iloc[0]
            forecast['current_water_level'] = station_data['water_level_mean'].iloc[-1]
            forecast['forecast_change'] = forecast['predicted_water_level'] - station_data['water_level_mean'].iloc[-1]
            
            all_forecasts.append(forecast)
            stations_forecasted += 1
            
            if stations_forecasted % 100 == 0:
                print(f"  Forecasted {stations_forecasted} stations...")
    
    print(f"\n✓ Generated forecasts for {stations_forecasted} stations")
    print(f"  Skipped {stations_skipped} stations (insufficient data)")
    
    if all_forecasts:
        # Combine all forecasts
        forecasts_df = pd.concat(all_forecasts, ignore_index=True)
        
        # Write to Gold layer
        write_parquet_to_s3(forecasts_df, 'gold/water_level_forecasts/forecasts.parquet')
        
        # Create 6-hour ahead summary for quick dashboard access
        six_hour_forecast = forecasts_df[
            forecasts_df['hour'] == forecasts_df.groupby('station_number')['hour'].transform('min') + timedelta(hours=6)
        ].copy()
        
        write_parquet_to_s3(six_hour_forecast, 'gold/water_level_forecasts/6hour_forecast.parquet')
        
        print(f"\nForecast Summary:")
        print(f"  Total forecast records: {len(forecasts_df):,}")
        print(f"  Stations with forecasts: {forecasts_df['station_number'].nunique():,}")
        print(f"  Forecast horizon: 24 hours")
        print(f"  Time range: {forecasts_df['hour'].min()} to {forecasts_df['hour'].max()}")
        
        # Show stations with significant predicted changes
        significant_changes = six_hour_forecast[
            six_hour_forecast['forecast_change'].abs() > 0.5
        ].sort_values('forecast_change', ascending=False)
        
        if not significant_changes.empty:
            print(f"\n⚠️  Stations with significant predicted changes (>0.5m in 6 hours):")
            print(f"  Rising: {len(significant_changes[significant_changes['forecast_change'] > 0.5])}")
            print(f"  Falling: {len(significant_changes[significant_changes['forecast_change'] < -0.5])}")
    
    print("\n" + "="*60)
    print("✓ FORECASTING COMPLETE")
    print("="*60)

if __name__ == "__main__":
    generate_all_forecasts()