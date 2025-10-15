#!/usr/bin/env python3
"""
PROJECT FLUME - Fixed Producer (Climate-Hourly optimized)
"""
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import requests
import boto3
from botocore.exceptions import ClientError

# -------- Config via environment variables ----------
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID")
STATE_S3_KEY = os.environ.get("STATE_S3_KEY", "data_state/state.json")

API_URL_SWOB = os.environ.get("API_URL_SWOB",
    "https://api.weather.gc.ca/collections/swob-realtime/items?lang=en")
API_URL_HYDROMETRIC = os.environ.get("API_URL_HYDROMETRIC",
    "https://api.weather.gc.ca/collections/hydrometric-realtime/items?lang=en")
API_URL_CLIMATE_HOURLY = os.environ.get("API_URL_CLIMATE_HOURLY",
    "https://api.weather.gc.ca/collections/climate-hourly/items?lang=en")

DEFAULT_INITIAL_LOOKBACK_MIN = int(os.environ.get("INITIAL_LOOKBACK_MIN", "60"))

# CHANGED: Increased from 24 hours to 7 days for Climate-Hourly
CLIMATE_HOURLY_LOOKBACK_DAYS = int(os.environ.get("CLIMATE_HOURLY_LOOKBACK_DAYS", "7"))

FETCH_LIMIT = int(os.environ.get("FETCH_LIMIT", "500"))
MIN_QA_THRESHOLD = int(os.environ.get("MIN_QA_THRESHOLD", "0"))
MAX_FUTURE_DAYS = int(os.environ.get("MAX_FUTURE_DAYS", "1"))

# NEW: Overlap buffer to catch late-arriving data (in minutes)
INCREMENTAL_OVERLAP_MIN = int(os.environ.get("INCREMENTAL_OVERLAP_MIN", "15"))

# ---------- Logging ----------
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"), 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("producer")

# ---------- Helpers ----------
s3 = boto3.client("s3")

def validate_timestamp(dt: datetime, source_str: str) -> bool:
    if not dt:
        return False
    
    now = datetime.now(timezone.utc)
    
    if dt > now + timedelta(days=MAX_FUTURE_DAYS):
        logger.warning(f"REJECTED future timestamp: {source_str} -> {dt.isoformat()} "
                      f"(>{MAX_FUTURE_DAYS} days in future)")
        return False
    
    if dt < now - timedelta(days=365):
        logger.warning(f"REJECTED old timestamp: {source_str} -> {dt.isoformat()} "
                      f"(>1 year old)")
        return False
    
    return True

def parse_iso_to_utc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    
    dt = None
    
    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s2).astimezone(timezone.utc)
        else:
            dt = datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
                   "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                break
            except Exception:
                continue
    
    if dt and not validate_timestamp(dt, s):
        return None
    
    return dt

def get_observation_timestamp(name: str, props: Dict[str, Any]) -> Optional[datetime]:
    timestamp_str = None
    
    if name == "swob":
        timestamp_str = props.get("date_tm-value") or props.get("obs_date_tm")
    elif name == "hydrometric":
        timestamp_str = props.get("DATETIME")
    elif name == "climate_hourly":
        timestamp_str = props.get("UTC_DATE") or props.get("LOCAL_DATE")
    
    if not timestamp_str:
        timestamp_str = props.get("processed_date_tm")
    
    return parse_iso_to_utc(timestamp_str)

def get_station_id(name: str, props: Dict[str, Any], feat_id: Optional[str] = None) -> str:
    station_key = None
    
    if name == "swob":
        station_key = props.get("tc_id-value") or props.get("msc_id-value")
    elif name == "hydrometric":
        station_key = props.get("STATION_NUMBER")
    elif name == "climate_hourly":
        station_key = props.get("CLIMATE_IDENTIFIER")
    
    if not station_key:
        station_key = feat_id or f"unknown_{hash(str(props)[:100])}"
    
    return str(station_key)

def is_high_quality_data(name: str, props: Dict[str, Any]) -> bool:
    if name != "swob":
        return True
    
    critical_fields = ["air_temp", "rel_hum", "stn_pres"]
    
    for field in critical_fields:
        qa_field = f"{field}-qa"
        qa_value = props.get(qa_field)
        
        if qa_value is None:
            continue
        
        if qa_value < MIN_QA_THRESHOLD:
            logger.debug(f"Rejecting record due to low quality: {field}={qa_value}")
            return False
    
    return True

def s3_get_json(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        data = json.loads(body)
        logger.info("Successfully loaded state from s3://%s/%s", bucket, key)
        return data
    except ClientError as e:
        if e.response['Error']['Code'] in ("NoSuchKey", "404"):
            logger.info("No existing state found at s3://%s/%s - starting fresh", bucket, key)
            return None
        logger.exception("Failed to get s3://%s/%s", bucket, key)
        raise

def s3_put_json(bucket: str, key: str, data: Dict[str, Any]):
    body = json.dumps(data, indent=2, default=str).encode("utf-8")
    extra = {}
    if KMS_KEY_ID:
        extra = {
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": KMS_KEY_ID
        }
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", **extra)
    logger.info("Successfully wrote state to s3://%s/%s", bucket, key)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def fetch_features_with_paging(start_url: str, params: Optional[Dict[str, str]] = None, 
                               limit: int = FETCH_LIMIT) -> List[Dict[str, Any]]:
    items = []
    next_url = start_url
    first_params = params.copy() if params else {}
    first_params["limit"] = str(limit)

    while next_url:
        logger.debug("GET %s (params=%s)", next_url, first_params if next_url == start_url else "")
        try:
            if next_url == start_url:
                resp = requests.get(next_url, params=first_params, timeout=30)
            else:
                resp = requests.get(next_url, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error("HTTP ERROR: Failed to fetch %s. Status Code: %s. Message: %s", 
                        next_url, e.response.status_code, e)
            return []
        except Exception as e:
            logger.exception("General error while fetching %s", next_url)
            raise

        j = resp.json()
        feats = j.get("features", []) or []
        items.extend(feats)

        next_url = None
        links = j.get("links", []) or []
        for link in links:
            if link.get("rel") == "next" and link.get("href"):
                next_url = link.get("href")
                break

        if not next_url:
            break

    logger.info("Fetched %d features from API", len(items))
    return items

def process_endpoint(name: str, api_url: str, state: Dict[str, Any], key_prefix: str) -> Dict[str, Any]:
    run_start_time = now_utc()
    
    logger.info("=" * 60)
    logger.info("Processing endpoint: %s", name.upper())
    logger.info("API URL: %s", api_url)
    logger.info("=" * 60)

    last_global_str = state.get(name, {}).get("global_last_processed_dt")
    
    # CHANGED: Better logic for Climate-Hourly
    if name == "climate_hourly":
        if last_global_str:
            last_global_dt = parse_iso_to_utc(last_global_str)
            # Add 15-min overlap buffer for climate-hourly
            fetch_start_dt = last_global_dt - timedelta(minutes=INCREMENTAL_OVERLAP_MIN)
            logger.info("Found previous state for %s. Last processed: %s", name, last_global_dt.isoformat())
            logger.info("Using INCREMENTAL with %d-min overlap buffer: Fetching from %s", 
                       INCREMENTAL_OVERLAP_MIN, fetch_start_dt.isoformat())
        else:
            # CHANGED: Use 7 days instead of 24 hours
            fetch_start_dt = now_utc() - timedelta(days=CLIMATE_HOURLY_LOOKBACK_DAYS)
            logger.info("No previous state for %s. Using %d-day lookback -> %s", 
                       name, CLIMATE_HOURLY_LOOKBACK_DAYS, fetch_start_dt.isoformat())
    else:
        # Real-time data with overlap buffer
        if last_global_str:
            last_global_dt = parse_iso_to_utc(last_global_str)
            # NEW: Add 15-min overlap to catch late-arriving data
            fetch_start_dt = last_global_dt - timedelta(minutes=INCREMENTAL_OVERLAP_MIN)
            logger.info("Found previous state for %s. Last processed: %s", name, last_global_dt.isoformat())
            logger.info("Using INCREMENTAL with %d-min overlap buffer: Fetching from %s",
                       INCREMENTAL_OVERLAP_MIN, fetch_start_dt.isoformat())
        else:
            fetch_start_dt = now_utc() - timedelta(minutes=DEFAULT_INITIAL_LOOKBACK_MIN)
            logger.info("No previous state for %s. Using lookback of %d minutes -> %s", 
                       name, DEFAULT_INITIAL_LOOKBACK_MIN, fetch_start_dt.isoformat())

    dt_filter = f"{fetch_start_dt.isoformat()}/.."
    params = {"datetime": dt_filter, "lang": "en"}
    
    logger.info("Fetching data with datetime filter: %s", dt_filter)

    features = []
    try:
        features = fetch_features_with_paging(api_url, params=params, limit=FETCH_LIMIT)
    except Exception:
        logger.exception("FATAL ERROR during data retrieval for %s. Skipping state update.", name)
        return state

    if not features:
        logger.info("No new features for %s since %s", name, fetch_start_dt.isoformat())
        ns = state.setdefault(name, {})
        ns["last_run_timestamp"] = now_utc().isoformat()
        ns["run_metadata"] = {
            "features_fetched": 0,
            "features_new": 0,
            "run_duration_seconds": (now_utc() - run_start_time).total_seconds(),
        }
        return state

    ns = state.setdefault(name, {})
    per_station = ns.setdefault("per_station", {})
    new_features = []
    max_seen_dt = fetch_start_dt
    min_obs_dt = None
    max_obs_dt = None
    
    rejected_quality_count = 0
    rejected_timestamp_count = 0
    seen_ids = set()

    for feat in features:
        props = feat.get("properties") or {}
        feat_id = feat.get("id")
        
        if feat_id in seen_ids:
            logger.debug("Skipping duplicate feature ID: %s", feat_id)
            continue
        seen_ids.add(feat_id)
        
        st_key = get_station_id(name, props, feat_id)
        obs_dt = get_observation_timestamp(name, props)
        
        if not obs_dt:
            rejected_timestamp_count += 1
            logger.warning("Feature %s has invalid/rejected timestamp - skipping", feat_id)
            continue
        
        if min_obs_dt is None or obs_dt < min_obs_dt:
            min_obs_dt = obs_dt
        if max_obs_dt is None or obs_dt > max_obs_dt:
            max_obs_dt = obs_dt
        
        last_station_str = per_station.get(st_key)
        last_station_dt = parse_iso_to_utc(last_station_str) if last_station_str else None

        include = False
        if last_station_dt is None:
            include = True
            logger.debug("New station %s - including", st_key)
        elif obs_dt > last_station_dt:
            include = True
            logger.debug("Station %s has newer data (%s > %s) - including", 
                       st_key, obs_dt.isoformat(), last_station_dt.isoformat())
        
        if include and not is_high_quality_data(name, props):
            include = False
            rejected_quality_count += 1
            logger.debug("Rejected feature %s due to low quality", feat_id)

        if include:
            new_features.append(feat)
            if st_key:
                existing = per_station.get(st_key)
                if not existing or (obs_dt and parse_iso_to_utc(existing) < obs_dt):
                    per_station[st_key] = obs_dt.isoformat()
            if obs_dt and obs_dt > max_seen_dt:
                max_seen_dt = obs_dt

    logger.info("Processed %d features, found %d NEW features for %s "
               "(rejected: %d quality, %d timestamp)", 
               len(features), len(new_features), name, rejected_quality_count, rejected_timestamp_count)

    if new_features:
        ts = now_utc().strftime("%Y%m%d%H%M%S")
        date_path = now_utc().strftime("year=%Y/month=%m/day=%d")
        filename = f"{name}_delta_{ts}.json"
        local_path = f"/tmp/{filename}"
        
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": new_features}, f, indent=2, default=str)

        s3_key = f"{key_prefix}/{date_path}/{filename}"
        put_args = dict(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=open(local_path, "rb"))
        if KMS_KEY_ID:
            put_args["ServerSideEncryption"] = "aws:kms"
            put_args["SSEKMSKeyId"] = KMS_KEY_ID
        s3.put_object(**put_args)
        logger.info("Uploaded delta to s3://%s/%s (count=%d)", S3_BUCKET_NAME, s3_key, len(new_features))
    else:
        logger.info("No delta to upload for %s", name)

    ns["global_last_processed_dt"] = max_seen_dt.isoformat()
    ns["per_station"] = per_station
    ns["last_run_timestamp"] = now_utc().isoformat()
    ns["stations_tracked"] = len(per_station)
    
    ns["run_metadata"] = {
        "features_fetched": len(features),
        "features_new": len(new_features),
        "features_rejected_quality": rejected_quality_count,
        "features_rejected_timestamp": rejected_timestamp_count,
        "run_duration_seconds": (now_utc() - run_start_time).total_seconds(),
        "oldest_observation": min_obs_dt.isoformat() if min_obs_dt else None,
        "newest_observation": max_obs_dt.isoformat() if max_obs_dt else None,
    }
    
    state[name] = ns
    logger.info("Updated state for %s: global_last=%s, stations_tracked=%d", 
               name, max_seen_dt.isoformat(), len(per_station))
    
    return state

def main():
    logger.info("=" * 80)
    logger.info("PROJECT FLUME - Data Producer (Climate-Hourly Optimized)")
    logger.info("=" * 80)
    
    if not S3_BUCKET_NAME:
        logger.error("S3_BUCKET_NAME is required in env; exiting.")
        return

    logger.info("Configuration:")
    logger.info("  S3 Bucket: %s", S3_BUCKET_NAME)
    logger.info("  State File: %s", STATE_S3_KEY)
    logger.info("  KMS Encryption: %s", "Enabled" if KMS_KEY_ID else "Disabled")
    logger.info("  Real-time Initial Lookback: %d minutes", DEFAULT_INITIAL_LOOKBACK_MIN)
    logger.info("  Climate-Hourly Lookback: %d days", CLIMATE_HOURLY_LOOKBACK_DAYS)
    logger.info("  Incremental Overlap Buffer: %d minutes", INCREMENTAL_OVERLAP_MIN)
    logger.info("  Quality Threshold: %d", MIN_QA_THRESHOLD)
    logger.info("  Max Future Days: %d (timestamp validation)", MAX_FUTURE_DAYS)
    logger.info("")

    state = s3_get_json(S3_BUCKET_NAME, STATE_S3_KEY) or {}
    logger.info("Current state contains: %s", list(state.keys()) if state else "No existing state")
    
    original_state = json.loads(json.dumps(state, default=str))

    try:
        logger.info("")
        state = process_endpoint("swob", API_URL_SWOB, state, key_prefix="swob_raw")
    except Exception:
        logger.exception("FATAL: Uncaught error processing SWOB endpoint.")
        state["swob"] = original_state.get("swob", {})

    try:
        logger.info("")
        state = process_endpoint("hydrometric", API_URL_HYDROMETRIC, state, key_prefix="hydrometric_raw")
    except Exception:
        logger.exception("FATAL: Uncaught error processing HYDROMETRIC endpoint.")
        state["hydrometric"] = original_state.get("hydrometric", {})

    try:
        logger.info("")
        state = process_endpoint("climate_hourly", API_URL_CLIMATE_HOURLY, state, key_prefix="climate_hourly_raw")
    except Exception:
        logger.exception("FATAL: Uncaught error processing CLIMATE-HOURLY endpoint.")
        state["climate_hourly"] = original_state.get("climate_hourly", {})

    try:
        logger.info("")
        logger.info("=" * 60)
        logger.info("Persisting final state to S3...")
        logger.info("State summary:")
        for endpoint_name in state:
            endpoint_state = state[endpoint_name]
            logger.info("  %s:", endpoint_name.upper())
            logger.info("    Last processed: %s", endpoint_state.get("global_last_processed_dt", "N/A"))
            logger.info("    Stations tracked: %s", endpoint_state.get("stations_tracked", "N/A"))
            metadata = endpoint_state.get("run_metadata", {})
            logger.info("    Last run: fetched=%s, new=%s, rejected_quality=%s, rejected_timestamp=%s", 
                       metadata.get("features_fetched", 0),
                       metadata.get("features_new", 0),
                       metadata.get("features_rejected_quality", 0),
                       metadata.get("features_rejected_timestamp", 0))
        
        s3_put_json(S3_BUCKET_NAME, STATE_S3_KEY, state)
        logger.info("=" * 60)
        logger.info("Producer run complete - state successfully saved")
        logger.info("=" * 60)
    except Exception:
        logger.critical("CRITICAL: Failed to persist final state. Next run will reprocess data.")
        raise

if __name__ == "__main__":
    main()