#!/usr/bin/env python3
"""
Sonarr-specific API functions
Handles all communication with the Sonarr API
"""

import requests
import json
import sys
import time
import datetime
import traceback
from typing import List, Dict, Any, Optional, Union, Callable, Set
# Correct the import path
from src.primary.utils.logger import get_logger
from src.primary.settings_manager import get_ssl_verify_setting

# Get logger for the Sonarr app
sonarr_logger = get_logger("sonarr")

# Use a session for better performance
session = requests.Session()

def arr_request(api_url: str, api_key: str, api_timeout: int, endpoint: str, method: str = "GET", data: Dict = None, count_api: bool = True) -> Any:
    """
    Make a request to the Sonarr API.

    Args:
        api_url: The base URL of the Sonarr API
        api_key: The API key for authentication
        api_timeout: Timeout for the API request
        endpoint: The API endpoint to call
        method: HTTP method (GET, POST, PUT, DELETE)
        data: Optional data payload for POST/PUT requests

    Returns:
        The parsed JSON response or None if the request failed
    """
    try:
        if not api_url or not api_key:
            sonarr_logger.error("No URL or API key provided")
            return None

        # Ensure api_url has a scheme
        if not (api_url.startswith('http://') or api_url.startswith('https://')):
            sonarr_logger.error(f"Invalid URL format: {api_url} - URL must start with http:// or https://")
            return None

        # Construct the full URL properly
        full_url = f"{api_url.rstrip('/')}/api/v3/{endpoint.lstrip('/')}"

        # Set up headers with User-Agent to identify Huntarr
        headers = {
            "X-Api-Key": api_key,
            "Content-Type": "application/json",
            "User-Agent": "Huntarr/1.0 (https://github.com/plexguide/Huntarr.io)"
        }

        # Get SSL verification setting
        verify_ssl = get_ssl_verify_setting()

        if not verify_ssl:
            sonarr_logger.debug("SSL verification disabled by user setting")

        try:
            if method.upper() == "GET":
                response = session.get(full_url, headers=headers, timeout=api_timeout, verify=verify_ssl)
            elif method.upper() == "POST":
                response = session.post(full_url, headers=headers, json=data, timeout=api_timeout, verify=verify_ssl)
            elif method.upper() == "PUT":
                response = session.put(full_url, headers=headers, json=data, timeout=api_timeout, verify=verify_ssl)
            elif method.upper() == "DELETE":
                response = session.delete(full_url, headers=headers, timeout=api_timeout, verify=verify_ssl)
            else:
                sonarr_logger.error(f"Unsupported HTTP method: {method}")
                return None

            # Check for successful response
            response.raise_for_status()

            # Increment API counter only if count_api is True and request was successful
            if count_api:
                try:
                    from src.primary.stats_manager import increment_hourly_cap
                    increment_hourly_cap("sonarr")
                except Exception as e:
                    sonarr_logger.warning(f"Failed to increment API counter for sonarr: {e}")

            # Check if there's any content before trying to parse JSON
            if response.content:
                try:
                    return response.json()
                except json.JSONDecodeError as jde:
                    # Log detailed information about the malformed response
                    sonarr_logger.error(f"Error decoding JSON response from {endpoint}: {str(jde)}")
                    sonarr_logger.error(f"Response status code: {response.status_code}")
                    sonarr_logger.error(f"Response content (first 200 chars): {response.content[:200]}")
                    return None
            else:
                sonarr_logger.debug(f"Empty response content from {endpoint}, returning empty dict")
                return {}

        except requests.exceptions.RequestException as e:
            # Add detailed error logging
            error_details = str(e)
            if hasattr(e, 'response') and e.response is not None:
                error_details += f", Status Code: {e.response.status_code}"
                if e.response.content:
                    error_details += f", Content: {e.response.content[:200]}"

            sonarr_logger.error(f"Error during {method} request to {endpoint}: {error_details}")
            return None
    except Exception as e:
        # Catch all exceptions and log them with traceback
        error_msg = f"CRITICAL ERROR in arr_request: {str(e)}"
        sonarr_logger.error(error_msg)
        sonarr_logger.error(f"Full traceback: {traceback.format_exc()}")
        print(error_msg, file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return None

def check_connection(api_url: str, api_key: str, api_timeout: int) -> bool:
    """Checks connection by fetching system status."""
    if not api_url:
        sonarr_logger.error("API URL is empty or not set")
        return False
    if not api_key:
        sonarr_logger.error("API Key is empty or not set")
        return False

    try:
        # Use a shorter timeout for a quick connection check
        quick_timeout = min(api_timeout, 15)
        status = get_system_status(api_url, api_key, quick_timeout)
        if status and isinstance(status, dict) and 'version' in status:
             # Log success only if debug is enabled to avoid clutter
             sonarr_logger.debug(f"Connection check successful for {api_url}. Version: {status.get('version')}")
             return True
        else:
             # Log details if the status response was unexpected
             sonarr_logger.warning(f"Connection check for {api_url} returned unexpected status: {str(status)[:200]}")
             return False
    except Exception:
        # Error should have been logged by arr_request, just indicate failure
        sonarr_logger.error(f"Connection check failed for {api_url}")
        return False

def get_system_status(api_url: str, api_key: str, api_timeout: int) -> Dict:
    """Get Sonarr system status."""
    response = arr_request(api_url, api_key, api_timeout, "system/status", count_api=False)
    if response:
        return response
    return {}

def get_series(api_url: str, api_key: str, api_timeout: int, series_id: Optional[int] = None) -> Union[List, Dict, None]:
    """
    Get series information from Sonarr.

    Returns:
        List of all series, a specific series, or None if request failed
    """
    if series_id:
        endpoint = f"series/{series_id}"
    else:
        endpoint = "series"

    return arr_request(api_url, api_key, api_timeout, endpoint, count_api=False)

# ---------------------------------------------------------------------------
# NEW: tag helpers for gating missing/upgrade hunting by tag label
# ---------------------------------------------------------------------------

def get_tag_id_by_label(api_url: str, api_key: str, api_timeout: int, tag_label: str) -> Optional[int]:
    """
    Return tag ID for a given label, or None if it doesn't exist.
    IMPORTANT: does NOT create tags (safe for gating).
    """
    try:
        if not tag_label:
            return None
        tags = arr_request(api_url, api_key, api_timeout, "tag", count_api=False)
        if not tags:
            return None

        want = tag_label.strip().lower()
        for t in tags:
            if (t.get("label") or "").strip().lower() == want:
                try:
                    return int(t.get("id"))
                except Exception:
                    return None
        return None
    except Exception as e:
        sonarr_logger.error(f"Error getting tag id for '{tag_label}': {e}")
        return None

def get_series_ids_with_tag(api_url: str, api_key: str, api_timeout: int, tag_id: int) -> Set[int]:
    """
    Return a set of Sonarr series IDs that have tag_id.
    """
    out: Set[int] = set()
    try:
        series_list = get_series(api_url, api_key, api_timeout)
        if not series_list:
            return out

        for s in series_list:
            try:
                sid = int(s.get("id"))
            except Exception:
                continue
            tags = s.get("tags") or []
            if tag_id in tags:
                out.add(sid)

        return out
    except Exception as e:
        sonarr_logger.error(f"Error building series set for tag {tag_id}: {e}")
        return out

def get_episode(api_url: str, api_key: str, api_timeout: int, episode_id: int) -> Dict:
    """Get episode information by ID."""
    response = arr_request(api_url, api_key, api_timeout, f"episode/{episode_id}", count_api=False)
    if response:
        return response
    return {}

def get_queue(api_url: str, api_key: str, api_timeout: int) -> List:
    """Get the current queue from Sonarr."""
    response = arr_request(api_url, api_key, api_timeout, "queue", count_api=False)
    if not response or "records" not in response:
        return []
    return response.get("records", [])

def get_calendar(api_url: str, api_key: str, api_timeout: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List:
    """Get calendar information for a date range."""
    params = []
    if start_date:
        params.append(f"start={start_date}")
    if end_date:
        params.append(f"end={end_date}")

    endpoint = "calendar"
    if params:
        endpoint = f"{endpoint}?{'&'.join(params)}"

    response = arr_request(api_url, api_key, api_timeout, endpoint, count_api=False)
    if response:
        return response
    return []

def command_status(api_url: str, api_key: str, api_timeout: int, command_id: Union[int, str]) -> Dict:
    """Get the status of a command by ID."""
    response = arr_request(api_url, api_key, api_timeout, f"command/{command_id}", count_api=False)
    if response:
        return response
    return {}

def get_missing_episodes(api_url: str, api_key: str, api_timeout: int, monitored_only: bool, series_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Get missing episodes from Sonarr, handling pagination."""
    endpoint = "wanted/missing"
    page = 1
    page_size = 1000
    all_missing_episodes = []
    retries_per_page = 2
    retry_delay = 3

    while True:
        retry_count = 0
        success = False

        while retry_count <= retries_per_page and not success:
            params = {
                "page": page,
                "pageSize": page_size,
                "includeSeries": "true",
                "monitored": monitored_only
            }

            if series_id is not None:
                params["seriesId"] = series_id

            base_url = api_url.rstrip('/')
            url = f"{base_url}/api/v3/{endpoint.lstrip('/')}"
            sonarr_logger.debug(f"Requesting missing episodes page {page} (attempt {retry_count+1}/{retries_per_page+1})")

            try:
                response = requests.get(url, headers={"X-Api-Key": api_key}, params=params, timeout=api_timeout)
                response.raise_for_status()

                if not response.content:
                    sonarr_logger.warning(f"Empty response for missing episodes page {page} (attempt {retry_count+1})")
                    if retry_count < retries_per_page:
                        retry_count += 1
                        time.sleep(retry_delay)
                        continue
                    else:
                        sonarr_logger.error(f"Giving up on empty response after {retries_per_page+1} attempts")
                        break

                try:
                    data = response.json()
                    records = data.get('records', [])
                    total_records_on_page = len(records)
                    sonarr_logger.debug(f"Parsed {total_records_on_page} missing episode records from page {page}")

                    if not records:
                        sonarr_logger.debug(f"No more records found on page {page}. Stopping pagination.")
                        success = True
                        break

                    all_missing_episodes.extend(records)

                    if total_records_on_page < page_size:
                        sonarr_logger.debug(f"Received {total_records_on_page} records (less than page size {page_size}). Last page.")
                        success = True
                        break

                    success = True
                    break

                except json.JSONDecodeError as e:
                    sonarr_logger.error(f"Failed to decode JSON response for missing episodes page {page} (attempt {retry_count+1}): {e}")
                    if retry_count < retries_per_page:
                        retry_count += 1
                        time.sleep(retry_delay)
                        continue
                    else:
                        sonarr_logger.error(f"Giving up after {retries_per_page+1} failed JSON decode attempts")
                        break

            except requests.exceptions.RequestException as e:
                sonarr_logger.error(f"Request error for missing episodes page {page} (attempt {retry_count+1}): {e}")
                if retry_count < retries_per_page:
                    retry_count += 1
                    time.sleep(retry_delay)
                    continue
                else:
                    sonarr_logger.error(f"Giving up on request after {retries_per_page+1} failed attempts")
                    break
            except Exception as e:
                sonarr_logger.error(f"Unexpected error for missing episodes page {page} (attempt {retry_count+1}): {e}")
                if retry_count < retries_per_page:
                    retry_count += 1
                    time.sleep(retry_delay)
                    continue
                else:
                    sonarr_logger.error(f"Giving up after unexpected error and {retries_per_page+1} attempts")
                    break

        if not success or not records:
            break

        page += 1

    sonarr_logger.info(f"Total missing episodes fetched across all pages: {len(all_missing_episodes)}")

    if monitored_only:
        original_count = len(all_missing_episodes)
        filtered_missing = [
            ep for ep in all_missing_episodes
            if ep.get('series', {}).get('monitored', False) and ep.get('monitored', False)
        ]
        sonarr_logger.debug(f"Filtered for monitored_only=True: {len(filtered_missing)} monitored episodes (out of {original_count} total)")
        return filtered_missing
    else:
        sonarr_logger.debug(f"Returning {len(all_missing_episodes)} episodes (monitored_only=False)")
        return all_missing_episodes

def get_cutoff_unmet_episodes(api_url: str, api_key: str, api_timeout: int, monitored_only: bool) -> List[Dict[str, Any]]:
    """Get cutoff unmet episodes from Sonarr, handling pagination."""
    endpoint = "wanted/cutoff"
    page = 1
    page_size = 1000
    all_cutoff_unmet = []
    retries_per_page = 2
    retry_delay = 3

    sonarr_logger.debug(f"Starting fetch for cutoff unmet episodes (monitored_only={monitored_only}).")

    while True:
        retry_count = 0
        success = False
        records = []

        while retry_count <= retries_per_page and not success:
            params = {
                "page": page,
                "pageSize": page_size,
                "includeSeries": "true",
                "sortKey": "airDateUtc",
                "sortDir": "asc",
                "monitored": monitored_only
            }
            url = f"{api_url}/api/v3/{endpoint}"
            sonarr_logger.debug(f"Requesting cutoff unmet page {page} (attempt {retry_count+1}/{retries_per_page+1})")

            try:
                response = requests.get(url, headers={"X-Api-Key": api_key}, params=params, timeout=api_timeout)
                sonarr_logger.debug(f"Sonarr API response status code for cutoff unmet page {page}: {response.status_code}")
                response.raise_for_status()

                if not response.content:
                    sonarr_logger.warning(f"Empty response for cutoff unmet episodes page {page} (attempt {retry_count+1})")
                    if retry_count < retries_per_page:
                        retry_count += 1
                        time.sleep(retry_delay)
                        continue
                    else:
                        sonarr_logger.error(f"Giving up on empty response after {retries_per_page+1} attempts")
                        break

                try:
                    data = response.json()
                    records = data.get('records', [])
                    total_records_on_page = len(records)
                    total_records_reported = data.get('totalRecords', 0)

                    if page == 1:
                        sonarr_logger.info(f"Sonarr API reports {total_records_reported} total cutoff unmet records.")

                    sonarr_logger.debug(f"Parsed {total_records_on_page} cutoff unmet records from page {page}")

                    if not records:
                        sonarr_logger.debug(f"No more cutoff unmet records found on page {page}. Stopping pagination.")
                        success = True
                        break

                    all_cutoff_unmet.extend(records)

                    if total_records_on_page < page_size:
                        sonarr_logger.debug(f"Received {total_records_on_page} records (less than page size {page_size}). Last page.")
                        success = True
                        break

                    success = True
                    break

                except json.JSONDecodeError as e:
                    sonarr_logger.error(f"Failed to decode JSON for cutoff unmet page {page} (attempt {retry_count+1}): {e}")
                    if retry_count < retries_per_page:
                        retry_count += 1
                        time.sleep(retry_delay)
                        continue
                    else:
                        sonarr_logger.error(f"Giving up after {retries_per_page+1} failed JSON decode attempts")
                        break

            except requests.exceptions.Timeout as e:
                sonarr_logger.error(f"Timeout for cutoff unmet page {page} (attempt {retry_count+1}): {e}")
                if retry_count < retries_per_page:
                    retry_count += 1
                    time.sleep(retry_delay * 2)
                    continue
                else:
                    sonarr_logger.error(f"Giving up after {retries_per_page+1} timeout failures")
                    break

            except requests.exceptions.RequestException as e:
                error_details = f"Error: {e}"
                if hasattr(e, 'response') and e.response is not None:
                    error_details += f", Status Code: {e.response.status_code}"
                    if hasattr(e.response, 'text') and e.response.text:
                        error_details += f", Response: {e.response.text[:500]}"

                sonarr_logger.error(f"Request error for cutoff unmet page {page} (attempt {retry_count+1}): {error_details}")
                if retry_count < retries_per_page:
                    retry_count += 1
                    time.sleep(retry_delay)
                    continue
                else:
                    sonarr_logger.error(f"Giving up on request after {retries_per_page+1} failed attempts")
                    break

            except Exception as e:
                sonarr_logger.error(f"Unexpected error for cutoff unmet page {page} (attempt {retry_count+1}): {e}", exc_info=True)
                if retry_count < retries_per_page:
                    retry_count += 1
                    time.sleep(retry_delay)
                    continue
                else:
                    sonarr_logger.error(f"Giving up after unexpected error and {retries_per_page+1} attempts")
                    break

        if not success or not records:
            break

        page += 1

    sonarr_logger.info(f"Total cutoff unmet episodes fetched across all pages: {len(all_cutoff_unmet)}")

    if monitored_only:
        original_count = len(all_cutoff_unmet)
        filtered_cutoff_unmet = [
            ep for ep in all_cutoff_unmet
            if ep.get('series', {}).get('monitored', False) and ep.get('monitored', False)
        ]
        sonarr_logger.debug(f"Filtered for monitored_only=True: {len(filtered_cutoff_unmet)} monitored cutoff unmet episodes remain (out of {original_count} total).")
        return filtered_cutoff_unmet
    else:
        sonarr_logger.debug(f"Returning {len(all_cutoff_unmet)} cutoff unmet episodes (monitored_only=False).")
        return all_cutoff_unmet

def get_cutoff_unmet_episodes_random_page(api_url: str, api_key: str, api_timeout: int, monitored_only: bool, count: int) -> List[Dict[str, Any]]:
    """
    Get a specified number of random cutoff unmet episodes by selecting a random page.
    """
    endpoint = "wanted/cutoff"
    page_size = 100

    params = {
        "page": 1,
        "pageSize": 1,
        "includeSeries": "true",
        "monitored": monitored_only
    }
    url = f"{api_url}/api/v3/{endpoint}"

    try:
        response = requests.get(url, headers={"X-Api-Key": api_key}, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        total_records = data.get('totalRecords', 0)

        if total_records == 0:
            sonarr_logger.info("No cutoff unmet episodes found in Sonarr.")
            return []

        total_pages = (total_records + page_size - 1) // page_size
        sonarr_logger.info(f"Found {total_records} total cutoff unmet episodes across {total_pages} pages")

        if total_pages == 0:
            return []

        import random
        random_page = random.randint(1, total_pages)
        sonarr_logger.info(f"Selected random page {random_page} of {total_pages} for quality upgrade selection")

        params = {
            "page": random_page,
            "pageSize": page_size,
            "includeSeries": "true",
            "monitored": monitored_only
        }

        response = requests.get(url, headers={"X-Api-Key": api_key}, params=params, timeout=api_timeout)
        response.raise_for_status()

        data = response.json()
        records = data.get('records', [])
        sonarr_logger.info(f"Retrieved {len(records)} episodes from page {random_page}")

        if monitored_only:
            filtered_records = [
                ep for ep in records
                if ep.get('series', {}).get('monitored', False) and ep.get('monitored', False)
            ]
            sonarr_logger.debug(f"Filtered to {len(filtered_records)} monitored episodes")
            records = filtered_records

        if len(records) > count:
            selected_records = random.sample(records, count)
            sonarr_logger.debug(f"Randomly selected {len(selected_records)} episodes from page {random_page}")
            return selected_records
        else:
            sonarr_logger.debug(f"Returning all {len(records)} episodes from page {random_page} (fewer than requested {count})")
            return records

    except requests.exceptions.RequestException as e:
        sonarr_logger.error(f"Error getting random cutoff unmet episodes from Sonarr: {str(e)}")
        return []
    except json.JSONDecodeError as e:
        sonarr_logger.error(f"Failed to decode JSON response for random cutoff selection: {str(e)}")
        return []
    except Exception as e:
        sonarr_logger.error(f"Unexpected error in random cutoff selection: {str(e)}", exc_info=True)
        return []

def get_missing_episodes_random_page(api_url: str, api_key: str, api_timeout: int, monitored_only: bool, count: int, series_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Get a specified number of random missing episodes by selecting a random page.
    """
    endpoint = "wanted/missing"
    page_size = 100
    retries = 2
    retry_delay = 3

    params = {
        "page": 1,
        "pageSize": 1,
        "includeSeries": "true",
        "monitored": monitored_only
    }
    url = f"{api_url}/api/v3/{endpoint}"

    for attempt in range(retries + 1):
        try:
            sonarr_logger.debug(f"Getting missing episodes count (attempt {attempt+1}/{retries+1})")
            response = requests.get(url, headers={"X-Api-Key": api_key}, params=params, timeout=api_timeout)
            response.raise_for_status()

            if not response.content:
                sonarr_logger.warning(f"Empty response when getting missing count (attempt {attempt+1})")
                if attempt < retries:
                    time.sleep(retry_delay)
                    continue
                return []

            try:
                data = response.json()
                total_records = data.get('totalRecords', 0)

                if total_records == 0:
                    sonarr_logger.info("No missing episodes found in Sonarr.")
                    return []

                total_pages = (total_records + page_size - 1) // page_size
                sonarr_logger.info(f"Found {total_records} total missing episodes across {total_pages} pages")

                if total_pages == 0:
                    return []

                import random
                random_page = random.randint(1, total_pages)
                sonarr_logger.info(f"Selected random page {random_page} of {total_pages} for missing episodes")

                params = {
                    "page": random_page,
                    "pageSize": page_size,
                    "includeSeries": "true",
                    "monitored": monitored_only
                }

                if series_id is not None:
                    params["seriesId"] = series_id

                response = requests.get(url, headers={"X-Api-Key": api_key}, params=params, timeout=api_timeout)
                response.raise_for_status()

                if not response.content:
                    sonarr_logger.warning(f"Empty response when getting missing episodes page {random_page}")
                    return []

                try:
                    data = response.json()
                    records = data.get('records', [])
                    sonarr_logger.info(f"Retrieved {len(records)} missing episodes from page {random_page}")

                    if monitored_only:
                        filtered_records = [
                            ep for ep in records
                            if ep.get('series', {}).get('monitored', False) and ep.get('monitored', False)
                        ]
                        sonarr_logger.debug(f"Filtered to {len(filtered_records)} monitored missing episodes")
                        records = filtered_records

                    if len(records) > count:
                        selected_records = random.sample(records, count)
                        sonarr_logger.debug(f"Randomly selected {len(selected_records)} missing episodes from page {random_page}")
                        return selected_records
                    else:
                        sonarr_logger.debug(f"Returning all {len(records)} missing episodes from page {random_page} (fewer than requested {count})")
                        return records

                except json.JSONDecodeError as jde:
                    sonarr_logger.error(f"Failed to decode JSON response for missing episodes page {random_page}: {str(jde)}")
                    if attempt < retries:
                        time.sleep(retry_delay)
                        continue
                    return []

            except json.JSONDecodeError as jde:
                sonarr_logger.error(f"Failed to decode JSON response for missing episodes count: {str(jde)}")
                if attempt < retries:
                    time.sleep(retry_delay)
                    continue
                return []

        except requests.exceptions.RequestException as e:
            sonarr_logger.error(f"Error getting missing episodes from Sonarr (attempt {attempt+1}): {str(e)}")
            if attempt < retries:
                time.sleep(retry_delay)
                continue
            return []

        except Exception as e:
            sonarr_logger.error(f"Unexpected error getting missing episodes (attempt {attempt+1}): {str(e)}", exc_info=True)
            if attempt < retries:
                time.sleep(retry_delay)
                continue
            return []

    sonarr_logger.error("All attempts to get missing episodes failed")
    return []

def search_episode(api_url: str, api_key: str, api_timeout: int, episode_ids: List[int]) -> Optional[Union[int, str]]:
    """Trigger a search for specific episodes in Sonarr."""
    if not episode_ids:
        sonarr_logger.warning("No episode IDs provided for search.")
        return None

    try:
        from src.primary.stats_manager import check_hourly_cap_exceeded
        if check_hourly_cap_exceeded("sonarr"):
            sonarr_logger.warning(f"🛑 Sonarr API hourly limit reached - skipping episode search for {len(episode_ids)} episodes")
            return None
    except Exception as e:
        sonarr_logger.error(f"Error checking hourly API cap: {e}")

    try:
        endpoint = f"{api_url}/api/v3/command"
        payload = {
            "name": "EpisodeSearch",
            "episodeIds": episode_ids
        }
        response = requests.post(endpoint, headers={"X-Api-Key": api_key}, json=payload, timeout=api_timeout)
        response.raise_for_status()
        command_id = response.json().get('id')
        sonarr_logger.info(f"Triggered Sonarr search for episode IDs: {episode_ids}. Command ID: {command_id}")

        try:
            from src.primary.stats_manager import increment_hourly_cap
            increment_hourly_cap("sonarr", 1)
            sonarr_logger.debug(f"Incremented Sonarr hourly API cap for episode search ({len(episode_ids)} episodes)")
        except Exception as cap_error:
            sonarr_logger.error(f"Failed to increment hourly API cap for episode search: {cap_error}")

        return command_id
    except requests.exceptions.RequestException as e:
        sonarr_logger.error(f"Error triggering Sonarr search for episode IDs {episode_ids}: {e}")
        return None
    except Exception as e:
        sonarr_logger.error(f"An unexpected error occurred while triggering Sonarr search: {e}")
        return None

def get_command_status(api_url: str, api_key: str, api_timeout: int, command_id: Union[int, str]) -> Optional[Dict[str, Any]]:
    """Get the status of a Sonarr command."""
    try:
        endpoint = f"{api_url}/api/v3/command/{command_id}"
        response = requests.get(endpoint, headers={"X-Api-Key": api_key}, timeout=api_timeout)
        response.raise_for_status()
        status = response.json()
        sonarr_logger.debug(f"Checked Sonarr command status for ID {command_id}: {status.get('status')}")
        return status
    except requests.exceptions.RequestException as e:
        sonarr_logger.error(f"Error getting Sonarr command status for ID {command_id}: {e}")
        return None
    except Exception as e:
        sonarr_logger.error(f"An unexpected error occurred while getting Sonarr command status: {e}")
        return None

def get_download_queue_size(api_url: str, api_key: str, api_timeout: int) -> int:
    """Get the current size of the Sonarr download queue."""
    retries = 2
    retry_delay = 3

    for attempt in range(retries + 1):
        try:
            endpoint = f"{api_url}/api/v3/queue?page=1&pageSize=1"
            response = requests.get(endpoint, headers={"X-Api-Key": api_key}, params={"includeSeries": "false"}, timeout=api_timeout)
            response.raise_for_status()

            if not response.content:
                sonarr_logger.warning(f"Empty response when getting queue size (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(retry_delay)
                    continue
                return -1

            try:
                queue_data = response.json()
                queue_size = queue_data.get('totalRecords', 0)
                sonarr_logger.debug(f"Sonarr download queue size: {queue_size}")
                return queue_size
            except json.JSONDecodeError as jde:
                sonarr_logger.error(f"Failed to decode queue JSON (attempt {attempt+1}/{retries+1}): {jde}")
                if attempt < retries:
                    time.sleep(retry_delay)
                    continue
                return -1

        except requests.exceptions.RequestException as e:
            sonarr_logger.error(f"Error getting Sonarr download queue size (attempt {attempt+1}/{retries+1}): {e}")
            if attempt < retries:
                sonarr_logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            return -1
        except Exception as e:
            sonarr_logger.error(f"Unexpected error getting queue size (attempt {attempt+1}/{retries+1}): {e}")
            if attempt < retries:
                time.sleep(retry_delay)
                continue
            return -1

    sonarr_logger.error(f"All {retries+1} attempts to get download queue size failed")
    return -1

def refresh_series(api_url: str, api_key: str, api_timeout: int, series_id: int) -> Optional[Union[int, str]]:
    """Refresh functionality has been removed as it was a performance bottleneck."""
    sonarr_logger.debug(f"Refresh functionality disabled for series ID: {series_id}")
    return 123

def get_series_by_id(api_url: str, api_key: str, api_timeout: int, series_id: int) -> Optional[Dict[str, Any]]:
    """Get series details by ID from Sonarr."""
    try:
        endpoint = f"{api_url}/api/v3/series/{series_id}"
        response = requests.get(endpoint, headers={"X-Api-Key": api_key}, timeout=api_timeout)
        response.raise_for_status()
        series_data = response.json()
        sonarr_logger.debug(f"Fetched details for Sonarr series ID: {series_id}")
        return series_data
    except requests.exceptions.RequestException as e:
        sonarr_logger.error(f"Error getting Sonarr series details for ID {series_id}: {e}")
        return None
    except Exception as e:
        sonarr_logger.error(f"An unexpected error occurred while getting Sonarr series details: {e}")
        return None

def search_season(api_url: str, api_key: str, api_timeout: int, series_id: int, season_number: int) -> Optional[Union[int, str]]:
    """Trigger a search for a specific season in Sonarr."""
    try:
        from src.primary.stats_manager import check_hourly_cap_exceeded
        if check_hourly_cap_exceeded("sonarr"):
            sonarr_logger.warning(f"🛑 Sonarr API hourly limit reached - skipping season search for series {series_id}, season {season_number}")
            return None
    except Exception as e:
        sonarr_logger.error(f"Error checking hourly API cap: {e}")

    try:
        endpoint = f"{api_url}/api/v3/command"
        payload = {
            "name": "SeasonSearch",
            "seriesId": series_id,
            "seasonNumber": season_number
        }
        response = requests.post(endpoint, headers={"X-Api-Key": api_key}, json=payload, timeout=api_timeout)
        response.raise_for_status()
        command_id = response.json().get('id')
        sonarr_logger.info(f"Triggered Sonarr season search for series ID: {series_id}, season: {season_number}. Command ID: {command_id}")

        try:
            from src.primary.stats_manager import increment_hourly_cap
            increment_hourly_cap("sonarr", 1)
            sonarr_logger.debug(f"Incremented Sonarr hourly API cap for season search (series: {series_id}, season: {season_number})")
        except Exception as cap_error:
            sonarr_logger.error(f"Failed to increment hourly API cap for season search: {cap_error}")

        return command_id
    except requests.exceptions.RequestException as e:
        sonarr_logger.error(f"Error triggering Sonarr season search for series ID {series_id}, season {season_number}: {e}")
        return None
    except Exception as e:
        sonarr_logger.error(f"An unexpected error occurred while triggering Sonarr season search: {e}")
        return None

def get_cutoff_unmet_episodes_for_series(api_url: str, api_key: str, api_timeout: int, series_id: int, monitored_only: bool = True) -> List[Dict[str, Any]]:
    """Get all cutoff unmet episodes for a specific series, handling pagination."""
    endpoint = "wanted/cutoff"
    page = 1
    page_size = 1000
    all_cutoff_unmet = []
    retries_per_page = 2
    retry_delay = 3

    sonarr_logger.debug(f"Fetching cutoff unmet episodes for series ID {series_id} using direct API filter (monitored_only={monitored_only})")

    while True:
        retry_count = 0
        success = False
        records = []

        while retry_count <= retries_per_page and not success:
            params = {
                "page": page,
                "pageSize": page_size,
                "includeSeries": "true",
                "sortKey": "airDateUtc",
                "sortDir": "asc",
                "seriesId": series_id,
                "monitored": monitored_only
            }
            url = f"{api_url}/api/v3/{endpoint}"
            sonarr_logger.debug(f"Requesting cutoff unmet page {page} for series {series_id} (attempt {retry_count+1}/{retries_per_page+1})")

            try:
                response = requests.get(url, headers={"X-Api-Key": api_key}, params=params, timeout=api_timeout)
                sonarr_logger.debug(f"Sonarr API response status code for cutoff unmet page {page}: {response.status_code}")
                response.raise_for_status()

                if not response.content:
                    sonarr_logger.warning(f"Empty response for cutoff unmet episodes page {page} (attempt {retry_count+1})")
                    if retry_count < retries_per_page:
                        retry_count += 1
                        time.sleep(retry_delay)
                        continue
                    else:
                        sonarr_logger.error(f"Giving up on empty response after {retries_per_page+1} attempts")
                        break

                try:
                    data = response.json()
                    records = data.get('records', [])
                    total_records_on_page = len(records)

                    if not records:
                        sonarr_logger.debug(f"No more cutoff unmet records found on page {page}. Stopping pagination.")
                        success = True
                        break

                    all_cutoff_unmet.extend(records)

                    if total_records_on_page < page_size:
                        sonarr_logger.debug(f"Received {total_records_on_page} records (less than page size {page_size}). Last page.")
                        success = True
                        break

                    success = True
                    break

                except json.JSONDecodeError as e:
                    sonarr_logger.error(f"Failed to decode JSON for cutoff unmet page {page} (attempt {retry_count+1}): {e}")
                    if retry_count < retries_per_page:
                        retry_count += 1
                        time.sleep(retry_delay)
                        continue
                    else:
                        sonarr_logger.error(f"Giving up on JSON decode error after {retries_per_page+1} attempts")
                        break

            except requests.exceptions.RequestException as e:
                sonarr_logger.error(f"Request error for cutoff unmet page {page} (attempt {retry_count+1}): {e}")
                if retry_count < retries_per_page:
                    retry_count += 1
                    time.sleep(retry_delay)
                    continue
                else:
                    sonarr_logger.error(f"Giving up after request error and {retries_per_page+1} attempts")
                    break

        if not success or not records:
            break

        page += 1

    verified_episodes = [ep for ep in all_cutoff_unmet if ep.get('seriesId') == series_id]

    sonarr_logger.info(f"Found {len(verified_episodes)} cutoff unmet episodes for series {series_id}")

    if monitored_only:
        original_count = len(verified_episodes)
        filtered_episodes = [
            ep for ep in verified_episodes
            if ep.get('series', {}).get('monitored', False) and ep.get('monitored', False)
        ]
        sonarr_logger.debug(f"Filtered for monitored_only=True: {len(filtered_episodes)} monitored episodes (out of {original_count} total)")
        return filtered_episodes
    else:
        return verified_episodes

def get_series_with_missing_episodes(api_url: str, api_key: str, api_timeout: int, monitored_only: bool = True, limit: int = 50, random_mode: bool = True) -> List[Dict[str, Any]]:
    """
    Get a list of series that have missing episodes, along with missing episode counts per season.
    """
    # Step 1: Get all series
    all_series = get_series(api_url, api_key, api_timeout)
    if not all_series:
        sonarr_logger.error("Failed to retrieve series list")
        return []

    # Step 2: Filter to monitored series if requested
    if monitored_only:
        filtered_series = [s for s in all_series if s.get('monitored', False)]
        sonarr_logger.info(f"Filtered from {len(all_series)} total series to {len(filtered_series)} monitored series")
    else:
        filtered_series = all_series

    if random_mode:
        import random
        sonarr_logger.info(f"Using RANDOM selection mode for missing episodes")
        random.shuffle(filtered_series)
    else:
        sonarr_logger.info(f"Using SEQUENTIAL selection mode for missing episodes")

    series_with_missing = []
    examined_count = 0

    for series in filtered_series[:limit]:
        examined_count += 1
        series_id = series.get('id')
        series_title = series.get('title', 'Unknown')

        if not series_id:
            continue

        try:
            endpoint = f"{api_url}/api/v3/episode?seriesId={series_id}"
            response = requests.get(endpoint, headers={"X-Api-Key": api_key}, timeout=api_timeout)
            response.raise_for_status()

            if not response.content:
                continue

            episodes = response.json()

            missing_episodes = [
                e for e in episodes
                if e.get('hasFile') is False and
                (not monitored_only or e.get('monitored', False))
            ]

            if not missing_episodes:
                continue

            seasons_dict = {}
            for episode in missing_episodes:
                season_number = episode.get('seasonNumber')
                if season_number is not None:
                    seasons_dict.setdefault(season_number, []).append(episode)

            if seasons_dict:
                missing_info = {
                    'series_id': series_id,
                    'series_title': series_title,
                    'seasons': [
                        {
                            'season_number': season,
                            'episode_count': len(eps),
                            'episodes': eps
                        }
                        for season, eps in seasons_dict.items()
                    ]
                }
                series_with_missing.append(missing_info)

                sonarr_logger.debug(f"Found series {series_title} with {len(missing_episodes)} missing episodes across {len(seasons_dict)} seasons")

        except Exception as e:
            sonarr_logger.error(f"Error checking missing episodes for series {series_title} (ID: {series_id}): {str(e)}")
            continue

    selection_mode = "RANDOM" if random_mode else "SEQUENTIAL"
    sonarr_logger.info(f"Examined {examined_count} series ({selection_mode} mode) and found {len(series_with_missing)} with missing episodes")
    return series_with_missing

def get_or_create_tag(api_url: str, api_key: str, api_timeout: int, tag_label: str) -> Optional[int]:
    """Get existing tag ID or create a new tag in Sonarr."""
    try:
        response = arr_request(api_url, api_key, api_timeout, "tag", count_api=False)
        if response:
            want = (tag_label or "").strip().lower()
            for tag in response:
                if (tag.get('label') or "").strip().lower() == want:
                    tag_id = tag.get('id')
                    sonarr_logger.debug(f"Found existing tag '{tag_label}' with ID: {tag_id}")
                    return tag_id

        tag_data = {"label": tag_label}
        response = arr_request(api_url, api_key, api_timeout, "tag", method="POST", data=tag_data, count_api=False)
        if response and 'id' in response:
            tag_id = response['id']
            sonarr_logger.info(f"Created new tag '{tag_label}' with ID: {tag_id}")
            return tag_id
        else:
            sonarr_logger.error(f"Failed to create tag '{tag_label}'. Response: {response}")
            return None

    except Exception as e:
        sonarr_logger.error(f"Error managing tag '{tag_label}': {e}")
        return None

def add_tag_to_series(api_url: str, api_key: str, api_timeout: int, series_id: int, tag_id: int) -> bool:
    """Add a tag to a series in Sonarr."""
    try:
        series_data = arr_request(api_url, api_key, api_timeout, f"series/{series_id}", count_api=False)
        if not series_data:
            sonarr_logger.error(f"Failed to get series data for ID: {series_id}")
            return False

        current_tags = series_data.get('tags', [])
        if tag_id in current_tags:
            sonarr_logger.debug(f"Tag {tag_id} already exists on series {series_id}")
            return True

        current_tags.append(tag_id)
        series_data['tags'] = current_tags

        response = arr_request(api_url, api_key, api_timeout, f"series/{series_id}", method="PUT", data=series_data, count_api=False)
        if response:
            sonarr_logger.debug(f"Successfully added tag {tag_id} to series {series_id}")
            return True
        else:
            sonarr_logger.error(f"Failed to update series {series_id} with tag {tag_id}")
            return False

    except Exception as e:
        sonarr_logger.error(f"Error adding tag {tag_id} to series {series_id}: {e}")
        return False

def tag_processed_series(api_url: str, api_key: str, api_timeout: int, series_id: int, tag_label: str = "huntarr-missing") -> bool:
    """Tag a series in Sonarr with the specified tag."""
    try:
        tag_id = get_or_create_tag(api_url, api_key, api_timeout, tag_label)
        if tag_id is None:
            sonarr_logger.error(f"Failed to get or create tag '{tag_label}' in Sonarr")
            return False

        success = add_tag_to_series(api_url, api_key, api_timeout, series_id, tag_id)
        if success:
            sonarr_logger.debug(f"Successfully tagged Sonarr series {series_id} with '{tag_label}'")
            return True
        else:
            sonarr_logger.error(f"Failed to add tag '{tag_label}' to Sonarr series {series_id}")
            return False

    except Exception as e:
        sonarr_logger.error(f"Error tagging Sonarr series {series_id} with '{tag_label}': {e}")
        return False

def get_quality_profiles(api_url: str, api_key: str, api_timeout: int) -> Optional[List[Dict]]:
    """Get all quality profiles configured in Sonarr."""
    try:
        sonarr_logger.debug("Fetching quality profiles from Sonarr...")

        profiles = arr_request(api_url, api_key, api_timeout, "qualityProfile", count_api=False)

        if profiles is None:
            sonarr_logger.error("Failed to retrieve quality profiles from Sonarr API.")
            return None

        sonarr_logger.debug(f"Found {len(profiles)} quality profiles in Sonarr")

        profile_names = [profile.get('name', 'Unknown') for profile in profiles]
        sonarr_logger.debug(f"Quality profiles: {', '.join(profile_names)}")

        return profiles

    except Exception as e:
        sonarr_logger.error(f"Error retrieving quality profiles: {str(e)}")
        return None
