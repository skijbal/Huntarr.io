#!/usr/bin/env python3
"""
Sonarr missing episode processing
Handles all missing episode operations for Sonarr

UPDATED:
- Only hunts missing for series tagged "search"
"""

import time
import random
from typing import List, Dict, Any, Callable, Set
from src.primary.utils.logger import get_logger
from src.primary.settings_manager import load_settings, get_advanced_setting
from src.primary.utils.history_utils import log_processed_media
from src.primary.stats_manager import increment_stat, increment_stat_only, check_hourly_cap_exceeded
from src.primary.stateful_manager import is_processed, add_processed_id
from src.primary.apps.sonarr import api as sonarr_api

# Get logger for the Sonarr app
sonarr_logger = get_logger("sonarr")

def should_delay_episode_search(air_date_str: str, delay_days: int) -> bool:
    """Delay searches until air date + delay_days."""
    if delay_days <= 0 or not air_date_str:
        return False

    try:
        air_date_unix = time.mktime(time.strptime(air_date_str, '%Y-%m-%dT%H:%M:%SZ'))
        current_unix = time.time()
        search_start_unix = air_date_unix + (delay_days * 24 * 60 * 60)
        return current_unix < search_start_unix
    except (ValueError, TypeError) as e:
        sonarr_logger.warning(f"Could not parse air date '{air_date_str}' for delay calculation: {e}")
        return False

def _get_allowed_series_ids_for_missing(api_url: str, api_key: str, api_timeout: int) -> Set[int]:
    """
    Returns a set of series IDs tagged with "search".
    If tag is missing or no series match, returns empty set.
    """
    sonarr_settings = load_settings("sonarr")
    search_tag_label = sonarr_settings.get("tag_search_label", "search")

    tag_id = sonarr_api.get_tag_id_by_label(api_url, api_key, api_timeout, search_tag_label)
    if tag_id is None:
        sonarr_logger.warning(
            f"Sonarr tag '{search_tag_label}' not found. Skipping missing processing to avoid hunting everything."
        )
        return set()

    allowed = sonarr_api.get_series_ids_with_tag(api_url, api_key, api_timeout, tag_id)
    if not allowed:
        sonarr_logger.info(f"No Sonarr series tagged '{search_tag_label}' found. Nothing to hunt.")
    return allowed

def process_missing_episodes(
    api_url: str,
    api_key: str,
    instance_name: str,
    api_timeout: int = get_advanced_setting("api_timeout", 120),
    monitored_only: bool = True,
    skip_future_episodes: bool = True,
    hunt_missing_items: int = 5,
    hunt_missing_mode: str = "seasons_packs",
    air_date_delay_days: int = 0,
    command_wait_delay: int = get_advanced_setting("command_wait_delay", 1),
    command_wait_attempts: int = get_advanced_setting("command_wait_attempts", 600),
    stop_check: Callable[[], bool] = lambda: False
) -> bool:
    """
    Process missing episodes for Sonarr.
    Supports seasons_packs, shows, and episodes modes.
    """
    if hunt_missing_items <= 0:
        sonarr_logger.info("'hunt_missing_items' setting is 0 or less. Skipping missing processing.")
        return False

    allowed_series_ids = _get_allowed_series_ids_for_missing(api_url, api_key, api_timeout)
    if not allowed_series_ids:
        return False

    sonarr_logger.info(
        f"Checking for {hunt_missing_items} missing episodes in {hunt_missing_mode} mode for instance '{instance_name}' "
        f"(tag-gated to series tagged 'search')..."
    )

    if hunt_missing_mode == "seasons_packs":
        sonarr_logger.info("Season [Packs] mode selected - searching for complete season packs")
        return process_missing_seasons_packs_mode(
            api_url, api_key, instance_name, api_timeout, monitored_only,
            skip_future_episodes, hunt_missing_items, air_date_delay_days,
            command_wait_delay, command_wait_attempts, stop_check,
            allowed_series_ids
        )
    elif hunt_missing_mode == "shows":
        sonarr_logger.info("Show-based missing mode selected")
        return process_missing_shows_mode(
            api_url, api_key, instance_name, api_timeout, monitored_only,
            skip_future_episodes, hunt_missing_items, air_date_delay_days,
            command_wait_delay, command_wait_attempts, stop_check,
            allowed_series_ids
        )
    elif hunt_missing_mode == "episodes":
        sonarr_logger.warning(
            "Episodes mode selected - WARNING: This mode makes excessive API calls and does not support tagging. "
            "Consider using Season Packs mode instead."
        )
        return process_missing_episodes_mode(
            api_url, api_key, instance_name, api_timeout, monitored_only,
            skip_future_episodes, hunt_missing_items, air_date_delay_days,
            command_wait_delay, command_wait_attempts, stop_check,
            allowed_series_ids
        )
    else:
        sonarr_logger.error("Invalid hunt_missing_mode. Valid options are 'seasons_packs', 'shows', or 'episodes'.")
        return False

def process_missing_seasons_packs_mode(
    api_url: str,
    api_key: str,
    instance_name: str,
    api_timeout: int,
    monitored_only: bool,
    skip_future_episodes: bool,
    hunt_missing_items: int,
    air_date_delay_days: int,
    command_wait_delay: int,
    command_wait_attempts: int,
    stop_check: Callable[[], bool],
    allowed_series_ids: Set[int]
) -> bool:
    """
    Process missing seasons using the SeasonSearch command (season packs).
    Tag-gated: only considers episodes whose seriesId is in allowed_series_ids.
    """
    processed_any = False

    sonarr_settings = load_settings("sonarr")
    tag_processed_items = sonarr_settings.get("tag_processed_items", True)

    missing_episodes = sonarr_api.get_missing_episodes_random_page(
        api_url, api_key, api_timeout, monitored_only, hunt_missing_items * 20
    )
    if not missing_episodes:
        sonarr_logger.info("No missing episodes found")
        return False

    # Tag gate (search tag): keep only episodes belonging to tagged series
    missing_episodes = [
        ep for ep in missing_episodes
        if int(ep.get("seriesId") or 0) in allowed_series_ids
    ]

    if not missing_episodes:
        sonarr_logger.info("No missing episodes found for series tagged 'search'.")
        return False

    sonarr_logger.info(f"Retrieved {len(missing_episodes)} missing episodes from random page selection (after tag gating).")

    # Filter out future episodes if configured
    if skip_future_episodes:
        now_unix = time.time()
        filtered_episodes = []
        skipped_count = 0

        for episode in missing_episodes:
            air_date_str = episode.get('airDateUtc')
            if air_date_str:
                try:
                    air_date_unix = time.mktime(time.strptime(air_date_str, '%Y-%m-%dT%H:%M:%SZ'))
                    if air_date_unix < now_unix:
                        filtered_episodes.append(episode)
                    else:
                        skipped_count += 1
                        sonarr_logger.debug(f"Skipping future episode ID {episode.get('id')} with air date: {air_date_str}")
                except (ValueError, TypeError) as e:
                    sonarr_logger.warning(
                        f"Could not parse air date '{air_date_str}' for episode ID {episode.get('id')}. Error: {e}. Including it."
                    )
                    filtered_episodes.append(episode)
            else:
                filtered_episodes.append(episode)

        missing_episodes = filtered_episodes
        if skipped_count > 0:
            sonarr_logger.info(f"Skipped {skipped_count} future episodes based on air date.")

    if not missing_episodes:
        sonarr_logger.info("No missing episodes left to process after filtering future episodes.")
        return False

    # NOTE: air_date_delay_days intentionally not applied in season pack mode (it was previously buggy)

    # Group episodes by series and season
    missing_seasons: Dict[str, Dict[str, Any]] = {}
    for episode in missing_episodes:
        if monitored_only and not episode.get('monitored', False):
            continue

        series_id = episode.get('seriesId')
        if not series_id:
            continue

        season_number = episode.get('seasonNumber')
        series_title = episode.get('series', {}).get('title', 'Unknown Series')

        key = f"{series_id}:{season_number}"
        if key not in missing_seasons:
            missing_seasons[key] = {
                'series_id': series_id,
                'season_number': season_number,
                'series_title': series_title,
                'episode_count': 0
            }
        missing_seasons[key]['episode_count'] += 1

    seasons_list = list(missing_seasons.values())
    seasons_list.sort(key=lambda x: x['episode_count'], reverse=True)

    # Filter out already processed seasons
    unprocessed_seasons = []
    for season in seasons_list:
        season_id = f"{season['series_id']}_{season['season_number']}"
        if not is_processed("sonarr", instance_name, season_id):
            unprocessed_seasons.append(season)
        else:
            sonarr_logger.debug(f"Skipping already processed season ID: {season_id}")

    sonarr_logger.info(f"Found {len(unprocessed_seasons)} unprocessed seasons with missing episodes out of {len(seasons_list)} total.")

    if not unprocessed_seasons:
        sonarr_logger.info("All seasons with missing episodes have been processed.")
        return False

    random.shuffle(unprocessed_seasons)

    processed_count = 0

    if unprocessed_seasons and hunt_missing_items > 0:
        seasons_to_process = unprocessed_seasons[:hunt_missing_items]
        sonarr_logger.info(f"Randomly selected {min(len(unprocessed_seasons), hunt_missing_items)} seasons with missing episodes:")
        for idx, season in enumerate(seasons_to_process):
            sonarr_logger.info(
                f"  {idx+1}. {season['series_title']} - Season {season['season_number']} "
                f"({season['episode_count']} missing episodes) (Series ID: {season['series_id']})"
            )

    for season in unprocessed_seasons:
        if processed_count >= hunt_missing_items:
            break

        if stop_check():
            sonarr_logger.info("Stop signal received, halting processing.")
            break

        try:
            if check_hourly_cap_exceeded("sonarr"):
                sonarr_logger.warning(f"🛑 Sonarr API hourly limit reached - stopping season pack processing after {processed_count} seasons")
                break
        except Exception as e:
            sonarr_logger.error(f"Error checking hourly API cap: {e}")

        series_id = season['series_id']
        season_number = season['season_number']
        series_title = season['series_title']
        episode_count = season['episode_count']

        sonarr_logger.info(f"Searching for season pack: {series_title} - Season {season_number} (contains {episode_count} missing episodes)")

        command_id = sonarr_api.search_season(api_url, api_key, api_timeout, series_id, season_number)

        if command_id:
            processed_any = True
            processed_count += 1

            season_id = f"{series_id}_{season_number}"
            add_processed_id("sonarr", instance_name, season_id)

            # Tag the series if enabled
            if tag_processed_items:
                from src.primary.settings_manager import get_custom_tag
                custom_tag = get_custom_tag("sonarr", "missing", "huntarr-missing")
                try:
                    sonarr_api.tag_processed_series(api_url, api_key, api_timeout, series_id, custom_tag)
                    sonarr_logger.debug(f"Tagged series {series_id} with '{custom_tag}'")
                except Exception as e:
                    sonarr_logger.warning(f"Failed to tag series {series_id} with '{custom_tag}': {e}")

            media_name = f"{series_title} - Season {season_number} (contains {episode_count} missing episodes)"
            log_processed_media("sonarr", media_name, season_id, instance_name, "missing")

            # Increment hunted stats for each missing episode in the season (API call already tracked in search_season)
            for _ in range(episode_count):
                increment_stat_only("sonarr", "hunted")

            if command_wait_delay > 0 and command_wait_attempts > 0:
                wait_for_command(
                    api_url, api_key, api_timeout, command_id,
                    command_wait_delay, command_wait_attempts, "Season Search", stop_check
                )
        else:
            sonarr_logger.error(f"Failed to trigger season search for {series_title} Season {season_number}.")

    sonarr_logger.info(f"Processed {processed_count} missing season packs for Sonarr.")
    return processed_any

def process_missing_shows_mode(
    api_url: str,
    api_key: str,
    instance_name: str,
    api_timeout: int,
    monitored_only: bool,
    skip_future_episodes: bool,
    hunt_missing_items: int,
    air_date_delay_days: int,
    command_wait_delay: int,
    command_wait_attempts: int,
    stop_check: Callable[[], bool],
    allowed_series_ids: Set[int]
) -> bool:
    """Process missing episodes in show mode - gets all missing episodes for entire shows (tag-gated)."""
    processed_any = False

    sonarr_settings = load_settings("sonarr")
    tag_processed_items = sonarr_settings.get("tag_processed_items", True)

    sonarr_logger.info("Retrieving series with missing episodes...")
    series_with_missing = sonarr_api.get_series_with_missing_episodes(
        api_url, api_key, api_timeout, monitored_only, random_mode=True
    )

    if not series_with_missing:
        sonarr_logger.info("No series with missing episodes found.")
        return False

    # Tag gate: only series tagged search
    series_with_missing = [
        s for s in series_with_missing
        if int(s.get("series_id") or 0) in allowed_series_ids
    ]

    if not series_with_missing:
        sonarr_logger.info("No series tagged 'search' have missing episodes.")
        return False

    unprocessed_series = []
    for series in series_with_missing:
        series_id = str(series.get("series_id"))
        if not is_processed("sonarr", instance_name, series_id):
            unprocessed_series.append(series)
        else:
            sonarr_logger.debug(f"Skipping already processed series ID: {series_id}")

    sonarr_logger.info(f"Found {len(unprocessed_series)} unprocessed series with missing episodes out of {len(series_with_missing)} total.")

    if not unprocessed_series:
        sonarr_logger.info("All series with missing episodes have been processed.")
        return False

    shows_to_process = random.sample(unprocessed_series, min(len(unprocessed_series), hunt_missing_items))

    if shows_to_process:
        sonarr_logger.info("Shows selected for processing in this cycle:")
        for idx, show in enumerate(shows_to_process):
            show_id = show.get('series_id')
            show_title = show.get('series_title', 'Unknown Show')
            episode_count = sum(season.get('episode_count', 0) for season in show.get('seasons', []))
            sonarr_logger.info(f"  {idx+1}. {show_title} ({episode_count} missing episodes) (Show ID: {show_id})")

    for show in shows_to_process:
        if stop_check():
            sonarr_logger.info("Stop signal received, halting processing.")
            break

        try:
            if check_hourly_cap_exceeded("sonarr"):
                sonarr_logger.warning("🛑 Sonarr API hourly limit reached - stopping shows processing")
                break
        except Exception as e:
            sonarr_logger.error(f"Error checking hourly API cap: {e}")

        show_id = show.get("series_id")
        show_title = show.get("series_title", "Unknown Show")

        missing_episodes = []
        for season in show.get('seasons', []):
            missing_episodes.extend(season.get('episodes', []))

        if skip_future_episodes:
            now_unix = time.time()
            filtered = []
            skipped = 0
            for ep in missing_episodes:
                air = ep.get("airDateUtc")
                if not air:
                    filtered.append(ep)
                    continue
                try:
                    if time.mktime(time.strptime(air, '%Y-%m-%dT%H:%M:%SZ')) < now_unix:
                        filtered.append(ep)
                    else:
                        skipped += 1
                except Exception:
                    filtered.append(ep)
            missing_episodes = filtered
            if skipped > 0:
                sonarr_logger.info(f"Skipped {skipped} future episodes for {show_title} based on air date.")

        if air_date_delay_days > 0:
            delayed_episodes = []
            delayed_count = 0
            for episode in missing_episodes:
                air_date_str = episode.get('airDateUtc')
                if should_delay_episode_search(air_date_str, air_date_delay_days):
                    delayed_count += 1
                else:
                    delayed_episodes.append(episode)
            missing_episodes = delayed_episodes
            if delayed_count > 0:
                sonarr_logger.info(f"Delayed {delayed_count} episodes for {show_title} due to {air_date_delay_days}-day air date delay setting.")

        if not missing_episodes:
            sonarr_logger.info(f"No eligible missing episodes found for {show_title} after filtering.")
            continue

        episode_ids = [episode.get('id') for episode in missing_episodes if episode.get('id')]
        if not episode_ids:
            sonarr_logger.warning(f"No valid episode IDs found for {show_title}.")
            continue

        sonarr_logger.info(f"Searching for {len(episode_ids)} missing episodes for {show_title}...")
        command_id = sonarr_api.search_episode(api_url, api_key, api_timeout, episode_ids)

        if command_id:
            processed_any = True
            sonarr_logger.info(f"Successfully triggered search for {len(episode_ids)} missing episodes in {show_title}")

            if tag_processed_items:
                from src.primary.settings_manager import get_custom_tag
                custom_tag = get_custom_tag("sonarr", "shows_missing", "huntarr-shows-missing")
                try:
                    sonarr_api.tag_processed_series(api_url, api_key, api_timeout, show_id, custom_tag)
                    sonarr_logger.debug(f"Tagged series {show_id} with '{custom_tag}'")
                except Exception as e:
                    sonarr_logger.warning(f"Failed to tag series {show_id} with '{custom_tag}': {e}")

            for episode_id in episode_ids:
                add_processed_id("sonarr", instance_name, str(episode_id))

                for episode in missing_episodes:
                    if episode.get('id') == episode_id:
                        season = episode.get('seasonNumber', 'Unknown')
                        ep_num = episode.get('episodeNumber', 'Unknown')
                        title = episode.get('title', 'Unknown Title')
                        try:
                            season_episode = f"S{season:02d}E{ep_num:02d}"
                        except Exception:
                            season_episode = f"S{season}E{ep_num}"
                        media_name = f"{show_title} - {season_episode} - {title}"
                        log_processed_media("sonarr", media_name, str(episode_id), instance_name, "missing")
                        break

            add_processed_id("sonarr", instance_name, str(show_id))
            log_processed_media("sonarr", f"{show_title} - Complete Series ({len(episode_ids)} episodes)", str(show_id), instance_name, "missing")

            increment_stat("sonarr", "hunted", len(episode_ids))

            if command_wait_delay > 0 and command_wait_attempts > 0:
                wait_for_command(
                    api_url, api_key, api_timeout, command_id,
                    command_wait_delay, command_wait_attempts, "Episode Search", stop_check
                )
        else:
            sonarr_logger.error(f"Failed to trigger search for {show_title}.")

    sonarr_logger.info("Show-based missing episode processing complete.")
    return processed_any

def process_missing_episodes_mode(
    api_url: str,
    api_key: str,
    instance_name: str,
    api_timeout: int,
    monitored_only: bool,
    skip_future_episodes: bool,
    hunt_missing_items: int,
    air_date_delay_days: int,
    command_wait_delay: int,
    command_wait_attempts: int,
    stop_check: Callable[[], bool],
    allowed_series_ids: Set[int]
) -> bool:
    """
    Process missing episodes in individual episode mode (tag-gated).
    """
    processed_any = False

    sonarr_logger.warning("Using Episodes mode - This will make more API calls and does not support tagging")

    missing_episodes = sonarr_api.get_missing_episodes_random_page(
        api_url, api_key, api_timeout, monitored_only, hunt_missing_items * 2
    )

    if not missing_episodes:
        sonarr_logger.info("No missing episodes found for individual processing.")
        return False

    # Tag gate (search tag)
    missing_episodes = [
        ep for ep in missing_episodes
        if int(ep.get("seriesId") or 0) in allowed_series_ids
    ]

    if not missing_episodes:
        sonarr_logger.info("No missing episodes found for series tagged 'search' (episodes mode).")
        return False

    if skip_future_episodes:
        now_unix = time.time()
        filtered_episodes = []
        skipped_count = 0

        for episode in missing_episodes:
            air_date_str = episode.get('airDateUtc')
            if air_date_str:
                try:
                    air_date_unix = time.mktime(time.strptime(air_date_str, '%Y-%m-%dT%H:%M:%SZ'))
                    if air_date_unix < now_unix:
                        filtered_episodes.append(episode)
                    else:
                        skipped_count += 1
                except Exception:
                    filtered_episodes.append(episode)
            else:
                filtered_episodes.append(episode)

        missing_episodes = filtered_episodes
        if skipped_count > 0:
            sonarr_logger.info(f"Skipped {skipped_count} future episodes based on air date.")

    if air_date_delay_days > 0:
        delayed_episodes = []
        for episode in missing_episodes:
            if not should_delay_episode_search(episode.get('airDateUtc'), air_date_delay_days):
                delayed_episodes.append(episode)
        missing_episodes = delayed_episodes

    if not missing_episodes:
        sonarr_logger.info("No missing episodes left to process after filtering.")
        return False

    unprocessed_episodes = []
    for episode in missing_episodes:
        episode_id = str(episode.get('id'))
        if not is_processed("sonarr", instance_name, episode_id):
            unprocessed_episodes.append(episode)

    if not unprocessed_episodes:
        sonarr_logger.info("All missing episodes have been processed.")
        return False

    random.shuffle(unprocessed_episodes)
    episodes_to_process = unprocessed_episodes[:hunt_missing_items]

    processed_count = 0
    for episode in episodes_to_process:
        if stop_check():
            sonarr_logger.info("Stop requested. Aborting episode processing.")
            break

        try:
            if check_hourly_cap_exceeded("sonarr"):
                sonarr_logger.warning(f"🛑 Sonarr API hourly limit reached - stopping episodes processing after {processed_count} episodes")
                break
        except Exception as e:
            sonarr_logger.error(f"Error checking hourly API cap: {e}")

        episode_id = episode.get('id')
        series_info = episode.get('series', {})
        series_title = series_info.get('title', 'Unknown Series')
        season_number = episode.get('seasonNumber', 'Unknown')
        episode_number = episode.get('episodeNumber', 'Unknown')
        episode_title = episode.get('title', 'Unknown Episode')

        try:
            season_episode = f"S{season_number:02d}E{episode_number:02d}"
        except Exception:
            season_episode = f"S{season_number}E{episode_number}"

        sonarr_logger.info(f"Processing episode: {series_title} - {season_episode} - {episode_title}")

        command_id = sonarr_api.search_episode(api_url, api_key, api_timeout, [episode_id])

        if command_id:
            processed_any = True
            processed_count += 1

            add_processed_id("sonarr", instance_name, str(episode_id))

            media_name = f"{series_title} - {season_episode} - {episode_title}"
            log_processed_media("sonarr", media_name, str(episode_id), instance_name, "missing")

            increment_stat("sonarr", "hunted")

            if command_wait_delay > 0 and command_wait_attempts > 0:
                wait_for_command(
                    api_url, api_key, api_timeout, command_id,
                    command_wait_delay, command_wait_attempts, "Episode Search", stop_check
                )
        else:
            sonarr_logger.error(f"Failed to trigger search for episode: {series_title} - {season_episode}")

    sonarr_logger.info(f"Processed {processed_count} individual missing episodes for Sonarr.")
    sonarr_logger.warning("Episodes mode processing complete - consider using Season Packs mode for better efficiency")
    return processed_any

def wait_for_command(
    api_url: str,
    api_key: str,
    api_timeout: int,
    command_id: int,
    wait_delay: int,
    max_attempts: int,
    command_name: str = "Command",
    stop_check: Callable[[], bool] = lambda: False
) -> bool:
    """Wait for a Sonarr command to complete or timeout."""
    if wait_delay <= 0 or max_attempts <= 0:
        sonarr_logger.debug(f"Not waiting for command to complete (wait_delay={wait_delay}, max_attempts={max_attempts})")
        return True

    sonarr_logger.debug(
        f"Waiting for {command_name} to complete (command ID: {command_id}). "
        f"Checking every {wait_delay}s for up to {max_attempts} attempts"
    )

    attempts = 0
    while attempts < max_attempts:
        if stop_check():
            sonarr_logger.info(f"Stopping wait for {command_name} due to stop request")
            return False

        command_status = sonarr_api.get_command_status(api_url, api_key, api_timeout, command_id)
        if not command_status:
            sonarr_logger.warning(f"Failed to get status for {command_name} (ID: {command_id}), attempt {attempts+1}")
            attempts += 1
            time.sleep(wait_delay)
            continue

        status = command_status.get('status')
        if status == 'completed':
            sonarr_logger.debug(f"Sonarr {command_name} (ID: {command_id}) completed successfully")
            return True
        elif status in ['failed', 'aborted']:
            sonarr_logger.warning(f"Sonarr {command_name} (ID: {command_id}) {status}")
            return False

        sonarr_logger.debug(f"Sonarr {command_name} (ID: {command_id}) status: {status}, attempt {attempts+1}/{max_attempts}")

        attempts += 1
        time.sleep(wait_delay)

    sonarr_logger.error(f"Sonarr command '{command_name}' (ID: {command_id}) timed out after {max_attempts} attempts.")
    return False
