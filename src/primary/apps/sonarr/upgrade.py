#!/usr/bin/env python3
"""
Sonarr cutoff upgrade processing module for Huntarr

UPDATED:
- Only searches for upgrades on series tagged "done"
"""

import time
import random
from typing import List, Dict, Any, Callable, Union, Set
from src.primary.utils.logger import get_logger
from src.primary.apps.sonarr import api as sonarr_api
from src.primary.stats_manager import increment_stat, check_hourly_cap_exceeded
from src.primary.stateful_manager import is_processed, add_processed_id
from src.primary.utils.history_utils import log_processed_media
from src.primary.settings_manager import get_advanced_setting, load_settings

sonarr_logger = get_logger("sonarr")

def _get_allowed_series_ids_for_upgrades(api_url: str, api_key: str, api_timeout: int) -> Set[int]:
    """
    Returns a set of series IDs tagged with "done".
    If tag is missing or no series match, returns empty set.
    """
    sonarr_settings = load_settings("sonarr")
    done_tag_label = sonarr_settings.get("tag_done_label", "done")

    tag_id = sonarr_api.get_tag_id_by_label(api_url, api_key, api_timeout, done_tag_label)
    if tag_id is None:
        sonarr_logger.warning(
            f"Sonarr tag '{done_tag_label}' not found. Skipping upgrade processing to avoid upgrading everything."
        )
        return set()

    allowed = sonarr_api.get_series_ids_with_tag(api_url, api_key, api_timeout, tag_id)
    if not allowed:
        sonarr_logger.info(f"No Sonarr series tagged '{done_tag_label}' found. Nothing to upgrade.")
    return allowed

def process_cutoff_upgrades(
    api_url: str,
    api_key: str,
    instance_name: str,
    api_timeout: int = get_advanced_setting("api_timeout", 120),
    monitored_only: bool = True,
    hunt_upgrade_items: int = 5,
    upgrade_mode: str = "seasons_packs",
    command_wait_delay: int = get_advanced_setting("command_wait_delay", 1),
    command_wait_attempts: int = get_advanced_setting("command_wait_attempts", 600),
    stop_check: Callable[[], bool] = lambda: False
) -> bool:
    if hunt_upgrade_items <= 0:
        sonarr_logger.info("'hunt_upgrade_items' setting is 0 or less. Skipping upgrade processing.")
        return False

    allowed_series_ids = _get_allowed_series_ids_for_upgrades(api_url, api_key, api_timeout)
    if not allowed_series_ids:
        return False

    sonarr_logger.info(
        f"Checking for {hunt_upgrade_items} quality upgrades for instance '{instance_name}' "
        f"(tag-gated to series tagged 'done')..."
    )
    sonarr_logger.info(f"Using {upgrade_mode.upper()} mode for quality upgrades")

    if upgrade_mode == "seasons_packs":
        return process_upgrade_seasons_mode(
            api_url, api_key, instance_name, api_timeout, monitored_only,
            hunt_upgrade_items, command_wait_delay, command_wait_attempts, stop_check,
            allowed_series_ids
        )
    elif upgrade_mode == "episodes":
        sonarr_logger.warning(
            "Episodes mode selected for upgrades - WARNING: This mode makes excessive API calls and does not support tagging. "
            "Consider using Season Packs mode instead."
        )
        return process_upgrade_episodes_mode(
            api_url, api_key, instance_name, api_timeout, monitored_only,
            hunt_upgrade_items, command_wait_delay, command_wait_attempts, stop_check,
            allowed_series_ids
        )
    else:
        sonarr_logger.error("Invalid upgrade_mode: Valid options are 'seasons_packs' or 'episodes'.")
        return False

def log_season_pack_upgrade(api_url: str, api_key: str, api_timeout: int, series_id: int, season_number: int, instance_name: str):
    """Log a season pack upgrade to the history."""
    try:
        series_details = sonarr_api.get_series(api_url, api_key, api_timeout, series_id)
        if series_details:
            series_title = series_details.get('title', f"Series ID {series_id}")
            try:
                season_disp = f"S{season_number:02d}" if isinstance(season_number, int) else f"S{season_number}"
            except Exception:
                season_disp = f"S{season_number}"

            season_id_num = f"{series_id}_{season_number}"
            media_name = f"{series_title} - {season_disp}"
            log_processed_media("sonarr", media_name, season_id_num, instance_name, "upgrade")
    except Exception as e:
        sonarr_logger.error(f"Failed to log season pack upgrade to history: {str(e)}")

def process_upgrade_seasons_mode(
    api_url: str,
    api_key: str,
    instance_name: str,
    api_timeout: int,
    monitored_only: bool,
    hunt_upgrade_items: int,
    command_wait_delay: int,
    command_wait_attempts: int,
    stop_check: Callable[[], bool],
    allowed_series_ids: Set[int]
) -> bool:
    """Process upgrades in season mode - groups episodes by season (tag-gated to 'done')."""
    processed_any = False

    sonarr_settings = load_settings("sonarr")
    tag_processed_items = sonarr_settings.get("tag_processed_items", True)

    skip_episode_history = True

    sample_size = hunt_upgrade_items * 10
    cutoff_unmet_episodes = sonarr_api.get_cutoff_unmet_episodes_random_page(
        api_url, api_key, api_timeout, monitored_only, sample_size
    )

    if not cutoff_unmet_episodes:
        sonarr_logger.info("No cutoff unmet episodes found in Sonarr.")
        return False

    # Tag gate (done tag)
    cutoff_unmet_episodes = [
        ep for ep in cutoff_unmet_episodes
        if int(ep.get("seriesId") or 0) in allowed_series_ids
    ]

    sonarr_logger.info(f"Received {len(cutoff_unmet_episodes)} cutoff unmet episodes from random page (after tag gating).")

    if not cutoff_unmet_episodes:
        sonarr_logger.info("No cutoff unmet episodes found for series tagged 'done'.")
        return False

    now_unix = time.time()
    original_count = len(cutoff_unmet_episodes)
    cutoff_unmet_episodes = [
        ep for ep in cutoff_unmet_episodes
        if ep.get('airDateUtc') and time.mktime(time.strptime(ep['airDateUtc'], '%Y-%m-%dT%H:%M:%SZ')) < now_unix
    ]
    skipped_count = original_count - len(cutoff_unmet_episodes)
    if skipped_count > 0:
        sonarr_logger.info(f"Skipped {skipped_count} future episodes based on air date for upgrades.")

    if stop_check():
        sonarr_logger.info("Stop requested during upgrade processing.")
        return processed_any

    series_season_episodes: Dict[int, Dict[int, List[Dict]]] = {}
    for episode in cutoff_unmet_episodes:
        series_id = episode.get('seriesId')
        season_number = episode.get('seasonNumber')
        if series_id is None or season_number is None:
            continue
        series_season_episodes.setdefault(series_id, {}).setdefault(season_number, []).append(episode)

    available_seasons = []
    for series_id, seasons in series_season_episodes.items():
        for season_number, episodes in seasons.items():
            series_title = episodes[0].get('series', {}).get('title', f"Series ID {series_id}")
            available_seasons.append((series_id, season_number, len(episodes), series_title))

    if not available_seasons:
        sonarr_logger.info("No valid seasons with cutoff unmet episodes found.")
        return False

    unprocessed_seasons = []
    for series_id, season_number, episode_count, series_title in available_seasons:
        season_id = f"{series_id}_{season_number}"
        if not is_processed("sonarr", instance_name, season_id):
            unprocessed_seasons.append((series_id, season_number, episode_count, series_title))
        else:
            sonarr_logger.debug(f"Skipping already processed season ID: {season_id} ({series_title} - Season {season_number})")

    sonarr_logger.info(f"Found {len(unprocessed_seasons)} unprocessed seasons out of {len(available_seasons)} total seasons with cutoff unmet episodes.")

    if not unprocessed_seasons:
        sonarr_logger.info("All seasons with cutoff unmet episodes have been processed.")
        return False

    random.shuffle(unprocessed_seasons)
    seasons_to_process = unprocessed_seasons[:hunt_upgrade_items]

    sonarr_logger.info(f"Selected {len(seasons_to_process)} seasons with cutoff unmet episodes to process")
    for idx, (series_id, season_number, episode_count, series_title) in enumerate(seasons_to_process):
        sonarr_logger.info(f" {idx+1}. {series_title} - Season {season_number} - {episode_count} cutoff unmet episodes")

    for series_id, season_number, episode_count, series_title in seasons_to_process:
        if stop_check():
            sonarr_logger.info("Stop requested during upgrade processing.")
            break

        try:
            if check_hourly_cap_exceeded("sonarr"):
                sonarr_logger.warning("🛑 Sonarr API hourly limit reached - stopping upgrade season processing")
                break
        except Exception as e:
            sonarr_logger.error(f"Error checking hourly API cap: {e}")

        sonarr_logger.info(f"Processing season pack upgrade: {series_title} Season {season_number} ({episode_count} cutoff unmet episodes)")

        episodes = series_season_episodes[series_id][season_number]
        episode_ids = [episode["id"] for episode in episodes]

        search_command_id = sonarr_api.search_season(api_url, api_key, api_timeout, series_id, season_number)

        if search_command_id:
            if wait_for_command(
                api_url, api_key, api_timeout, search_command_id,
                command_wait_delay, command_wait_attempts, "Episode Upgrade Search", stop_check
            ):
                processed_any = True
                sonarr_logger.info(f"Successfully triggered season pack search for {series_title} Season {season_number} with {len(episode_ids)} cutoff unmet episodes")

                if tag_processed_items:
                    from src.primary.settings_manager import get_custom_tag
                    custom_tag = get_custom_tag("sonarr", "upgrade", "huntarr-upgraded")
                    try:
                        sonarr_api.tag_processed_series(api_url, api_key, api_timeout, series_id, custom_tag)
                    except Exception as e:
                        sonarr_logger.warning(f"Failed to tag series {series_id} with '{custom_tag}': {e}")

                log_season_pack_upgrade(api_url, api_key, api_timeout, series_id, season_number, instance_name)

                season_id = f"{series_id}_{season_number}"
                add_processed_id("sonarr", instance_name, season_id)

                for episode_id in episode_ids:
                    add_processed_id("sonarr", instance_name, str(episode_id))

                    from src.primary.stats_manager import increment_stat_only
                    increment_stat_only("sonarr", "upgraded")

                    if not skip_episode_history:
                        try:
                            episode_details = sonarr_api.get_episode(api_url, api_key, api_timeout, episode_id)
                            if episode_details:
                                s_title = episode_details.get('series', {}).get('title', 'Unknown Series')
                                e_title = episode_details.get('title', 'Unknown Episode')
                                s_num = episode_details.get('seasonNumber', 'Unknown Season')
                                e_num = episode_details.get('episodeNumber', 'Unknown Episode')
                                try:
                                    season_episode = f"S{s_num:02d}E{e_num:02d}"
                                except Exception:
                                    season_episode = f"S{s_num}E{e_num}"
                                media_name = f"{s_title} - {season_episode} - {e_title}"
                                log_processed_media("sonarr", media_name, episode_id, instance_name, "upgrade")
                        except Exception as e:
                            sonarr_logger.error(f"Failed to log history for episode ID {episode_id}: {str(e)}")
            else:
                sonarr_logger.warning(f"Season pack search command for {series_title} Season {season_number} did not complete successfully")
        else:
            sonarr_logger.error(f"Failed to trigger season pack search command for {series_title} Season {season_number}")

    sonarr_logger.info("Finished quality cutoff upgrades processing cycle (season mode) for Sonarr.")
    return processed_any

def process_upgrade_episodes_mode(
    api_url: str,
    api_key: str,
    instance_name: str,
    api_timeout: int,
    monitored_only: bool,
    hunt_upgrade_items: int,
    command_wait_delay: int,
    command_wait_attempts: int,
    stop_check: Callable[[], bool],
    allowed_series_ids: Set[int]
) -> bool:
    """Process upgrades in individual episode mode (tag-gated to 'done')."""
    processed_any = False

    sonarr_logger.warning("Using Episodes mode for upgrades - This will make more API calls and does not support tagging")

    cutoff_unmet_episodes = sonarr_api.get_cutoff_unmet_episodes_random_page(
        api_url, api_key, api_timeout, monitored_only, hunt_upgrade_items * 2
    )

    if not cutoff_unmet_episodes:
        sonarr_logger.info("No cutoff unmet episodes found in Sonarr for individual processing.")
        return False

    # Tag gate (done tag)
    cutoff_unmet_episodes = [
        ep for ep in cutoff_unmet_episodes
        if int(ep.get("seriesId") or 0) in allowed_series_ids
    ]

    if not cutoff_unmet_episodes:
        sonarr_logger.info("No cutoff unmet episodes found for series tagged 'done' (episodes mode).")
        return False

    now_unix = time.time()
    original_count = len(cutoff_unmet_episodes)
    cutoff_unmet_episodes = [
        ep for ep in cutoff_unmet_episodes
        if ep.get('airDateUtc') and time.mktime(time.strptime(ep['airDateUtc'], '%Y-%m-%dT%H:%M:%SZ')) < now_unix
    ]
    skipped_count = original_count - len(cutoff_unmet_episodes)
    if skipped_count > 0:
        sonarr_logger.info(f"Skipped {skipped_count} future episodes based on air date for upgrades.")

    if stop_check():
        sonarr_logger.info("Stop requested during upgrade processing.")
        return processed_any

    unprocessed_episodes = []
    for episode in cutoff_unmet_episodes:
        episode_id = str(episode.get('id'))
        if not is_processed("sonarr", instance_name, episode_id):
            unprocessed_episodes.append(episode)

    if not unprocessed_episodes:
        sonarr_logger.info("All cutoff unmet episodes have been processed.")
        return False

    random.shuffle(unprocessed_episodes)
    episodes_to_process = unprocessed_episodes[:hunt_upgrade_items]

    processed_count = 0
    for episode in episodes_to_process:
        if stop_check():
            sonarr_logger.info("Stop requested. Aborting episode upgrade processing.")
            break

        try:
            if check_hourly_cap_exceeded("sonarr"):
                sonarr_logger.warning(f"🛑 Sonarr API hourly limit reached - stopping episode upgrade processing after {processed_count} episodes")
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

        sonarr_logger.info(f"Processing upgrade for episode: {series_title} - {season_episode} - {episode_title}")

        command_id = sonarr_api.search_episode(api_url, api_key, api_timeout, [episode_id])
        if command_id:
            if command_wait_delay > 0 and command_wait_attempts > 0:
                ok = wait_for_command(
                    api_url, api_key, api_timeout, command_id,
                    command_wait_delay, command_wait_attempts, "Episode Upgrade Search", stop_check
                )
                if not ok:
                    sonarr_logger.warning(f"Episode upgrade search command for {series_title} - {season_episode} did not complete successfully")
                    continue

            processed_any = True
            processed_count += 1

            add_processed_id("sonarr", instance_name, str(episode_id))

            media_name = f"{series_title} - {season_episode} - {episode_title}"
            log_processed_media("sonarr", media_name, str(episode_id), instance_name, "upgrade")

            increment_stat("sonarr", "upgraded")
        else:
            sonarr_logger.error(f"Failed to trigger upgrade search for episode: {series_title} - {season_episode}")

    sonarr_logger.info(f"Processed {processed_count} individual episode upgrades for Sonarr.")
    sonarr_logger.warning("Episodes mode upgrade processing complete - consider using Season Packs mode for better efficiency")
    return processed_any

def wait_for_command(
    api_url: str,
    api_key: str,
    api_timeout: int,
    command_id: Union[int, str],
    wait_delay: int,
    max_attempts: int,
    command_name: str = "Command",
    stop_check: Callable[[], bool] = lambda: False
) -> bool:
    if wait_delay <= 0 or max_attempts <= 0:
        sonarr_logger.debug(f"Not waiting for command to complete (wait_delay={wait_delay}, max_attempts={max_attempts})")
        return True

    sonarr_logger.debug(f"Waiting for {command_name} to complete (command ID: {command_id}). Checking every {wait_delay}s for up to {max_attempts} attempts")

    attempts = 0
    while attempts < max_attempts:
        if stop_check():
            sonarr_logger.info(f"Stopping wait for {command_name} due to stop request")
            return False

        command_status = sonarr_api.get_command_status(api_url, api_key, api_timeout, command_id)

        if command_status is None:
            sonarr_logger.warning(
                f"Failed to get status for {command_name} (ID: {command_id}), attempt {attempts+1}. "
                f"Command may have already completed or the ID is no longer valid."
            )
            return False

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
