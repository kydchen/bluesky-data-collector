#!/usr/bin/env python3
"""
Bluesky Comprehensive Data Collection Tool
A powerful tool for collecting comprehensive data from Bluesky using ATP API
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable
import argparse

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv('config.env')

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from src.client.atp_client import ATPClient
from src.client.parallel_collector import ParallelCollector
from src.utils.data_saver import DataSaver

# --- Configuration ---
# Read from environment variables with defaults
RATE_LIMIT_DELAY = float(os.getenv('RATE_LIMIT_DELAY', 0.1))
DATA_DIR = os.getenv('DATA_DIR', 'data')
KEYWORDS_DIR = os.path.join(DATA_DIR, "keywords")  # Renamed from SEARCH_DIR
USERS_DIR = os.path.join(DATA_DIR, "users")
USER_PROFILES_DIR = os.path.join(USERS_DIR, "profiles")  # New profiles directory
USER_POSTS_DIR = os.path.join(USERS_DIR, "posts")  # New posts directory
FEEDS_DIR = os.path.join(DATA_DIR, "feeds")  # New feeds directory
DISCOVERED_USERS_PATH = os.path.join(USERS_DIR, "discovered_users.json")

DEFAULT_KEYWORD_LIMIT = int(os.getenv('DEFAULT_KEYWORD_LIMIT', 1000))
DEFAULT_USER_POSTS_LIMIT = int(os.getenv('DEFAULT_USER_POSTS_LIMIT', 10000))

# Bluesky public launch date (February 2024)
BLUESKY_PUBLIC_LAUNCH_DATE = "2024-02-01T00:00:00Z"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BlueskyDataCollector:
    """Comprehensive Bluesky data collector with rate limiting and pagination"""
    
    def __init__(self):
        self.client = ATPClient()
        self.saver = DataSaver()
        self.rate_limit_delay = RATE_LIMIT_DELAY
        self._quoted_posts_cache = {}  # Cache for quoted posts info
    
    def _generate_post_url(self, author_handle: str, post_uri: str) -> Optional[str]:
        """
        Generate Bluesky web URL for a post.
        Format: https://bsky.app/profile/{author_handle}/post/{post_id}
        """
        try:
            if author_handle and post_uri and '/app.bsky.feed.post/' in post_uri:
                post_id = post_uri.split('/app.bsky.feed.post/')[-1]
                return f"https://bsky.app/profile/{author_handle}/post/{post_id}"
            return None
        except (TypeError, IndexError):
            return None
        
    def _get_time(self, *sources: dict, key: str) -> str:
        """Safely extract timestamp field, trying multiple common key variations from several source dicts."""
        for source in sources:
            if not isinstance(source, dict):
                continue
            # Try different variations of the key
            variations = [key, key[0].lower() + key[1:], key.lower()]
            for var in variations:
                value = source.get(var)
                if value:
                    return value
            # Also try nested access for complex structures
            if key == 'createdAt':
                # Try to get from record.created_at or post.created_at
                record = source.get('record', {})
                if isinstance(record, dict):
                    value = record.get('created_at') or record.get('createdAt')
                    if value:
                        return value
                # Try from post directly
                value = source.get('created_at') or source.get('createdAt')
                if value:
                    return value
        return ""
        
    def authenticate(self, username: Optional[str] = None, 
                          password: Optional[str] = None,
                          app_password: Optional[str] = None) -> bool:
        """Authenticate with Bluesky"""
        try:
            username = username or os.getenv('BLUESKY_USERNAME')
            password = password or os.getenv('BLUESKY_PASSWORD')
            app_password = app_password or os.getenv('BLUESKY_APP_PASSWORD')
            
            if not username or (not password and not app_password):
                logger.error("Missing credentials. Set BLUESKY_USERNAME and a password in config.env.")
                return False
            
            success = self.client.authenticate(username, password, app_password)
            if success:
                logger.info("Authentication successful!")
                return True
            else:
                logger.error("Authentication failed!")
                return False
                
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    async def collect_by_keyword(self, keyword: str, limit: int = 10000, 
                                author: Optional[str] = None, domain: Optional[str] = None,
                                lang: Optional[str] = None, mentions: Optional[str] = None,
                                tag: Optional[str] = None, url: Optional[str] = None,
                                since: Optional[str] = None, until: Optional[str] = None,
                                sort: Optional[str] = None, use_default_since: bool = True) -> bool:
        """Collect posts by keyword with deduplication, user discovery, and batch processing."""
        try:
            # Use default since date if not specified and use_default_since is True
            if use_default_since and since is None:
                since = BLUESKY_PUBLIC_LAUNCH_DATE
                logger.info(f"Using default since date: {since}")
            
            logger.info(f"Collecting posts for keyword: '{keyword}' (since: {since}, until: {until})")
            
            # File paths - support worker-specific paths for parallel collection
            if hasattr(self, '_worker_filepaths'):
                # Use worker-specific paths if provided (for parallel collection)
                search_filepath = self._worker_filepaths['search_filepath']
                temp_filepath = self._worker_filepaths['temp_filepath']
            else:
                # Use standard paths for single-threaded collection, making them unique
                # if a time range is provided to avoid conflicts between concurrent runs.
                filename_suffix = ""
                # Create a unique suffix from the time range to prevent file collisions
                # when running multiple single-threaded jobs for the same keyword.
                if since or until:
                    # Sanitize since/until for filesystem-friendly names
                    since_str = since.split('T')[0] if since else "start"
                    until_str = until.split('T')[0] if until else "end"
                    filename_suffix = f"_{since_str}_to_{until_str}"
                    
                keyword_safe = keyword.replace(' ', '_').replace('.', '_')
                search_filepath = Path(KEYWORDS_DIR) / f"search_{keyword_safe}{filename_suffix}.json"
                temp_filepath = Path(KEYWORDS_DIR) / f"search_{keyword_safe}{filename_suffix}_temp.json"
            
            # Load existing data for resume functionality
            existing_data = self._load_existing_search_data(search_filepath, temp_filepath)
            posts_data = existing_data.get('posts', [])
            topic_participants = existing_data.get('topic_participants', {})
            post_uris_seen = set(post.get('uri') for post in posts_data if post.get('uri'))
            
            # Convert topic_participants from list to dict for easier handling
            if isinstance(topic_participants, list):
                topic_participants = {p['handle']: p for p in topic_participants if p.get('handle')}
            
            cursor = existing_data.get('cursor')
            collected_count = len(posts_data)
            batch_size = 100  # Save every 100 posts (reduced from 500 for faster progress visibility)
            last_save_count = collected_count - (collected_count % batch_size)
            
            # For incremental batch saving
            save_batch_participants = {}

            logger.info(f"Resuming collection: {collected_count} posts already collected, cursor: {cursor}")
            
            while limit == 0 or collected_count < limit:
                batch_limit = min(100, limit - collected_count) if limit > 0 else 100
                await asyncio.sleep(self.rate_limit_delay)
                
                search_response = self.client.search_posts(
                    keyword, batch_limit, cursor, author, domain, lang, 
                    mentions, tag, url, since, until, sort
                )
                if not search_response or not search_response.get('posts'):
                    break
                
                batch_posts = []
                batch_participants = {}
                
                for post_data in search_response['posts']:
                    if not isinstance(post_data, dict) or not post_data.get('uri'):
                        continue
                    
                    post_uri = post_data['uri']
                    if post_uri in post_uris_seen:
                        continue  # Skip duplicate posts
                    
                    post_uris_seen.add(post_uri)
                    
                    # Create post_view structure for processing
                    post_view = {'post': post_data}
                    processed_post = await self._process_post_view_for_search(post_view, keyword)
                    if processed_post:
                        posts_data.append(processed_post)
                        batch_posts.append(processed_post)
                        
                        # Collect user information from post author and interactions
                        author_handle = processed_post.get('author_handle')
                        if author_handle:
                            if author_handle not in topic_participants:
                                topic_participants[author_handle] = {
                                    'handle': author_handle,
                                    'did': processed_post.get('author_did'),
                                    'displayName': processed_post.get('author_displayName', '')
                                }
                                batch_participants[author_handle] = topic_participants[author_handle]
                                save_batch_participants[author_handle] = topic_participants[author_handle]
                        
                        # Collect users from interactions for topic participants
                        for interaction_type in ['likes', 'reposts']:
                            interaction_data = processed_post.get(interaction_type, [])
                            for user_data in interaction_data:
                                if isinstance(user_data, dict) and user_data.get('handle'):
                                    handle = user_data['handle']
                                    if handle not in topic_participants:
                                        topic_participants[handle] = {
                                            'handle': handle,
                                            'did': user_data.get('did'),
                                            'displayName': user_data.get('displayName', '')
                                        }
                                        batch_participants[handle] = topic_participants[handle]
                                        save_batch_participants[handle] = topic_participants[handle]
                
                collected_count = len(posts_data)
                cursor = search_response.get('cursor')
                
                # Save batch if we've collected enough new posts
                if collected_count >= last_save_count + batch_size:
                    if hasattr(self, '_worker_filepaths'):
                        # Parallel worker saves incremental batches
                        new_posts_slice = posts_data[last_save_count:collected_count]
                        await self._save_search_batch(
                            keyword, new_posts_slice, save_batch_participants, cursor, 
                            search_filepath, temp_filepath, collected_count, 
                            last_save_count // batch_size + 1
                        )
                        save_batch_participants = {} # Reset for next batch
                    else:
                        # Single-threaded saves the entire accumulated data to the temp file
                        await self._save_search_batch(
                            keyword, posts_data, topic_participants, cursor, 
                            search_filepath, temp_filepath, collected_count
                        )
                    
                    last_save_count = collected_count - (collected_count % batch_size)
                    
                    # Update discovered users for this batch
                    if batch_participants:
                        participant_handles = set(batch_participants.keys())
                        await self._update_discovered_users(participant_handles, keyword)
                        logger.info(f"Added {len(participant_handles)} new participants to discovered users")
                
                if not cursor:
                    logger.info("No more pages to fetch.")
                    break
                logger.info(f"Collected {collected_count} unique posts so far for keyword '{keyword}'")

            # Final save
            await self._save_search_batch(
                keyword, posts_data, topic_participants, None, 
                search_filepath, temp_filepath, collected_count, is_final=True
            )
            
            # Final update of discovered users
            if topic_participants:
                participant_handles = set(topic_participants.keys())
                await self._update_discovered_users(participant_handles, keyword)
                logger.info(f"Final update: Added {len(participant_handles)} participants to discovered users")
            
            # Clean up temp file
            if temp_filepath.exists():
                temp_filepath.unlink()
            
            logger.info(f"Successfully saved {len(posts_data)} posts and {len(topic_participants)} participants for keyword '{keyword}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to collect posts for keyword '{keyword}': {e}", exc_info=True)
            return False

    def _load_existing_search_data(self, search_filepath: Path, temp_filepath: Path) -> dict:
        """Load existing search data for resume functionality."""
        # Check if this is a parallel worker by checking the temp file name pattern
        is_parallel_worker = '_worker_' in temp_filepath.stem and temp_filepath.stem.endswith('_temp')

        if is_parallel_worker:
            worker_stem = temp_filepath.stem.replace('_temp', '')
            batch_pattern = f"{worker_stem}_batch_*.json"
            batch_files = sorted(
                list(temp_filepath.parent.glob(batch_pattern)),
                key=lambda x: int(x.stem.split('_batch_')[-1])
            )

            if batch_files:
                logger.info(f"Found {len(batch_files)} existing batch files for worker. Resuming...")
                all_posts = []
                all_participants = {}
                last_cursor = None
                for batch_file in batch_files:
                    try:
                        with open(batch_file, 'r', encoding='utf-8') as f:
                            batch_data = json.load(f)
                        
                        posts = batch_data.get('posts', [])
                        all_posts.extend(posts)
                        
                        participants = batch_data.get('topic_participants', [])
                        for p in participants:
                            if isinstance(p, dict) and p.get('handle') and p['handle'] not in all_participants:
                                all_participants[p['handle']] = p
                        
                        cursor_in_batch = batch_data.get('cursor')
                        if cursor_in_batch:
                            last_cursor = cursor_in_batch
                    except Exception as e:
                        logger.warning(f"Could not load or process batch file {batch_file}: {e}")
                
                logger.info(f"Resumed with {len(all_posts)} posts and {len(all_participants)} participants for worker.")
                return {
                    'posts': all_posts,
                    'topic_participants': all_participants,
                    'cursor': last_cursor
                }

        # Original logic for single-threaded or if no batch files are found
        existing_data = {'posts': [], 'topic_participants': {}, 'cursor': None}
        
        # Try to load from temp file first (incomplete session) - for single-threaded
        if temp_filepath.exists() and not is_parallel_worker:
            try:
                with open(temp_filepath, 'r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                    existing_data['posts'] = temp_data.get('posts', [])
                    existing_data['topic_participants'] = temp_data.get('topic_participants', {})
                    existing_data['cursor'] = temp_data.get('cursor')
                    logger.info(f"Resumed from temp file: {len(existing_data['posts'])} posts")
                    return existing_data
            except Exception as e:
                logger.warning(f"Failed to load temp file: {e}")
        
        # Try to load from final file
        if search_filepath.exists():
            try:
                with open(search_filepath, 'r', encoding='utf-8') as f:
                    final_data = json.load(f)
                    existing_data['posts'] = final_data.get('posts', [])
                    existing_data['topic_participants'] = final_data.get('topic_participants', {})
                    logger.info(f"Resumed from final file: {len(existing_data['posts'])} posts")
                    return existing_data
            except Exception as e:
                logger.warning(f"Failed to load final file: {e}")
        
        return existing_data

    async def _save_search_batch(self, keyword: str, posts_data: list, topic_participants: dict, 
                                cursor: Optional[str], search_filepath: Path, temp_filepath: Path, 
                                collected_count: int, batch_number: int = None, is_final: bool = False):
        """Save search results in batches."""
        try:
            # For parallel collection, we need to save individual batches
            if hasattr(self, '_worker_filepaths'):
                # This is a parallel collection worker
                worker_id = self._worker_filepaths.get('worker_id', 0)
                
                # If batch_number is not provided, calculate it. This is for the final save.
                if batch_number is None:
                    batch_number = (collected_count // 100) + 1 if collected_count > 0 else 1
                
                batch_temp_filepath = temp_filepath.parent / f"{temp_filepath.stem}_batch_{batch_number}.json"
                
                # For incremental saving, `posts_data` is already the slice of new posts
                batch_posts = posts_data
                
                batch_result = {
                    "batch_metadata": {
                        "keyword": keyword,
                        "batch_number": batch_number,
                        "worker_id": worker_id,
                        "posts_in_batch": len(batch_posts),
                        "collected_at": self._get_current_time(),
                        "is_final": is_final
                    },
                    "posts": batch_posts,
                    "topic_participants": list(topic_participants.values()),
                    "cursor": cursor
                }
                
                self.saver.save_json(batch_result, batch_temp_filepath)
                logger.info(f"Worker {worker_id} Batch {batch_number}: Saved {len(batch_posts)} new posts to {batch_temp_filepath}")
                
                # If this is the final save, merge all batches for this worker
                if is_final:
                    await self._merge_worker_batches(keyword, search_filepath, temp_filepath, worker_id)
                
            else:
                # Single-threaded collection - always save all accumulated data
                result_data = {
                    "search_metadata": {
                        "keyword": keyword,
                        "total_results": len(posts_data),
                        "recursion_strategy": "original_only",
                        "collected_at": self._get_current_time(),
                        "batch_saved": True,
                        "is_final": is_final
                    },
                    "posts": posts_data,
                    "topic_participants": list(topic_participants.values()),
                    "cursor": cursor
                }
                
                # Save to temp file during collection, final file when complete
                save_path = search_filepath if is_final else temp_filepath
                self.saver.save_json(result_data, save_path)
                
                if is_final:
                    logger.info(f"Final save: {len(posts_data)} posts saved to {search_filepath}")
                else:
                    logger.info(f"Batch save: {len(posts_data)} posts saved to temp file (cursor: {cursor})")
                
        except Exception as e:
            logger.error(f"Failed to save search batch: {e}")
            raise

    async def _merge_worker_batches(self, keyword: str, search_filepath: Path, temp_filepath: Path, worker_id: int):
        """Merge all batch files for a specific worker into a single worker file"""
        try:
            # Find all batch files for this worker
            batch_pattern = f"{temp_filepath.stem}_batch_*.json"
            batch_files = sorted(
                list(temp_filepath.parent.glob(batch_pattern)),
                key=lambda x: int(x.stem.split('_batch_')[-1])
            )
            
            if not batch_files:
                logger.warning(f"No batch files found for worker {worker_id}")
                return
            
            all_posts = []
            all_participants = {}

            logger.info(f"Merging {len(batch_files)} batch files for worker {worker_id}...")

            for batch_file in batch_files:
                try:
                    with open(batch_file, 'r', encoding='utf-8') as f:
                        batch_data = json.load(f)
                    
                    all_posts.extend(batch_data.get('posts', []))
                    
                    participants_slice = batch_data.get('topic_participants', [])
                    for p in participants_slice:
                        if isinstance(p, dict) and p.get('handle') and p['handle'] not in all_participants:
                            all_participants[p['handle']] = p

                except Exception as e:
                    logger.error(f"Error processing batch file {batch_file} for merging: {e}")

            # Clean up all batch files
            for batch_file in batch_files:
                try:
                    batch_file.unlink()
                    logger.info(f"Cleaned up batch file: {batch_file}")
                except Exception as e:
                    logger.error(f"Failed to clean up batch file {batch_file}: {e}")

            # Create final worker file
            worker_result = {
                "search_metadata": {
                    "keyword": keyword,
                    "total_results": len(all_posts),
                    "recursion_strategy": "original_only",
                    "collected_at": self._get_current_time(),
                    "worker_id": worker_id,
                    "batches_processed": len(batch_files),
                    "is_worker_final": True
                },
                "posts": all_posts,
                "topic_participants": list(all_participants.values())
            }
            
            # Save worker file
            self.saver.save_json(worker_result, search_filepath)
            logger.info(f"Worker {worker_id}: Created final file with {len(all_posts)} posts from {len(batch_files)} batches")
            
        except Exception as e:
            logger.error(f"Error merging worker batches for worker {worker_id}: {e}")
            raise

    async def _merge_user_worker_batches(self, handle: str, user_filepath: Path, temp_filepath: Path, worker_id: int):
        """Merge all batch files for a specific user posts worker into a single worker file"""
        try:
            # Find all batch files for this worker
            batch_pattern = f"{temp_filepath.stem}_batch_*.json"
            batch_files = sorted(
                list(temp_filepath.parent.glob(batch_pattern)),
                key=lambda x: int(x.stem.split('_batch_')[-1])
            )
            
            if not batch_files:
                logger.warning(f"No batch files found for user worker {worker_id}")
                return
            
            all_posts = []
            logger.info(f"Merging {len(batch_files)} batch files for user worker {worker_id}...")

            for batch_file in batch_files:
                try:
                    with open(batch_file, 'r', encoding='utf-8') as f:
                        batch_data = json.load(f)
                    all_posts.extend(batch_data.get('posts', []))
                except Exception as e:
                    logger.error(f"Error processing batch file {batch_file} for merging: {e}")

            # Clean up all batch files
            for batch_file in batch_files:
                try:
                    batch_file.unlink()
                    logger.info(f"Cleaned up user batch file: {batch_file}")
                except Exception as e:
                    logger.error(f"Failed to clean up user batch file {batch_file}: {e}")
            
            # Create final worker file
            worker_result = {
                "user_metadata": {
                    "handle": handle,
                    "total_results": len(all_posts),
                    "collected_at": self._get_current_time(),
                    "worker_id": worker_id,
                    "batches_processed": len(batch_files),
                    "is_worker_final": True
                },
                "posts": all_posts
            }
            
            # Save worker file
            self.saver.save_json(worker_result, user_filepath)
            logger.info(f"User Worker {worker_id}: Created final file with {len(all_posts)} posts from {len(batch_files)} batches")
            
        except Exception as e:
            logger.error(f"Error merging user worker batches for worker {worker_id}: {e}")
            raise

    def _get_current_time(self) -> str:
        """Get current time in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()

    async def _process_post_view_for_search(self, post_view: dict, keyword: str) -> Optional[dict]:
        """Process a single post for keyword search with original-only recursion strategy."""
        # Reuse existing processing logic, but use special marker for keyword search
        processed_post = await self._process_post_view(post_view, "KEYWORD_SEARCH")
        if not processed_post:
            return None
            
        # Add keyword search specific fields
        processed_post['search_keyword'] = keyword
        return processed_post

    async def collect_user_profile(self, handle: str) -> bool:
        """Collect and save a single user's profile."""
        try:
            logger.info(f"Collecting profile for user: {handle}")
            profile = self.client.get_user_profile(handle)
            if not profile:
                logger.error(f"Profile not found or failed to fetch for user {handle}")
                return False

            user_filepath = Path(USER_PROFILES_DIR) / f"{handle.replace('.', '_')}_profile.json"
            self.saver.save_json(profile, user_filepath)
            return True
        except Exception as e:
            logger.error(f"Failed to collect profile for user {handle}: {e}")
            return False
    
    async def collect_user_posts(self, handle: str, limit: int = 10000, 
                                since: Optional[str] = None, until: Optional[str] = None, 
                                use_default_since: bool = True) -> bool:
        """Collect user posts and their full, recursive interaction trees with batch processing."""
        try:
            # Use default since date if not specified and use_default_since is True
            if use_default_since and since is None:
                since = BLUESKY_PUBLIC_LAUNCH_DATE
                logger.info(f"Using default since date: {since}")
            
            logger.info(f"Collecting posts for user: {handle} (since: {since}, until: {until})")
            
            # File paths - support worker-specific paths for parallel collection
            if hasattr(self, '_worker_filepaths'):
                 # Use worker-specific paths if provided (for parallel collection)
                user_filepath = self._worker_filepaths['search_filepath'] # Reusing key from keyword search
                temp_filepath = self._worker_filepaths['temp_filepath'] # Reusing key from keyword search
            else:
                # Add time range to filename to avoid conflicts in concurrent runs
                filename_suffix = ""
                if since or until:
                    since_str = since.split('T')[0] if since else "start"
                    until_str = until.split('T')[0] if until else "end"
                    filename_suffix = f"_{since_str}_to_{until_str}"
                    
                handle_safe = handle.replace('.', '_')
                user_filepath = Path(USER_POSTS_DIR) / f"{handle_safe}{filename_suffix}_posts.json"
                temp_filepath = Path(USER_POSTS_DIR) / f"{handle_safe}{filename_suffix}_posts_temp.json"
            
            # Load existing data for resume functionality
            existing_data = self._load_existing_user_data(user_filepath, temp_filepath)
            posts_data = existing_data.get('posts', [])
            cursor = existing_data.get('cursor')
            collected_count = len(posts_data)
            batch_size = 100  # Save every 100 posts (reduced from 500 for faster progress visibility)
            last_save_count = collected_count - (collected_count % batch_size)
            
            logger.info(f"Resuming user collection: {collected_count} posts already collected, cursor: {cursor}")
            
            while limit == 0 or collected_count < limit:
                batch_limit = min(100, limit - collected_count) if limit > 0 else 100
                await asyncio.sleep(self.rate_limit_delay)
                feed = self.client.get_user_feed(handle, batch_limit, cursor, since, until)
                if not feed or not feed.get('feed'):
                    break
                
                batch_posts = []
                for post_view in feed['feed']:
                    post_data = await self._process_post_view(post_view, handle)
                    if post_data: # Filter out malformed posts
                        posts_data.append(post_data)
                        batch_posts.append(post_data)

                collected_count = len(posts_data)
                cursor = feed.get('cursor')
                
                # Save batch if we've collected enough new posts
                if collected_count >= last_save_count + batch_size:
                    if hasattr(self, '_worker_filepaths'):
                        # Parallel worker saves incremental batches
                        new_posts_slice = posts_data[last_save_count:collected_count]
                        await self._save_user_batch(
                            handle, new_posts_slice, cursor, user_filepath, temp_filepath, 
                            collected_count, last_save_count // batch_size + 1
                        )
                    else:
                        # Single-threaded saves the entire accumulated data to the temp file
                        await self._save_user_batch(
                            handle, posts_data, cursor, user_filepath, temp_filepath, 
                            collected_count
                        )
                    last_save_count = collected_count - (collected_count % batch_size)
                
                if not cursor:
                    logger.info("No more pages to fetch.")
                    break
                logger.info(f"Collected {collected_count} posts so far for user {handle}")

            # Final save
            await self._save_user_batch(
                handle, posts_data, None, user_filepath, temp_filepath, collected_count, is_final=True
            )
            
            # Extract and update discovered users
            all_handles = self._extract_all_handles(posts_data, primary_handle=handle)
            await self._update_discovered_users(all_handles)
            
            # Clean up temp file
            if temp_filepath.exists():
                temp_filepath.unlink()
            
            return True
        except Exception as e:
            logger.error(f"Failed to collect posts for user {handle}: {e}", exc_info=True)
            return False

    async def _process_post_view(self, post_view: dict, current_user_handle: str = None, *, recursion_seen_uris: Optional[set] = None) -> Optional[dict]:
        """Process a single post/reply/quote to extract its full, recursive interaction tree."""
        # Check if post_view is None or not a dict
        if not isinstance(post_view, dict):
            logger.warning(f"Skipping feed item with invalid post_view: {post_view}")
            return None
            
        post = post_view.get('post')
        if not isinstance(post, dict) or not post.get('uri'):
            logger.warning(f"Skipping feed item with missing or invalid post object: {post_view}")
            return None

        author = post.get('author', {})
        record = post.get('record', {})
        uri, cid = post['uri'], post['cid']
        
        # Avoid infinite loops during recursion by tracking seen URIs
        if recursion_seen_uris and uri in recursion_seen_uris:
            logger.info(f"Skipping post {uri} to avoid recursive cycle.")
            return None
        
        # Initialize or copy the set for the current recursion path
        current_seen_uris = recursion_seen_uris.copy() if recursion_seen_uris else set()
        current_seen_uris.add(uri)
        
        post_author_handle = author.get('handle')
        
        # Extract relationship data - this reflects how the current user interacted with this post
        is_repost, is_quote, is_reply, original_post_uri, original_post_author, parent_uri, root_uri = await self._extract_post_relations(post_view, record)
        
        # Determine if this is an original post by the current user (not a repost, quote, or reply)
        is_original_by_current_user = (not (is_repost or is_quote or is_reply) and 
                                     current_user_handle and 
                                     post_author_handle == current_user_handle)
        
        # Determine the data collection strategy based on the context
        should_get_full_interactions = False
        should_get_basic_interactions = False
        
        is_original_post = not (is_reply or is_quote or is_repost)

        if current_user_handle == "RECURSIVE_COLLECTION":
            # This is a recursive call for a reply/quote. Get its full interaction tree.
            # Cycle detection is handled by the `recursion_seen_uris` parameter.
            should_get_full_interactions = True
        
        elif current_user_handle == "KEYWORD_SEARCH" or current_user_handle == "FEED_COLLECTION":
            # For keyword/feed search, apply the "original_only" strategy.
            if is_original_post:
                # Original posts get the full recursive treatment.
                should_get_full_interactions = True
            else:
                # Replies/quotes/reposts found in search results only get basic data (user lists for discovery).
                # But they should NOT trigger recursive collection of their own replies/quotes.
                should_get_basic_interactions = True

        elif current_user_handle: # This implies it's a specific user's feed collection
            if is_original_by_current_user:
                 # An original post by the user whose feed we are collecting gets the full treatment.
                should_get_full_interactions = True
            # For all other posts in a user's feed (replies, quotes, reposts), we only get counts,
            # so both flags remain False, and the code will fall through to the final `else` block.
        
        if should_get_full_interactions:
            # Get full interaction tree for user's own original posts or recursive calls
            
            # Likes and Reposts: Get all users who liked/reposted (no recursion needed)
            likes_data = await self._get_interaction_data(self.client.get_post_likes, uri, cid, 'likes')
            reposts_data = await self._get_interaction_data(self.client.get_post_reposts, uri, cid, 'reposted_by')
            
            # Quotes: Get all quotes with their full interaction trees (recursive)
            quotes_data = await self._get_quotes_recursively(uri, cid, recursion_seen_uris=current_seen_uris)
            
            # Replies: Get all replies with their full interaction trees (recursive)
            replies_data = await self._get_replies(uri, recursion_seen_uris=current_seen_uris)
        
        elif should_get_basic_interactions:
            # For non-original posts in keyword/feed searches, get basic interaction data (user lists and counts)
            try:
                await asyncio.sleep(self.rate_limit_delay)
                # Get basic interaction data for user discovery
                likes_response = self.client.get_post_likes(uri=uri, cid=cid, limit=100)
                reposts_response = self.client.get_post_reposts(uri=uri, cid=cid, limit=100)
                quotes_response = self.client.get_post_quotes(uri=uri, cid=cid, limit=100)
                
                # For non-original posts in searches, we only collect user handles for discovery
                # but we DON'T store detailed user information in the likes/reposts arrays
                discovered_users = set()
                
                if likes_response and likes_response.get('likes'):
                    for like_data in likes_response['likes']:
                        if isinstance(like_data, dict) and like_data.get('actor', {}).get('handle'):
                            discovered_users.add(like_data['actor']['handle'])
                
                if reposts_response and reposts_response.get('reposted_by'):
                    for repost_data in reposts_response['reposted_by']:
                        if isinstance(repost_data, dict) and repost_data.get('actor', {}).get('handle'):
                            discovered_users.add(repost_data['actor']['handle'])
                
                # Store discovered users for later batch processing, but don't include detailed user data
                # in the post's likes/reposts arrays
                if discovered_users:
                    await self._update_discovered_users(discovered_users)
                
                # For non-original posts in searches, we NEVER store detailed interaction data
                likes_data = []
                reposts_data = []
                quotes_data = []
                replies_data = []
                
                # Get counts only
                likes_count = len(likes_response.get('likes', [])) if likes_response else 0
                reposts_count = len(reposts_response.get('reposted_by', [])) if reposts_response else 0
                quotes_count = len(quotes_response.get('posts', [])) if quotes_response else 0
                
                # Get reply count from thread (but not the actual replies)
                try:
                    thread_response = self.client.get_post_thread(uri, depth=1)  # Only get immediate replies count
                    if thread_response and thread_response.get('thread'):
                        thread = thread_response['thread']
                        replies_count = len(thread.get('replies', [])) if thread else 0
                    else:
                        replies_count = 0
                except Exception as e:
                    logger.warning(f"Failed to get reply count for {uri}: {e}")
                    replies_count = 0
                    
            except Exception as e:
                logger.warning(f"Failed to get basic interaction data for post {uri}: {e}")
                likes_count = reposts_count = quotes_count = replies_count = 0
                likes_data = reposts_data = quotes_data = replies_data = []
        else:
            # For all other cases (reposts, quotes, replies, or other users' posts), only get basic info
            try:
                await asyncio.sleep(self.rate_limit_delay)
                # Get minimal interaction data to avoid redundancy
                likes_response = self.client.get_post_likes(uri=uri, cid=cid, limit=100)
                reposts_response = self.client.get_post_reposts(uri=uri, cid=cid, limit=100)
                quotes_response = self.client.get_post_quotes(uri=uri, cid=cid, limit=100)
                
                # Get counts from response headers or metadata
                likes_count = len(likes_response.get('likes', [])) if likes_response else 0
                reposts_count = len(reposts_response.get('reposted_by', [])) if reposts_response else 0
                quotes_count = len(quotes_response.get('posts', [])) if quotes_response else 0
                
                # For non-user-own posts, we don't need detailed interaction data
                likes_data = []
                reposts_data = []
                quotes_data = []
                replies_data = []
                
                # Get reply count from thread
                try:
                    thread_response = self.client.get_post_thread(uri, depth=10)
                    if thread_response and thread_response.get('thread'):
                        thread = thread_response['thread']
                        replies_count = len(thread.get('replies', [])) if thread else 0
                    else:
                        replies_count = 0
                except Exception as e:
                    logger.warning(f"Failed to get reply count for {uri}: {e}")
                    replies_count = 0
                    
            except Exception as e:
                logger.warning(f"Failed to get counts for post {uri}: {e}")
                likes_count = reposts_count = quotes_count = replies_count = 0
                likes_data = reposts_data = quotes_data = replies_data = []

        # Add quoted post info if this is a quote
        quoted_post_info = None
        if is_quote and original_post_uri:
            if original_post_uri in self._quoted_posts_cache:
                quoted_post_info = self._quoted_posts_cache[original_post_uri]
            else:
                # Try to fetch quoted post info if not in cache
                try:
                    await asyncio.sleep(self.rate_limit_delay)
                    post_info = self.client.get_posts([original_post_uri])
                    if post_info and post_info.get('posts'):
                        original_post = post_info['posts'][0]
                        quoted_post_info = {
                            'author_handle': original_post.get('author', {}).get('handle'),
                            'author_did': original_post.get('author', {}).get('did'),
                            'text': original_post.get('record', {}).get('text', ''),
                            'created_at': self._get_time(original_post.get('record', {}), original_post, key='createdAt'),
                            'uri': original_post_uri
                        }
                        self._quoted_posts_cache[original_post_uri] = quoted_post_info
                except Exception as e:
                    logger.warning(f"Could not fetch quoted post info for {original_post_uri}: {e}")

        # Extract rich media content (images, etc.)
        # Simplified: only keep text field, skip complex rich media extraction
        # rich_media = self._extract_rich_media(record, post)

        return {
            'uri': uri, 'cid': cid,
            'author_did': author.get('did'), 'author_handle': author.get('handle'),
            'author_displayName': author.get('displayName', author.get('display_name', '')),  # Try both field names
            'text': record.get('text', ''),
            # 'rich_media': rich_media,  # Removed for simplicity
            'url': self._generate_post_url(author.get('handle'), uri),
            'created_at': self._get_time(record, post, key='createdAt'),
            'indexed_at': self._get_time(post, record, key='indexedAt'),
            'reply_count': len(replies_data) if should_get_full_interactions else replies_count,
            'repost_count': len(reposts_data) if should_get_full_interactions else reposts_count,
            'like_count': len(likes_data) if should_get_full_interactions else likes_count,
            'quote_count': len(quotes_data) if should_get_full_interactions else quotes_count,
            'is_reply': is_reply, 'is_repost': is_repost, 'is_quote': is_quote,
            'parent_uri': parent_uri, 'root_uri': root_uri,
            'original_post_uri': original_post_uri,
            'original_post_author': original_post_author,
            'quoted_post_info': quoted_post_info,  # Add quoted post details
            'likes': likes_data, 'reposts': reposts_data,
            'replies': replies_data, 'quotes': quotes_data,
            'interaction_type': current_user_handle
        }

    def _extract_rich_media(self, record: dict, post: dict) -> dict:
        """Extract rich media content like images, videos, etc."""
        rich_media = {
            'images': [],
            'videos': [],
            'links': [],
            'mentions': [],
            'hashtags': []
        }
        
        # Extract images
        embed = record.get('embed', {})
        if isinstance(embed, dict):
            if embed.get('$type') == 'app.bsky.embed.images':
                images = embed.get('images', [])
                for img in images:
                    if isinstance(img, dict):
                        rich_media['images'].append({
                            'alt': img.get('alt', ''),
                            'image': img.get('image', ''),
                            'aspect_ratio': img.get('aspectRatio', {})
                        })
            
            # Extract external links
            elif embed.get('$type') == 'app.bsky.embed.external':
                external = embed.get('external', {})
                if isinstance(external, dict):
                    rich_media['links'].append({
                        'title': external.get('title', ''),
                        'description': external.get('description', ''),
                        'uri': external.get('uri', ''),
                        'thumb': external.get('thumb', '')
                    })
        
        # Extract mentions and hashtags from facets
        facets = record.get('facets')
        if facets and isinstance(facets, list):  # Add safety check
            for facet in facets:
                if isinstance(facet, dict):
                    features = facet.get('features', [])
                    for feature in features:
                        if isinstance(feature, dict):
                            if feature.get('$type') == 'app.bsky.richtext.facet#mention':
                                rich_media['mentions'].append({
                                    'did': feature.get('did', ''),
                                    'handle': feature.get('handle', '')
                                })
                            elif feature.get('$type') == 'app.bsky.richtext.facet#tag':
                                rich_media['hashtags'].append(feature.get('tag', ''))
        
        return rich_media

    async def _extract_post_relations(self, post_view: dict, record: dict) -> tuple:
        """
        Extracts relationship data (repost, quote, reply) from a post.
        
        These fields reflect how the current user (whose feed we're collecting) 
        interacted with this post, not the original post's properties.
        
        Returns:
            is_repost: True if current user reposted this content
            is_quote: True if current user quoted this content  
            is_reply: True if current user replied to this content
            original_post_uri: URI of the original post being reposted/quoted
            original_post_author: Handle of the original post author
            parent_uri: URI of the parent post (for replies)
            root_uri: URI of the root post (for replies)
        """
        is_repost, is_quote, is_reply = False, False, False
        original_post_uri, original_post_author, parent_uri, root_uri = None, None, None, None

        # Check if post_view is None or not a dict
        if not isinstance(post_view, dict):
            logger.warning(f"post_view is not a dict: {type(post_view)} - {post_view}")
            return is_repost, is_quote, is_reply, original_post_uri, original_post_author, parent_uri, root_uri

        # Additional safety check
        if post_view is None:
            logger.warning("post_view is None")
            return is_repost, is_quote, is_reply, original_post_uri, original_post_author, parent_uri, root_uri

        # Check for repost first (reason field in post_view)
        reason = post_view.get('reason')
        if reason and isinstance(reason, dict):
            if reason.get('py_type') == 'app.bsky.feed.defs#reasonRepost':
                is_repost = True
                # For reposts, the original post is the one being reposted
                # The current post_view contains the original post
                post_in_view = post_view.get('post', {})
                original_post_uri = post_in_view.get('uri')
                original_post_author = post_in_view.get('author', {}).get('handle')
                # Don't check for quote if this is already a repost
                # The user's action is repost, not quote
                return is_repost, is_quote, is_reply, original_post_uri, original_post_author, parent_uri, root_uri
        
        # Check for quote (embed field in record) - only if not a repost
        embed = record.get('embed', {})
        if isinstance(embed, dict):
            # Check for simple quote (app.bsky.embed.record)
            if embed.get('py_type') == 'app.bsky.embed.record':
                is_quote = True
                record_embed = embed.get('record', {})
                original_post_uri = record_embed.get('uri')
                original_post_author = record_embed.get('author', {}).get('handle')
                
                # If we don't have author info, try to fetch the original post
                if not original_post_author and original_post_uri:
                    try:
                        await asyncio.sleep(self.rate_limit_delay)
                        post_info = self.client.get_posts([original_post_uri])
                        if post_info and post_info.get('posts'):
                            original_post = post_info['posts'][0]
                            original_post_author = original_post.get('author', {}).get('handle')
                            # Store additional info about the quoted post
                            self._quoted_posts_cache[original_post_uri] = {
                                'author_handle': original_post_author,
                                'author_did': original_post.get('author', {}).get('did'),
                                'text': original_post.get('record', {}).get('text', ''),
                                'created_at': self._get_time(original_post.get('record', {}), original_post, key='createdAt'),
                                'uri': original_post_uri
                            }
                    except Exception as e:
                        logger.warning(f"Could not fetch author for quoted post {original_post_uri}: {e}")
            
            # Check for quote with media (app.bsky.embed.recordWithMedia)
            elif embed.get('py_type') == 'app.bsky.embed.recordWithMedia':
                is_quote = True
                record_embed = embed.get('record', {})
                if isinstance(record_embed, dict):
                    record_ref = record_embed.get('record', {})
                    original_post_uri = record_ref.get('uri')
                    # For recordWithMedia, we need to fetch the original post to get author info
                    if original_post_uri:
                        try:
                            await asyncio.sleep(self.rate_limit_delay)
                            post_info = self.client.get_posts([original_post_uri])
                            if post_info and post_info.get('posts'):
                                original_post = post_info['posts'][0]
                                original_post_author = original_post.get('author', {}).get('handle')
                                # Store additional info about the quoted post
                                self._quoted_posts_cache[original_post_uri] = {
                                    'author_handle': original_post_author,
                                    'author_did': original_post.get('author', {}).get('did'),
                                    'text': original_post.get('record', {}).get('text', ''),
                                    'created_at': self._get_time(original_post.get('record', {}), original_post, key='createdAt'),
                                    'uri': original_post_uri
                                }
                        except Exception as e:
                            logger.warning(f"Could not fetch author for quoted post with media {original_post_uri}: {e}")
        
        # Check for reply (reply field in record)
        reply_field = record.get('reply')
        if isinstance(record, dict) and reply_field:
            is_reply = True
            reply_ref = record.get('reply', {})
            parent_uri = reply_ref.get('parent', {}).get('uri')
            root_uri = reply_ref.get('root', {}).get('uri')

        return is_repost, is_quote, is_reply, original_post_uri, original_post_author, parent_uri, root_uri

    async def _get_interaction_data(self, api_method: Callable, uri: str, cid: str, data_key: str, processor: Optional[Callable] = None) -> list:
        """Generic method to fetch and process paginated interaction data."""
        items = []
        cursor = None
        while True:
            try:
                await asyncio.sleep(self.rate_limit_delay)
                response = api_method(uri=uri, cid=cid, limit=100, cursor=cursor)
                if not response or not isinstance(response, dict) or not response.get(data_key):
                    break
                
                data_list = response.get(data_key, [])
                if processor:
                    for item_data in data_list:
                        processed_item = await processor(item_data)
                        if processed_item: items.append(processed_item)
                else:
                    # Process user data to keep only essential fields
                    for item_data in data_list:
                        processed_item = self._process_user_data(item_data, data_key)
                        if processed_item: items.append(processed_item)
                
                cursor = response.get('cursor')
                if not cursor or not data_list:
                    break
            except Exception as e:
                logger.warning(f"API call for {api_method.__name__} on {uri} failed: {e}")
                break
        return items

    def _process_user_data(self, user_data: dict, interaction_type: str) -> dict:
        """Process user data for interactions, keeping only essential fields."""
        if not isinstance(user_data, dict):
            return {}
        
        # Handle different API response structures
        # For likes/reposts, user data is in 'actor' field
        if 'actor' in user_data:
            actor = user_data['actor']
            return {
                'did': actor.get('did'),
                'handle': actor.get('handle'),
                'displayName': actor.get('display_name'),  # Note: API uses display_name, not displayName
                'indexedAt': actor.get('indexed_at'),      # Note: API uses indexed_at, not indexedAt
                'labels': actor.get('labels', []),
                'interaction_type': interaction_type
            }
        # For direct user data (fallback)
        else:
            return {
                'did': user_data.get('did'),
                'handle': user_data.get('handle'),
                'displayName': user_data.get('display_name') or user_data.get('displayName'),
                'indexedAt': user_data.get('indexed_at') or user_data.get('indexedAt'),
                'labels': user_data.get('labels', []),
                'interaction_type': interaction_type
            }

    async def _get_quotes_recursively(self, uri: str, cid: str, *, recursion_seen_uris: Optional[set] = None) -> list:
        """Get all quotes for a post and process them recursively with full interaction trees."""
        quotes = []
        cursor = None
        while True:
            try:
                await asyncio.sleep(self.rate_limit_delay)
                response = self.client.get_post_quotes(uri=uri, cid=cid, limit=100, cursor=cursor)
                if not response or not isinstance(response, dict) or not response.get('posts'):
                    break
                
                posts_list = response.get('posts', [])
                for post_data in posts_list:
                    if isinstance(post_data, dict):
                        # Create a post_view structure for _process_post_view
                        post_view = {'post': post_data}
                        # Process quote with full interaction tree (recursive)
                        # Pass a special flag to indicate this is a quote/reply that should get full interactions
                        processed_quote = await self._process_post_view(post_view, "RECURSIVE_COLLECTION", recursion_seen_uris=recursion_seen_uris)
                        if processed_quote:
                            quotes.append(processed_quote)
                
                cursor = response.get('cursor')
                if not cursor or not posts_list:
                    break
            except Exception as e:
                logger.warning(f"Failed to get quotes for {uri}: {e}")
                break
        return quotes

    async def _get_replies(self, uri: str, *, recursion_seen_uris: Optional[set] = None) -> list:
        """Get all replies for a given post uri by fetching the thread with full interaction trees."""
        if not uri: return []
        try:
            await asyncio.sleep(self.rate_limit_delay)
            thread = self.client.get_post_thread(uri, depth=10)
            if thread and isinstance(thread, dict) and thread.get('thread'):
                return await self._extract_replies_from_thread(thread['thread'], recursion_seen_uris=recursion_seen_uris)
        except Exception as e:
            logger.warning(f"Failed to get reply thread for {uri}: {e}")
        return []

    async def _extract_replies_from_thread(self, thread_data: dict, *, recursion_seen_uris: Optional[set] = None) -> list:
        """Recursively extract replies from a thread structure with full interaction trees."""
        replies = []
        if isinstance(thread_data, dict) and 'replies' in thread_data:
            for reply_view in thread_data.get('replies', []):
                if isinstance(reply_view, dict) and 'post' in reply_view:
                    # Process reply with full interaction tree (recursive)
                    # Pass a special flag to indicate this is a quote/reply that should get full interactions
                    reply_data = await self._process_post_view(reply_view, "RECURSIVE_COLLECTION", recursion_seen_uris=recursion_seen_uris)
                    if reply_data: 
                        replies.append(reply_data)
        return replies

    async def _update_discovered_users(self, discovered_users: set, keyword: Optional[str] = None):
        """Update discovered users with thread-safe file handling and atomic operations."""
        if not discovered_users:
            return
        
        # Always update the global discovered_users.json
        # Use asyncio.to_thread to handle file I/O in a thread pool
        # This prevents blocking the event loop and provides better concurrency handling
        await asyncio.to_thread(self._atomic_update_discovered_users, discovered_users, Path(DISCOVERED_USERS_PATH))
        
        # Also update keyword-specific discovered_users file if keyword is provided
        if keyword:
            keyword_file_path = Path(USERS_DIR) / f"discovered_users_{keyword}.json"
            await asyncio.to_thread(self._atomic_update_discovered_users, discovered_users, keyword_file_path)

    def _safe_read_discovered_users(self) -> Optional[List[str]]:
        """Safely read discovered users file with file locking."""
        import fcntl
        
        filepath = Path(DISCOVERED_USERS_PATH)
        if not filepath.exists():
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                # Acquire shared lock for reading
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    content = f.read()
                    if content:
                        return json.loads(content)
                    return []
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(f"Could not read or parse discovered users file at {filepath}: {e}")
            return None

    def _atomic_update_discovered_users(self, new_users: set, filepath: Path):
        """Atomically update discovered users file with file locking."""
        import fcntl
        import tempfile
        import shutil
        
        if not new_users:
            return

        # Create a temporary file for atomic write
        temp_filepath = filepath.with_suffix('.tmp')
        
        try:
            # Read existing users
            existing_users: set = set()
            if filepath.exists():
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        # Acquire shared lock for reading
                        fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                        try:
                            content = f.read()
                            if content:
                                existing_users.update(json.loads(content))
                        finally:
                            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                except (IOError, json.JSONDecodeError) as e:
                    logger.warning(f"Could not read or parse existing discovered users file at {filepath}: {e}. Starting fresh.")
            
            # Calculate new users
            new_users_to_add = new_users - existing_users
            if not new_users_to_add:
                logger.info(f"No new users to add to the discovered list. Total remains {len(existing_users)}.")
                return

            updated_users = existing_users.union(new_users)
            
            # Write to temporary file first
            try:
                temp_filepath.parent.mkdir(parents=True, exist_ok=True)
                with open(temp_filepath, 'w', encoding='utf-8') as f:
                    # Acquire exclusive lock for writing
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    try:
                        json.dump(sorted(list(updated_users)), f, ensure_ascii=False, indent=2)
                        f.flush()  # Ensure data is written to disk
                        os.fsync(f.fileno())  # Force sync to disk
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                
                # Atomic move: rename temp file to target file
                # This is atomic on most filesystems
                shutil.move(str(temp_filepath), str(filepath))
                
                logger.info(f"Updated discovered users file with {len(new_users_to_add)} new users. Total: {len(updated_users)}")
                
            except Exception as e:
                # Clean up temp file if something went wrong
                if temp_filepath.exists():
                    temp_filepath.unlink()
                raise e
                
        except Exception as e:
            logger.error(f"Error saving discovered users to {filepath}: {e}")
            # Clean up temp file if it exists
            if temp_filepath.exists():
                temp_filepath.unlink()
            raise

    def _extract_all_handles(self, data: Any, primary_handle: Optional[str] = None) -> set:
        """Recursively extracts all user handles from collected data."""
        handles = set()
        if isinstance(data, dict):
            for key, value in data.items():
                if key in ('author_handle', 'actor_handle', 'original_post_author') and isinstance(value, str) and value != primary_handle:
                    if value: handles.add(value)
                elif isinstance(value, (dict, list)):
                    handles.update(self._extract_all_handles(value, primary_handle))
        elif isinstance(data, list):
            for item in data:
                handles.update(self._extract_all_handles(item, primary_handle))
        return handles

    # --- Feed Discovery and Collection Methods ---
    
    async def collect_suggested_feeds(self, limit: int = 100) -> bool:
        """Collect suggested feeds for discovery"""
        try:
            logger.info(f"Collecting suggested feeds (limit: {limit})")
            
            # Use the new method to get all available feeds
            response = self.client.get_all_suggested_feeds(limit)
            if not response or not response.get('feeds'):
                logger.error("Failed to get suggested feeds")
                return False
            
            feeds_data = response['feeds']
            api_calls_made = response.get('api_calls_made', 0)
            
            logger.info(f"Made {api_calls_made} API calls to collect feeds")

            # Save suggested feeds
            feeds_filepath = Path(FEEDS_DIR) / "suggested_feeds.json"
            self.saver.save_json(feeds_data, feeds_filepath)
            logger.info(f"Successfully saved {len(feeds_data)} suggested feeds")
            
            return True
        except Exception as e:
            logger.error(f"Failed to collect suggested feeds: {e}", exc_info=True)
            return False
    
    async def collect_feed_content(self, feed_uri: str, limit: int = 1000) -> bool:
        """Collect content from a specific feed with batch processing"""
        try:
            logger.info(f"Collecting content from feed: {feed_uri}")
            
            # File paths
            feed_name = feed_uri.replace('at://', '').replace('/', '_').replace('.', '_')
            feed_filepath = Path(FEEDS_DIR) / f"feed_{feed_name}.json"
            temp_filepath = Path(FEEDS_DIR) / f"feed_{feed_name}_temp.json"
            
            # Load existing data for resume functionality
            existing_data = self._load_existing_feed_data(feed_filepath, temp_filepath)
            posts_data = existing_data.get('posts', [])
            feed_participants = existing_data.get('feed_participants', {})
            post_uris_seen = set(post.get('uri') for post in posts_data if post.get('uri'))
            
            # Convert feed_participants from list to dict for easier handling
            if isinstance(feed_participants, list):
                feed_participants = {p['handle']: p for p in feed_participants if p.get('handle')}
            
            cursor = existing_data.get('cursor')
            collected_count = len(posts_data)
            batch_size = 100  # Save every 100 posts (reduced from 500 for faster progress visibility)
            last_save_count = collected_count - (collected_count % batch_size)
            
            logger.info(f"Resuming feed collection: {collected_count} posts already collected, cursor: {cursor}")
            
            while limit == 0 or collected_count < limit:
                batch_limit = min(100, limit - collected_count) if limit > 0 else 100
                await asyncio.sleep(self.rate_limit_delay)
                
                feed_response = self.client.get_feed(feed_uri, batch_limit, cursor)
                if not feed_response or not feed_response.get('feed'):
                    break
                
                batch_posts = []
                batch_participants = {}
                
                for post_view in feed_response['feed']:
                    if not isinstance(post_view, dict) or not post_view.get('post'):
                        continue
                    
                    post_uri = post_view['post'].get('uri')
                    if not post_uri or post_uri in post_uris_seen:
                        continue  # Skip duplicate posts
                    
                    post_uris_seen.add(post_uri)
                    
                    # Process post with feed context
                    processed_post = await self._process_post_view_for_feed(post_view, feed_uri)
                    if processed_post:
                        posts_data.append(processed_post)
                        batch_posts.append(processed_post)
                        
                        # Collect user information from post author and interactions
                        author_handle = processed_post.get('author_handle')
                        if author_handle:
                            if author_handle not in feed_participants:
                                feed_participants[author_handle] = {
                                    'handle': author_handle,
                                    'did': processed_post.get('author_did'),
                                    'displayName': processed_post.get('author_displayName', '')
                                }
                                batch_participants[author_handle] = feed_participants[author_handle]
                        
                        # Collect users from interactions for feed participants
                        for interaction_type in ['likes', 'reposts']:
                            interaction_data = processed_post.get(interaction_type, [])
                            for user_data in interaction_data:
                                if isinstance(user_data, dict) and user_data.get('handle'):
                                    handle = user_data['handle']
                                    if handle not in feed_participants:
                                        feed_participants[handle] = {
                                            'handle': handle,
                                            'did': user_data.get('did'),
                                            'displayName': user_data.get('displayName', '')
                                        }
                                        batch_participants[handle] = feed_participants[handle]
                
                collected_count = len(posts_data)
                cursor = feed_response.get('cursor')
                
                # Save batch if we've collected enough new posts
                if collected_count >= last_save_count + batch_size:
                    if hasattr(self, '_worker_filepaths'):
                        # Parallel worker saves incremental batches
                        new_posts_slice = posts_data[last_save_count:collected_count]
                        await self._save_feed_batch(
                            feed_uri, new_posts_slice, feed_participants, cursor,
                            feed_filepath, temp_filepath, collected_count
                        )
                    else:
                        # Single-threaded saves the entire accumulated data to the temp file
                        await self._save_feed_batch(
                            feed_uri, posts_data, feed_participants, cursor,
                            feed_filepath, temp_filepath, collected_count
                        )
                    last_save_count = collected_count - (collected_count % batch_size)
                    
                    # Update discovered users for this batch
                    if batch_participants:
                        participant_handles = set(batch_participants.keys())
                        await self._update_discovered_users(participant_handles)
                        logger.info(f"Added {len(participant_handles)} new participants to discovered users")
                
                if not cursor:
                    logger.info("No more pages to fetch.")
                    break
                logger.info(f"Collected {collected_count} unique posts so far from feed '{feed_uri}'")

            # Final save
            await self._save_feed_batch(
                feed_uri, posts_data, feed_participants, None,
                feed_filepath, temp_filepath, collected_count, is_final=True
            )
            
            # Final update of discovered users
            if feed_participants:
                participant_handles = set(feed_participants.keys())
                await self._update_discovered_users(participant_handles)
                logger.info(f"Final update: Added {len(participant_handles)} participants to discovered users")
            
            # Clean up temp file
            if temp_filepath.exists():
                temp_filepath.unlink()
            
            logger.info(f"Successfully saved {len(posts_data)} posts and {len(feed_participants)} participants from feed '{feed_uri}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to collect content from feed '{feed_uri}': {e}", exc_info=True)
            return False
    
    async def collect_all_feeds_content(self, feeds_limit: int = 50, posts_per_feed: int = 500) -> bool:
        """Collect content from all suggested feeds"""
        try:
            logger.info(f"Starting comprehensive feed collection (feeds: {feeds_limit}, posts per feed: {posts_per_feed})")
            
            # First, collect suggested feeds
            suggested_feeds = await self.collect_suggested_feeds(feeds_limit)
            if not suggested_feeds:
                logger.error("Failed to collect suggested feeds")
                return False
            
            # Read the collected feeds
            feeds_filepath = Path(FEEDS_DIR) / "suggested_feeds.json"
            if not feeds_filepath.exists():
                logger.error("Suggested feeds file not found")
                return False
            
            with open(feeds_filepath, 'r', encoding='utf-8') as f:
                feeds_data = json.load(f)
            
            # Collect content from each feed
            successful_feeds = 0
            total_feeds = len(feeds_data)
            
            for i, feed_data in enumerate(feeds_data, 1):
                if isinstance(feed_data, dict):
                    feed_uri = feed_data.get('uri')
                    if feed_uri:
                        logger.info(f"Processing feed {i}/{total_feeds}: {feed_uri}")
                        success = await self.collect_feed_content(feed_uri, posts_per_feed)
                        if success:
                            successful_feeds += 1
                        await asyncio.sleep(self.rate_limit_delay * 2)  # Extra delay between feeds
            
            logger.info(f"Feed collection completed: {successful_feeds}/{total_feeds} feeds processed successfully")
            return successful_feeds > 0
            
        except Exception as e:
            logger.error(f"Failed to collect all feeds content: {e}", exc_info=True)
            return False
    
    def _load_existing_feed_data(self, feed_filepath: Path, temp_filepath: Path) -> dict:
        """Load existing feed data for resume functionality."""
        existing_data = {'posts': [], 'feed_participants': {}, 'cursor': None}
        
        # Try to load from temp file first (incomplete session)
        if temp_filepath.exists():
            try:
                with open(temp_filepath, 'r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                    existing_data['posts'] = temp_data.get('posts', [])
                    existing_data['feed_participants'] = temp_data.get('feed_participants', {})
                    existing_data['cursor'] = temp_data.get('cursor')
                    logger.info(f"Resumed feed from temp file: {len(existing_data['posts'])} posts")
                    return existing_data
            except Exception as e:
                logger.warning(f"Failed to load feed temp file: {e}")
        
        # Try to load from final file
        if feed_filepath.exists():
            try:
                with open(feed_filepath, 'r', encoding='utf-8') as f:
                    final_data = json.load(f)
                    existing_data['posts'] = final_data.get('posts', [])
                    existing_data['feed_participants'] = final_data.get('feed_participants', {})
                    logger.info(f"Resumed feed from final file: {len(existing_data['posts'])} posts")
                    return existing_data
            except Exception as e:
                logger.warning(f"Failed to load feed final file: {e}")
        
        return existing_data

    async def _save_feed_batch(self, feed_uri: str, posts_data: list, feed_participants: dict,
                              cursor: Optional[str], feed_filepath: Path, temp_filepath: Path,
                              collected_count: int, is_final: bool = False):
        """Save feed results in batches."""
        try:
            result_data = {
                "feed_metadata": {
                    "feed_uri": feed_uri,
                    "total_results": len(posts_data),
                    "collected_at": self._get_current_time(),
                    "batch_saved": True,
                    "is_final": is_final
                },
                "posts": posts_data,
                "feed_participants": list(feed_participants.values()),
                "cursor": cursor  # Save cursor for resume
            }
            
            # Save to temp file during collection, final file when complete
            save_path = feed_filepath if is_final else temp_filepath
            self.saver.save_json(result_data, save_path)
            
            if is_final:
                logger.info(f"Final feed save: {len(posts_data)} posts saved to {feed_filepath}")
            else:
                logger.info(f"Feed batch save: {len(posts_data)} posts saved to temp file (cursor: {cursor})")
                
        except Exception as e:
            logger.error(f"Failed to save feed batch: {e}")
            raise

    def _load_existing_user_data(self, user_filepath: Path, temp_filepath: Path) -> dict:
        """Load existing user data for resume functionality."""
        # Check if this is a parallel worker by checking the temp file name pattern
        is_parallel_worker = '_worker_' in temp_filepath.stem and temp_filepath.stem.endswith('_temp')

        if is_parallel_worker:
            worker_stem = temp_filepath.stem.replace('_temp', '')
            batch_pattern = f"{worker_stem}_batch_*.json"
            batch_files = sorted(
                list(temp_filepath.parent.glob(batch_pattern)),
                key=lambda x: int(x.stem.split('_batch_')[-1])
            )
            
            if batch_files:
                logger.info(f"Found {len(batch_files)} existing batch files for user worker. Resuming...")
                all_posts = []
                last_cursor = None
                for batch_file in batch_files:
                    try:
                        with open(batch_file, 'r', encoding='utf-8') as f:
                            batch_data = json.load(f)
                        
                        posts = batch_data.get('posts', [])
                        all_posts.extend(posts)
                        
                        cursor_in_batch = batch_data.get('cursor')
                        if cursor_in_batch:
                            last_cursor = cursor_in_batch
                    except Exception as e:
                        logger.warning(f"Could not load or process user batch file {batch_file}: {e}")
                
                logger.info(f"Resumed with {len(all_posts)} posts for user worker.")
                return {
                    'posts': all_posts,
                    'cursor': last_cursor
                }

        # Original logic for single-threaded or if no batch files are found
        existing_data = {'posts': [], 'cursor': None}
        
        # Try to load from temp file first (incomplete session)
        if temp_filepath.exists() and not is_parallel_worker:
            try:
                with open(temp_filepath, 'r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                    existing_data['posts'] = temp_data.get('posts', [])
                    existing_data['cursor'] = temp_data.get('cursor')
                    logger.info(f"Resumed user from temp file: {len(existing_data['posts'])} posts")
                    return existing_data
            except Exception as e:
                logger.warning(f"Failed to load user temp file: {e}")
        
        # Try to load from final file
        if user_filepath.exists():
            try:
                with open(user_filepath, 'r', encoding='utf-8') as f:
                    final_data = json.load(f)
                    existing_data['posts'] = final_data.get('posts', [])
                    logger.info(f"Resumed user from final file: {len(existing_data['posts'])} posts")
                    return existing_data
            except Exception as e:
                logger.warning(f"Failed to load user final file: {e}")
        
        return existing_data

    async def _save_user_batch(self, handle: str, posts_data: list, cursor: Optional[str],
                              user_filepath: Path, temp_filepath: Path, collected_count: int, 
                              is_final: bool = False):
        """Save user posts in batches."""
        try:
            # For parallel collection, we need to save individual batches
            if hasattr(self, '_worker_filepaths'):
                # This is a parallel collection worker
                worker_id = self._worker_filepaths.get('worker_id', 0)
                
                # Calculate batch number based on total collected count
                batch_number = (collected_count // 100) + 1
                batch_temp_filepath = temp_filepath.parent / f"{temp_filepath.stem}_batch_{batch_number}.json"
                
                # For incremental saving, `posts_data` is already the slice of new posts
                batch_posts = posts_data
                
                batch_result = {
                    "batch_metadata": {
                        "handle": handle,
                        "batch_number": batch_number,
                        "worker_id": worker_id,
                        "posts_in_batch": len(batch_posts),
                        "collected_at": self._get_current_time(),
                        "is_final": is_final
                    },
                    "posts": batch_posts,
                    "cursor": cursor
                }
                
                self.saver.save_json(batch_result, batch_temp_filepath)
                logger.info(f"Worker {worker_id} Batch {batch_number}: Saved {len(batch_posts)} new posts to {batch_temp_filepath}")
                
                # If this is the final save, merge all batches for this worker
                if is_final:
                    await self._merge_user_worker_batches(handle, user_filepath, temp_filepath, worker_id)
                
            else:
                # Single-threaded collection - save all accumulated data
                result_data = {
                    "user_metadata": {
                        "handle": handle,
                        "total_results": len(posts_data),
                        "collected_at": self._get_current_time(),
                        "batch_saved": True,
                        "is_final": is_final
                    },
                    "posts": posts_data,
                    "cursor": cursor  # Save cursor for resume
                }
                
                # Save to temp file during collection, final file when complete
                save_path = user_filepath if is_final else temp_filepath
                self.saver.save_json(result_data, save_path)
                
                if is_final:
                    logger.info(f"Final user save: {len(posts_data)} posts saved to {user_filepath}")
                else:
                    logger.info(f"User batch save: {len(posts_data)} posts saved to temp file (cursor: {cursor})")
                
        except Exception as e:
            logger.error(f"Failed to save user batch: {e}")
            raise

    async def _process_post_view_for_feed(self, post_view: dict, feed_uri: str) -> Optional[dict]:
        """Process a single post for feed collection"""
        # Reuse existing processing logic, but use special marker for feed collection
        processed_post = await self._process_post_view(post_view, "FEED_COLLECTION")
        if not processed_post:
            return None
            
        # Add feed-specific fields
        processed_post['source_feed'] = feed_uri
        return processed_post

    def collect_all_discovered_users(self, collect_profiles: bool = True, collect_posts: bool = True, skip_existing: bool = True) -> bool:
        """Collect profiles and/or posts for all discovered users"""
        try:
            # Read discovered users with file locking
            discovered_users = self._safe_read_discovered_users()
            if discovered_users is None:
                logger.warning("No discovered users file found. Run keyword search or feed collection first.")
                return False
            
            if not discovered_users:
                logger.warning("No discovered users found.")
                return False
            
            logger.info(f"Found {len(discovered_users)} discovered users")
            
            successful_profiles = 0
            successful_posts = 0
            skipped_profiles = 0
            skipped_posts = 0
            
            for i, handle in enumerate(discovered_users, 1):
                if not isinstance(handle, str):
                    continue
                
                logger.info(f"Processing user {i}/{len(discovered_users)}: {handle}")
                
                # Check if files already exist
                profile_file = Path(USER_PROFILES_DIR) / f"{handle.replace('.', '_')}_profile.json"
                posts_file = Path(USER_POSTS_DIR) / f"{handle.replace('.', '_')}_posts.json"
                
                # Collect profile
                if collect_profiles:
                    if skip_existing and profile_file.exists():
                        logger.info(f"Skipping profile for {handle} (already exists)")
                        skipped_profiles += 1
                    else:
                        try:
                            profile = self.client.get_user_profile(handle)
                            if profile:
                                self.saver.save_json(profile, profile_file)
                                successful_profiles += 1
                                logger.info(f"Successfully collected profile for {handle}")
                            else:
                                logger.warning(f"Failed to get profile for {handle}")
                        except Exception as e:
                            logger.error(f"Error collecting profile for {handle}: {e}")
                
                # Collect posts
                if collect_posts:
                    if skip_existing and posts_file.exists():
                        logger.info(f"Skipping posts for {handle} (already exists)")
                        skipped_posts += 1
                    else:
                        try:
                            # Use a reasonable limit for batch collection
                            limit = 1000  # Collect 1000 posts per user for batch processing
                            success = asyncio.run(self.collect_user_posts(handle, limit))
                            if success:
                                successful_posts += 1
                                logger.info(f"Successfully collected posts for {handle}")
                            else:
                                logger.warning(f"Failed to collect posts for {handle}")
                        except Exception as e:
                            logger.error(f"Error collecting posts for {handle}: {e}")
                
                # Rate limiting between users
                import time
                time.sleep(self.rate_limit_delay)
            
            # Summary
            logger.info(f"Batch collection completed:")
            if collect_profiles:
                logger.info(f"  Profiles: {successful_profiles} collected, {skipped_profiles} skipped")
            if collect_posts:
                logger.info(f"  Posts: {successful_posts} collected, {skipped_posts} skipped")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to collect discovered users: {e}", exc_info=True)
            return False



async def main():
    """Main function to handle command line arguments and execute data collection"""
    parser = argparse.ArgumentParser(description="Bluesky Comprehensive Data Collection Tool")
    
    parser.add_argument('--keyword', help='Search for posts containing this keyword')
    parser.add_argument('--keywords', help='Comma-separated list of keywords to search')
    parser.add_argument('--user', help='User handle to collect')
    parser.add_argument('--user-profile', action='store_true', help='Collect only user profile')
    parser.add_argument('--user-posts', action='store_true', help='Collect only user posts')
    parser.add_argument('--user-all', action='store_true', help='Collect both user profile and posts')

    parser.add_argument('--limit', type=int, help='Maximum number of items to collect (0 for unlimited)')
    
    # Feed discovery and collection
    parser.add_argument('--feeds', action='store_true', help='Collect suggested feeds')
    parser.add_argument('--feed', help='Collect content from a specific feed URI')
    parser.add_argument('--all-feeds', action='store_true', help='Collect content from all suggested feeds')
    parser.add_argument('--feeds-limit', type=int, default=50, help='Maximum number of feeds to collect (default: 50)')
    parser.add_argument('--posts-per-feed', type=int, default=500, help='Maximum posts per feed (default: 500)')
    
    # Batch processing discovered users
    parser.add_argument('--batch-profiles', action='store_true', help='Collect profiles for all discovered users')
    parser.add_argument('--batch-posts', action='store_true', help='Collect posts for all discovered users')
    parser.add_argument('--batch-all', action='store_true', help='Collect both profiles and posts for all discovered users')
    parser.add_argument('--skip-existing', action='store_true', help='Skip users whose data already exists')
    
    # Parallel collection (new)
    parser.add_argument('--parallel', action='store_true', help='Use parallel collection with multiple accounts')
    parser.add_argument('--parallel-keyword', help='Search for posts containing this keyword using parallel collection')
    parser.add_argument('--parallel-keywords', help='Comma-separated list of keywords to search using parallel collection')
    parser.add_argument('--parallel-user', help='User handle to collect using parallel collection')
    parser.add_argument('--parallel-batch-all', action='store_true', help='Batch process all discovered users using parallel collection')
    parser.add_argument('--show-accounts', action='store_true', help='Show configured accounts for parallel collection')
    
    # Keyword search filters
    parser.add_argument('--author', help='Filter by author handle')
    parser.add_argument('--domain', help='Filter by domain')
    parser.add_argument('--lang', help='Filter by language code')
    parser.add_argument('--mentions', help='Filter by mentioned user')
    parser.add_argument('--tag', help='Filter by tag')
    parser.add_argument('--url', help='Filter by URL')
    parser.add_argument('--since', help='Filter posts since this date (ISO 8601 format)')
    parser.add_argument('--until', help='Filter posts until this date (ISO 8601 format)')
    parser.add_argument('--sort', choices=['top', 'latest', 'oldest'], help='Sort order for search results')
    
    args = parser.parse_args()
    
    # Show account information if requested
    if args.show_accounts:
        parallel_collector = ParallelCollector()
        account_info = parallel_collector.get_account_info()
        print("\n=== Parallel Collection Account Configuration ===")
        print(f"Total accounts: {account_info['total_accounts']}")
        print(f"Parallel workers: {account_info['parallel_workers']}")
        print(f"Time division strategy: {account_info['time_division_strategy']}")
        if account_info['time_division_strategy'] == 'overlap':
            print(f"Time overlap percent: {account_info['time_overlap_percent']}%")
        print("\nAccounts:")
        for acc in account_info['accounts']:
            print(f"  Worker {acc['worker_id']}: {acc['username']} {'(with app password)' if acc['has_app_password'] else ''}")
        return
    
    # Handle parallel collection
    if args.parallel or args.parallel_keyword or args.parallel_keywords or args.parallel_user or args.parallel_batch_all:
        await handle_parallel_collection(args)
        return
    
    # Handle regular collection
    collector = BlueskyDataCollector()
    
    if not collector.authenticate():
        logger.error("Authentication failed. Exiting.")
        return
        
    tasks = []
    if args.keyword:
        limit = args.limit if args.limit is not None else DEFAULT_KEYWORD_LIMIT
        tasks.append(collector.collect_by_keyword(
            args.keyword, limit, args.author, args.domain, args.lang,
            args.mentions, args.tag, args.url, args.since, args.until, args.sort
        ))
    
    if args.keywords:
        limit = args.limit if args.limit is not None else DEFAULT_KEYWORD_LIMIT
        keywords = [k.strip() for k in args.keywords.split(',')]
        for keyword in keywords:
            tasks.append(collector.collect_by_keyword(
                keyword, limit, args.author, args.domain, args.lang,
                args.mentions, args.tag, args.url, args.since, args.until, args.sort
            ))
    
    if args.user:
        if args.user_profile or args.user_all:
            tasks.append(collector.collect_user_profile(args.user))
        if args.user_posts or args.user_all:
            limit = args.limit if args.limit is not None else DEFAULT_USER_POSTS_LIMIT
            tasks.append(collector.collect_user_posts(args.user, limit))
        if not (args.user_profile or args.user_posts or args.user_all):
             logger.info("For --user, please specify --user-profile, --user-posts, or --user-all.")

    if args.feeds:
        tasks.append(collector.collect_suggested_feeds(args.feeds_limit))
    
    if args.feed:
        tasks.append(collector.collect_feed_content(args.feed, args.posts_per_feed))
    
    if args.all_feeds:
        tasks.append(collector.collect_all_feeds_content(args.feeds_limit, args.posts_per_feed))
    
    if args.batch_profiles or args.batch_posts or args.batch_all:
        collect_profiles = args.batch_profiles or args.batch_all
        collect_posts = args.batch_posts or args.batch_all
        tasks.append(asyncio.to_thread(collector.collect_all_discovered_users, collect_profiles, collect_posts, args.skip_existing))
    
    if not tasks:
        logger.info("No collection operations specified. Use --help for usage information.")
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successful_ops = sum(1 for r in results if r is True)
    total_ops = len(results)

    # Log any exceptions that occurred
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"A collection task failed: {result}", exc_info=result)

    if successful_ops == total_ops:
        logger.info(f"All {total_ops} data collection operations completed successfully!")
    else:
        logger.error(f"Some data collection operations failed ({successful_ops}/{total_ops} successful)")

async def handle_parallel_collection(args):
    """Handle parallel collection operations"""
    parallel_collector = ParallelCollector()
    
    # Check if accounts are configured
    account_info = parallel_collector.get_account_info()
    if account_info['total_accounts'] == 0:
        logger.error("No accounts configured for parallel collection. Please set up multi-account configuration in config.env")
        return
    
    logger.info(f"Starting parallel collection with {account_info['total_accounts']} accounts")
    
    tasks = []
    
    # Parallel keyword search
    if args.parallel_keyword:
        limit = args.limit if args.limit is not None else DEFAULT_KEYWORD_LIMIT
        filters = {
            'author': args.author, 'domain': args.domain, 'lang': args.lang,
            'mentions': args.mentions, 'tag': args.tag, 'url': args.url,
            'since': args.since, 'until': args.until, 'sort': args.sort
        }
        # Remove None values
        filters = {k: v for k, v in filters.items() if v is not None}
        
        result = await parallel_collector.collect_keyword_parallel(args.parallel_keyword, limit, **filters)
        print_parallel_result("Parallel Keyword Search", result)
    
    # Parallel keywords search
    if args.parallel_keywords:
        limit = args.limit if args.limit is not None else DEFAULT_KEYWORD_LIMIT
        keywords = [k.strip() for k in args.parallel_keywords.split(',')]
        filters = {
            'author': args.author, 'domain': args.domain, 'lang': args.lang,
            'mentions': args.mentions, 'tag': args.tag, 'url': args.url,
            'since': args.since, 'until': args.until, 'sort': args.sort
        }
        # Remove None values
        filters = {k: v for k, v in filters.items() if v is not None}
        
        for keyword in keywords:
            result = await parallel_collector.collect_keyword_parallel(keyword, limit, **filters)
            print_parallel_result(f"Parallel Keywords Search: {keyword}", result)
    
    # Parallel user posts collection
    if args.parallel_user:
        limit = args.limit if args.limit is not None else DEFAULT_USER_POSTS_LIMIT
        result = await parallel_collector.collect_user_posts_parallel(args.parallel_user, limit)
        print_parallel_result("Parallel User Posts Collection", result)
    
    # Parallel batch processing
    if args.parallel_batch_all:
        result = await parallel_collector.collect_batch_parallel(args.skip_existing)
        print_parallel_result("Parallel Batch Processing", result)

def print_parallel_result(operation_name: str, result: Dict[str, Any]):
    """Print parallel collection results in a formatted way"""
    print(f"\n=== {operation_name} Results ===")
    print(f"Success: {result.get('success', False)}")
    print(f"Successful workers: {result.get('successful_workers', 0)}/{result.get('total_workers', 0)}")
    
    if 'total_collected' in result:
        print(f"Total collected: {result.get('total_collected', 0)}")
    
    if 'total_profiles_collected' in result:
        print(f"Total profiles collected: {result.get('total_profiles_collected', 0)}")
    
    if 'total_posts_collected' in result:
        print(f"Total posts collected: {result.get('total_posts_collected', 0)}")
    
    if 'time_windows' in result and result['time_windows']:
        print("\nTime Windows:")
        for window in result['time_windows']:
            print(f"  Worker {window['worker_id']}: {window['start_time']} to {window['end_time']}")
    
    if 'user_distribution' in result and result['user_distribution']:
        print("\nUser Distribution:")
        for dist in result['user_distribution']:
            print(f"  Worker {dist['worker_id']}: {dist['users_assigned']} users assigned")
    
    if 'errors' in result and result['errors']:
        print("\nErrors:")
        for error in result['errors']:
            print(f"  {error}")

if __name__ == "__main__":
    asyncio.run(main())