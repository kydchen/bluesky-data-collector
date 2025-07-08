"""
Parallel Data Collector for Bluesky
Handles multi-account, multi-threaded data collection with automatic time window division
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dataclasses import dataclass
from dotenv import load_dotenv

from .atp_client import ATPClient

# Load environment variables from config.env
load_dotenv('config.env')

logger = logging.getLogger(__name__)

@dataclass
class AccountCredentials:
    """Account credentials for parallel collection"""
    username: str
    password: str
    app_password: Optional[str] = None
    worker_id: int = 0

@dataclass
class TimeWindow:
    """Time window for data collection"""
    start_time: str
    end_time: str
    worker_id: int = 0

class ParallelCollector:
    """Multi-account, multi-threaded data collector with automatic time window division"""
    
    def __init__(self):
        self.accounts: List[AccountCredentials] = []
        self.time_windows: List[TimeWindow] = []
        self.parallel_workers = int(os.getenv('PARALLEL_WORKERS', 3))
        self.time_division_strategy = os.getenv('TIME_DIVISION_STRATEGY', 'weighted')
        self.time_overlap_percent = int(os.getenv('TIME_OVERLAP_PERCENT', 10))
        self.custom_time_windows = os.getenv('CUSTOM_TIME_WINDOWS', '')
        
        # Thread-local storage for clients
        self._local = threading.local()
        
        # Load accounts from environment
        self._load_accounts()
        
        # Validate that parallel_workers matches account count
        if len(self.accounts) > 0 and self.parallel_workers != len(self.accounts):
            logger.warning(f"PARALLEL_WORKERS ({self.parallel_workers}) doesn't match account count ({len(self.accounts)}). Using account count.")
            self.parallel_workers = len(self.accounts)
        
    def _load_accounts(self):
        """Load multiple account credentials from environment variables"""
        usernames = os.getenv('MULTI_ACCOUNT_USERNAMES', '').split(',')
        passwords = os.getenv('MULTI_ACCOUNT_PASSWORDS', '').split(',')
        app_passwords = os.getenv('MULTI_ACCOUNT_APP_PASSWORDS', '').split(',')
        
        # Check if multi-account configuration is properly set
        if usernames and usernames[0].strip() and usernames[0].strip() != 'account1.bsky.social':
            # Multi-account configuration is set
            logger.info(f"Found multi-account configuration with {len(usernames)} accounts")
        else:
            # Fallback to single account if multi-account not configured
            single_username = os.getenv('BLUESKY_USERNAME')
            single_password = os.getenv('BLUESKY_PASSWORD')
            single_app_password = os.getenv('BLUESKY_APP_PASSWORD')
            
            if single_username and single_password:
                self.accounts = [AccountCredentials(
                    username=single_username,
                    password=single_password,
                    app_password=single_app_password,
                    worker_id=0
                )]
                logger.info("Using single account configuration")
                return
            else:
                logger.warning("No account configuration found")
                return
        
        # Validate multi-account configuration
        if len(usernames) != len(passwords):
            logger.error("Number of usernames and passwords must match")
            return
        
        # Create account credentials
        for i, (username, password) in enumerate(zip(usernames, passwords)):
            if username.strip() and password.strip():
                app_password = app_passwords[i] if i < len(app_passwords) and app_passwords[i].strip() else None
                self.accounts.append(AccountCredentials(
                    username=username.strip(),
                    password=password.strip(),
                    app_password=app_password,
                    worker_id=i
                ))
        
        logger.info(f"Loaded {len(self.accounts)} accounts for parallel collection")
    
    def _get_client(self) -> ATPClient:
        """Get thread-local ATP client"""
        if not hasattr(self._local, 'client'):
            self._local.client = ATPClient()
        return self._local.client
    
    def _authenticate_account(self, account: AccountCredentials) -> bool:
        """Authenticate a specific account"""
        client = self._get_client()
        return client.authenticate(account.username, account.password, account.app_password)
    
    def _divide_time_windows(self, since: Optional[str] = None, until: Optional[str] = None) -> List[TimeWindow]:
        """Divide time range into windows for parallel collection"""
        if self.time_division_strategy == 'custom' and self.custom_time_windows:
            return self._parse_custom_time_windows()
        
        # Default to Bluesky public launch date (2024-02-01) to today if no time range specified
        if not since:
            since = "2024-02-01T00:00:00Z"  # Bluesky public launch date
        if not until:
            until = datetime.now().isoformat() + 'Z'
        
        start_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(until.replace('Z', '+00:00'))
        
        if self.time_division_strategy == 'equal':
            return self._divide_equal_windows(start_dt, end_dt)
        elif self.time_division_strategy == 'overlap':
            return self._divide_overlap_windows(start_dt, end_dt)
        elif self.time_division_strategy == 'weighted':
            return self._divide_weighted_windows(start_dt, end_dt)
        else:
            # Default to weighted division for better workload balance
            logger.warning(f"Unknown time division strategy '{self.time_division_strategy}'. Defaulting to 'weighted'.")
            return self._divide_weighted_windows(start_dt, end_dt)
    
    def _parse_custom_time_windows(self) -> List[TimeWindow]:
        """Parse custom time windows from configuration"""
        windows = []
        custom_windows = self.custom_time_windows.split(',')
        
        for i, window_str in enumerate(custom_windows):
            if '-' in window_str:
                start_end = window_str.split('-', 1)
                if len(start_end) == 2:
                    windows.append(TimeWindow(
                        start_time=start_end[0].strip(),
                        end_time=start_end[1].strip(),
                        worker_id=i
                    ))
        
        logger.info(f"Parsed {len(windows)} custom time windows")
        return windows
    
    def _divide_equal_windows(self, start_dt: datetime, end_dt: datetime) -> List[TimeWindow]:
        """Divide time range into equal windows"""
        total_duration = end_dt - start_dt
        # Use the validated parallel_workers count
        num_workers = self.parallel_workers
        window_duration = total_duration / num_workers
        
        windows = []
        for i in range(num_workers):
            window_start = start_dt + (window_duration * i)
            window_end = start_dt + (window_duration * (i + 1))
            
            # Ensure last window ends at the specified end time
            if i == num_workers - 1:
                window_end = end_dt
            
            windows.append(TimeWindow(
                start_time=window_start.isoformat().replace('+00:00', 'Z'),
                end_time=window_end.isoformat().replace('+00:00', 'Z'),
                worker_id=i
            ))
        
        logger.info(f"Divided time range into {len(windows)} equal windows for {num_workers} workers")
        return windows
    
    def _divide_weighted_windows(self, start_dt: datetime, end_dt: datetime) -> List[TimeWindow]:
        """Divides time into windows with shorter durations for more recent periods."""
        num_workers = self.parallel_workers
        if num_workers <= 0: return []

        # A factor determining how much shorter each subsequent window is.
        # A higher base means more aggressive weighting towards recent periods.
        base = 2.0 
        
        # The durations will be proportional to [b^(n-1), b^(n-2), ..., b^1, b^0]
        # This gives the longest duration to the earliest time window (worker 0).
        weights = [base**(num_workers - 1 - i) for i in range(num_workers)]
        total_weight = sum(weights)
        
        total_duration = end_dt - start_dt
        windows = []
        current_start_dt = start_dt
        
        for i in range(num_workers):
            duration_seconds = total_duration.total_seconds() * (weights[i] / total_weight)
            window_duration = timedelta(seconds=duration_seconds)
            
            window_end = current_start_dt + window_duration
            if i == num_workers - 1:
                window_end = end_dt # Ensure final window ends precisely.

            windows.append(TimeWindow(
                start_time=current_start_dt.isoformat().replace('+00:00', 'Z'),
                end_time=window_end.isoformat().replace('+00:00', 'Z'),
                worker_id=i
            ))
            
            current_start_dt = window_end
            
        logger.info(f"Divided time range into {len(windows)} weighted windows.")
        for w in windows:
            logger.info(f"  Worker {w.worker_id}: {w.start_time} to {w.end_time}")
            
        return windows
    
    def _divide_overlap_windows(self, start_dt: datetime, end_dt: datetime) -> List[TimeWindow]:
        """Divide time range into overlapping windows"""
        total_duration = end_dt - start_dt
        # Use the validated parallel_workers count
        num_workers = self.parallel_workers
        overlap_duration = total_duration * (self.time_overlap_percent / 100)
        window_duration = (total_duration + overlap_duration * (num_workers - 1)) / num_workers
        
        windows = []
        for i in range(num_workers):
            window_start = start_dt + (window_duration * i) - (overlap_duration * i)
            window_end = start_dt + (window_duration * (i + 1)) - (overlap_duration * i)
            
            # Ensure first window starts at the specified start time
            if i == 0:
                window_start = start_dt
            
            # Ensure last window ends at the specified end time
            if i == num_workers - 1:
                window_end = end_dt
            
            windows.append(TimeWindow(
                start_time=window_start.isoformat().replace('+00:00', 'Z'),
                end_time=window_end.isoformat().replace('+00:00', 'Z'),
                worker_id=i
            ))
        
        logger.info(f"Divided time range into {len(windows)} overlapping windows for {num_workers} workers")
        return windows
    
    async def _collect_keyword_parallel(self, keyword: str, limit: int, 
                                      account: AccountCredentials, 
                                      time_window: TimeWindow,
                                      **filters) -> Tuple[int, str]:
        """Collect keyword data for a specific account and time window"""
        try:
            # Authenticate account
            if not self._authenticate_account(account):
                logger.error(f"Failed to authenticate account {account.username}")
                return 0, f"Authentication failed for {account.username}"
            
            # Create collector instance for this worker
            from main import BlueskyDataCollector
            collector = BlueskyDataCollector()
            collector.client = self._get_client()
            
            # Add time window filters
            filters['since'] = time_window.start_time
            filters['until'] = time_window.end_time
            
            # Adjust limit per worker
            # For unlimited collection (limit = 0), each worker should also be unlimited
            # For limited collection, divide the limit among workers
            worker_limit = 0 if limit == 0 else limit // len(self.accounts)
            
            logger.info(f"Worker {account.worker_id}: Collecting '{keyword}' from {time_window.start_time} to {time_window.end_time}")
            
            # Use worker-specific file paths to avoid conflicts
            import os
            from pathlib import Path
            data_dir = os.getenv('DATA_DIR', 'data')
            keywords_dir = os.path.join(data_dir, "keywords")
            
            # Create unique file names that include time range to avoid conflicts between concurrent parallel runs
            since_str = time_window.start_time.split('T')[0] if time_window.start_time else "start"
            until_str = time_window.end_time.split('T')[0] if time_window.end_time else "end"
            time_range_suffix = f"_{since_str}_to_{until_str}"
            worker_suffix = f"_worker_{account.worker_id}"
            
            keyword_safe = keyword.replace(' ', '_').replace('.', '_')
            search_filepath = Path(keywords_dir) / f"search_{keyword_safe}{time_range_suffix}{worker_suffix}.json"
            temp_filepath = Path(keywords_dir) / f"search_{keyword_safe}{time_range_suffix}{worker_suffix}_temp.json"
            
            # Override the file paths in the collector
            collector._worker_filepaths = {
                'search_filepath': search_filepath,
                'temp_filepath': temp_filepath,
                'worker_id': account.worker_id
            }
            
            # Collect data
            success = await collector.collect_by_keyword(
                keyword, worker_limit, **filters
            )
            
            if success:
                # Count the actual posts collected by this worker by reading the final file
                try:
                    if search_filepath.exists():
                        with open(search_filepath, 'r', encoding='utf-8') as f:
                            search_data = json.load(f)
                            collected_count = len(search_data.get('posts', []))
                    else:
                        collected_count = 0
                except Exception as e:
                    logger.warning(f"Could not count posts for worker {account.worker_id}: {e}")
                    collected_count = worker_limit if worker_limit > 0 else 1000  # Estimate
                
                return collected_count, f"Successfully collected {collected_count} posts for {account.username}"
            else:
                return 0, f"Collection failed for {account.username}"
                
        except Exception as e:
            logger.error(f"Error in worker {account.worker_id}: {e}")
            return 0, f"Error: {str(e)}"
    
    async def collect_keyword_parallel(self, keyword: str, limit: int = 1000, **filters) -> Dict[str, Any]:
        """Collect keyword data using multiple accounts in parallel"""
        if not self.accounts:
            logger.error("No accounts configured for parallel collection")
            return {"success": False, "error": "No accounts configured"}
        
        # Divide time windows
        self.time_windows = self._divide_time_windows(
            filters.get('since'), filters.get('until')
        )
        
        if len(self.time_windows) != len(self.accounts):
            logger.warning(f"Time windows ({len(self.time_windows)}) don't match accounts ({len(self.accounts)})")
            # Adjust time windows to match account count
            if len(self.time_windows) < len(self.accounts):
                # Duplicate last window
                last_window = self.time_windows[-1] if self.time_windows else TimeWindow(
                    start_time="2024-02-01T00:00:00Z",
                    end_time=datetime.now().isoformat() + 'Z'
                )
                while len(self.time_windows) < len(self.accounts):
                    self.time_windows.append(TimeWindow(
                        start_time=last_window.start_time,
                        end_time=last_window.end_time,
                        worker_id=len(self.time_windows)
                    ))
            else:
                # Truncate to account count
                self.time_windows = self.time_windows[:len(self.accounts)]
        
        logger.info(f"Starting parallel collection with {len(self.accounts)} accounts")
        logger.info(f"Time windows: {len(self.time_windows)}")
        
        # Create tasks for parallel execution
        tasks = []
        for i, account in enumerate(self.accounts):
            if i < len(self.time_windows):
                time_window = self.time_windows[i]
                task = self._collect_keyword_parallel(
                    keyword, limit, account, time_window, **filters
                )
                tasks.append(task)
        
        # Execute tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        total_collected = 0
        successful_workers = 0
        error_messages = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_messages.append(f"Worker {i}: {str(result)}")
            else:
                collected, message = result
                total_collected += collected
                if collected > 0:
                    successful_workers += 1
                if message:
                    error_messages.append(f"Worker {i}: {message}")
        
        # Merge worker files into final result
        if successful_workers > 0:
            await self._merge_worker_files(keyword)
        
        return {
            "success": successful_workers > 0,
            "total_collected": total_collected,
            "successful_workers": successful_workers,
            "total_workers": len(self.accounts),
            "time_windows": [
                {
                    "worker_id": w.worker_id,
                    "start_time": w.start_time,
                    "end_time": w.end_time
                } for w in self.time_windows
            ],
            "errors": error_messages
        }
    
    async def collect_user_posts_parallel(self, handle: str, limit: int = 10000, 
                                        since: Optional[str] = None, until: Optional[str] = None) -> Dict[str, Any]:
        """Collect user posts using multiple accounts in parallel with time-based distribution"""
        if not self.accounts:
            logger.error("No accounts configured for parallel collection")
            return {"success": False, "error": "No accounts configured"}
        
        logger.info(f"Starting parallel user posts collection for {handle}")
        
        # Use default time range if not specified (2024-02-01 to today)
        if not since:
            since = "2024-02-01T00:00:00Z"
        if not until:
            until = datetime.now().isoformat() + 'Z'
        
        # Divide time windows for parallel collection
        self.time_windows = self._divide_time_windows(since, until)
        
        if len(self.time_windows) != len(self.accounts):
            logger.warning(f"Time windows ({len(self.time_windows)}) don't match accounts ({len(self.accounts)})")
            # Adjust time windows to match account count
            if len(self.time_windows) < len(self.accounts):
                # Duplicate last window
                last_window = self.time_windows[-1] if self.time_windows else TimeWindow(
                    start_time=since,
                    end_time=until
                )
                while len(self.time_windows) < len(self.accounts):
                    self.time_windows.append(TimeWindow(
                        start_time=last_window.start_time,
                        end_time=last_window.end_time,
                        worker_id=len(self.time_windows)
                    ))
            else:
                # Truncate to account count
                self.time_windows = self.time_windows[:len(self.accounts)]
        
        logger.info(f"Divided time range into {len(self.time_windows)} windows for parallel collection")
        
        # Create tasks for parallel execution with different time windows
        tasks = []
        for i, account in enumerate(self.accounts):
            if i < len(self.time_windows):
                time_window = self.time_windows[i]
                task = self._collect_user_posts_worker_with_time_window(handle, limit, account, time_window)
                tasks.append(task)
            else:
                # If more accounts than time windows, use the last window
                time_window = self.time_windows[-1] if self.time_windows else TimeWindow(
                    start_time=since,
                    end_time=until,
                    worker_id=i
                )
                task = self._collect_user_posts_worker_with_time_window(handle, limit, account, time_window)
                tasks.append(task)
        
        # Execute tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful_workers = 0
        error_messages = []
        total_collected = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_messages.append(f"Worker {i}: {str(result)}")
            else:
                success, message, collected = result
                if success:
                    successful_workers += 1
                    total_collected += collected
                if message:
                    error_messages.append(f"Worker {i}: {message}")
        
        return {
            "success": successful_workers > 0,
            "successful_workers": successful_workers,
            "total_workers": len(self.accounts),
            "total_collected": total_collected,
            "time_windows": [
                {
                    "worker_id": w.worker_id,
                    "start_time": w.start_time,
                    "end_time": w.end_time
                } for w in self.time_windows
            ],
            "errors": error_messages
        }
    

    
    async def _collect_user_posts_worker_with_time_window(self, handle: str, limit: int, 
                                                        account: AccountCredentials, 
                                                        time_window: TimeWindow) -> Tuple[bool, str, int]:
        """Collect user posts for a specific account within a specific time window"""
        try:
            # Authenticate account
            if not self._authenticate_account(account):
                logger.error(f"Failed to authenticate account {account.username}")
                return False, f"Authentication failed for {account.username}", 0
            
            # Create collector instance for this worker
            from main import BlueskyDataCollector
            collector = BlueskyDataCollector()
            collector.client = self._get_client()
            
            # Adjust limit per worker
            # For unlimited collection (limit = 0), each worker should also be unlimited
            # For limited collection, divide the limit among workers
            worker_limit = 0 if limit == 0 else limit // len(self.accounts)
            
            logger.info(f"Worker {account.worker_id}: Collecting posts for {handle} from {time_window.start_time} to {time_window.end_time}")
            
            # Set up worker-specific file paths for batch processing
            import os
            from pathlib import Path
            data_dir = os.getenv('DATA_DIR', 'data')
            users_dir = os.path.join(data_dir, "users")
            user_posts_dir = os.path.join(users_dir, "posts")
            
            # Create worker-specific file names
            worker_suffix = f"_worker_{account.worker_id}"
            user_filepath = Path(user_posts_dir) / f"{handle.replace('.', '_')}{worker_suffix}_posts.json"
            temp_filepath = Path(user_posts_dir) / f"{handle.replace('.', '_')}{worker_suffix}_posts_temp.json"
            
            # Override the file paths in the collector for batch processing
            collector._worker_filepaths = {
                'search_filepath': user_filepath,
                'temp_filepath': temp_filepath,
                'worker_id': account.worker_id
            }
            
            # Collect data within the specified time window
            success = await collector.collect_user_posts(
                handle, worker_limit, 
                since=time_window.start_time, 
                until=time_window.end_time,
                use_default_since=False  # Don't use default since we're providing our own
            )
            
            if success:
                # Count the posts collected by this worker by reading the final file
                try:
                    if user_filepath.exists():
                        with open(user_filepath, 'r', encoding='utf-8') as f:
                            user_data = json.load(f)
                            collected_count = len(user_data.get('posts', []))
                    else:
                        collected_count = 0
                except Exception as e:
                    logger.warning(f"Could not count posts for worker {account.worker_id}: {e}")
                    collected_count = worker_limit if worker_limit > 0 else 1000  # Estimate
                
                return True, f"Successfully collected {collected_count} posts for {account.username}", collected_count
            else:
                return False, f"Collection failed for {account.username}", 0
                
        except Exception as e:
            logger.error(f"Error in worker {account.worker_id}: {e}")
            return False, f"Error: {str(e)}", 0
    
    async def collect_batch_parallel(self, skip_existing: bool = True) -> Dict[str, Any]:
        """Collect profiles and posts for all discovered users using parallel processing"""
        if not self.accounts:
            logger.error("No accounts configured for parallel collection")
            return {"success": False, "error": "No accounts configured"}
        
        logger.info(f"Starting parallel batch processing with {len(self.accounts)} accounts")
        
        # Read discovered users
        from main import BlueskyDataCollector
        collector = BlueskyDataCollector()
        discovered_users = collector._safe_read_discovered_users()
        
        if not discovered_users:
            logger.warning("No discovered users found. Run keyword search or feed collection first.")
            return {"success": False, "error": "No discovered users found"}
        
        logger.info(f"Found {len(discovered_users)} discovered users for batch processing")
        
        # Distribute users among accounts
        users_per_account = len(discovered_users) // len(self.accounts)
        remainder = len(discovered_users) % len(self.accounts)
        
        user_distribution = []
        start_idx = 0
        for i in range(len(self.accounts)):
            end_idx = start_idx + users_per_account + (1 if i < remainder else 0)
            user_distribution.append(discovered_users[start_idx:end_idx])
            start_idx = end_idx
        
        # Create tasks for parallel execution
        tasks = []
        for i, account in enumerate(self.accounts):
            if i < len(user_distribution):
                assigned_users = user_distribution[i]
                task = self._collect_batch_worker(account, assigned_users, skip_existing)
                tasks.append(task)
        
        # Execute tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful_workers = 0
        error_messages = []
        total_profiles = 0
        total_posts = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_messages.append(f"Worker {i}: {str(result)}")
            else:
                success, profiles, posts, message = result
                if success:
                    successful_workers += 1
                    total_profiles += profiles
                    total_posts += posts
                if message:
                    error_messages.append(f"Worker {i}: {message}")
        
        return {
            "success": successful_workers > 0,
            "successful_workers": successful_workers,
            "total_workers": len(self.accounts),
            "total_profiles_collected": total_profiles,
            "total_posts_collected": total_posts,
            "user_distribution": [
                {
                    "worker_id": i,
                    "users_assigned": len(user_distribution[i]) if i < len(user_distribution) else 0
                } for i in range(len(self.accounts))
            ],
            "errors": error_messages
        }
    
    async def _collect_batch_worker(self, account: AccountCredentials, users: List[str], 
                                  skip_existing: bool) -> Tuple[bool, int, int, str]:
        """Collect profiles and posts for assigned users"""
        try:
            # Authenticate account
            if not self._authenticate_account(account):
                logger.error(f"Failed to authenticate account {account.username}")
                return False, 0, 0, f"Authentication failed for {account.username}"
            
            # Create collector instance for this worker
            from main import BlueskyDataCollector
            collector = BlueskyDataCollector()
            collector.client = self._get_client()
            
            logger.info(f"Worker {account.worker_id}: Processing {len(users)} users")
            
            profiles_collected = 0
            posts_collected = 0
            
            for i, handle in enumerate(users, 1):
                if not isinstance(handle, str):
                    continue
                
                logger.info(f"Worker {account.worker_id}: Processing user {i}/{len(users)}: {handle}")
                
                # Check if files already exist
                from pathlib import Path
                import os
                data_dir = os.getenv('DATA_DIR', 'data')
                users_dir = os.path.join(data_dir, "users")
                user_profiles_dir = os.path.join(users_dir, "profiles")
                user_posts_dir = os.path.join(users_dir, "posts")
                
                profile_file = Path(user_profiles_dir) / f"{handle.replace('.', '_')}_profile.json"
                posts_file = Path(user_posts_dir) / f"{handle.replace('.', '_')}_posts.json"
                
                # Collect profile
                if skip_existing and profile_file.exists():
                    logger.info(f"Worker {account.worker_id}: Skipping profile for {handle} (already exists)")
                else:
                    try:
                        profile = collector.client.get_user_profile(handle)
                        if profile:
                            collector.saver.save_json(profile, profile_file)
                            profiles_collected += 1
                            logger.info(f"Worker {account.worker_id}: Successfully collected profile for {handle}")
                        else:
                            logger.warning(f"Worker {account.worker_id}: Failed to get profile for {handle}")
                    except Exception as e:
                        logger.error(f"Worker {account.worker_id}: Error collecting profile for {handle}: {e}")
                
                # Collect posts
                if skip_existing and posts_file.exists():
                    logger.info(f"Worker {account.worker_id}: Skipping posts for {handle} (already exists)")
                else:
                    try:
                        # Use a reasonable limit for batch collection
                        limit = 1000  # Collect 1000 posts per user for batch processing
                        success = await collector.collect_user_posts(handle, limit)
                        if success:
                            posts_collected += 1
                            logger.info(f"Worker {account.worker_id}: Successfully collected posts for {handle}")
                        else:
                            logger.warning(f"Worker {account.worker_id}: Failed to collect posts for {handle}")
                    except Exception as e:
                        logger.error(f"Worker {account.worker_id}: Error collecting posts for {handle}: {e}")
                
                # Rate limiting between users
                await asyncio.sleep(0.1)  # 100ms delay between users
            
            return True, profiles_collected, posts_collected, f"Successfully processed {len(users)} users"
                
        except Exception as e:
            logger.error(f"Error in batch worker {account.worker_id}: {e}")
            return False, 0, 0, f"Error: {str(e)}"
    
    async def _merge_worker_files(self, keyword: str):
        """Merge worker-specific files into a single final file"""
        try:
            import os
            from pathlib import Path
            data_dir = os.getenv('DATA_DIR', 'data')
            keywords_dir = os.path.join(data_dir, "keywords")
            
            # Generate time range suffix for final file based on the first time window
            time_range_suffix = ""
            if self.time_windows:
                first_window = self.time_windows[0]
                last_window = self.time_windows[-1]
                since_str = first_window.start_time.split('T')[0] if first_window.start_time else "start"
                until_str = last_window.end_time.split('T')[0] if last_window.end_time else "end"
                time_range_suffix = f"_{since_str}_to_{until_str}"
            
            # Final merged file path with time range
            keyword_safe = keyword.replace(' ', '_').replace('.', '_')
            final_filepath = Path(keywords_dir) / f"search_{keyword_safe}{time_range_suffix}.json"
            
            # Collect all worker files
            all_posts = []
            all_participants = {}
            post_uris_seen = set()
            
            logger.info(f"Merging worker files for keyword '{keyword}' with time range {time_range_suffix}...")
            
            for worker_id in range(len(self.accounts)):
                worker_suffix = f"_worker_{worker_id}"
                worker_filepath = Path(keywords_dir) / f"search_{keyword_safe}{time_range_suffix}{worker_suffix}.json"
                
                if worker_filepath.exists():
                    try:
                        with open(worker_filepath, 'r', encoding='utf-8') as f:
                            worker_data = json.load(f)
                            
                        # Merge posts (deduplicate by URI)
                        worker_posts = worker_data.get('posts', [])
                        for post in worker_posts:
                            if isinstance(post, dict) and post.get('uri'):
                                post_uri = post['uri']
                                if post_uri not in post_uris_seen:
                                    post_uris_seen.add(post_uri)
                                    all_posts.append(post)
                        
                        # Merge participants
                        worker_participants = worker_data.get('topic_participants', [])
                        for participant in worker_participants:
                            if isinstance(participant, dict) and participant.get('handle'):
                                handle = participant['handle']
                                if handle not in all_participants:
                                    all_participants[handle] = participant
                        
                        logger.info(f"Worker {worker_id}: Merged {len(worker_posts)} posts and {len(worker_participants)} participants")
                        
                        # Clean up worker file
                        worker_filepath.unlink()
                        logger.info(f"Cleaned up worker file: {worker_filepath}")
                        
                    except Exception as e:
                        logger.error(f"Error processing worker {worker_id} file: {e}")
                else:
                    logger.warning(f"Worker {worker_id} file not found: {worker_filepath}")
            
            # Create final merged data
            final_data = {
                "search_metadata": {
                    "keyword": keyword,
                    "total_results": len(all_posts),
                    "recursion_strategy": "original_only",
                    "collected_at": datetime.now().isoformat(),
                    "parallel_collection": True,
                    "workers_used": len(self.accounts),
                    "time_range": time_range_suffix,
                    "time_windows": [
                        {
                            "worker_id": w.worker_id,
                            "start_time": w.start_time,
                            "end_time": w.end_time
                        } for w in self.time_windows
                    ]
                },
                "posts": all_posts,
                "topic_participants": list(all_participants.values())
            }
            
            # Save final merged file
            final_filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(final_filepath, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
            
            # Update keyword-specific discovered users
            if all_participants:
                from main import BlueskyDataCollector
                collector = BlueskyDataCollector()
                participant_handles = set(all_participants.keys())
                import asyncio
                await collector._update_discovered_users(participant_handles, keyword)
                logger.info(f"Updated keyword-specific discovered users for '{keyword}' with {len(participant_handles)} participants")
            
            logger.info(f"Successfully merged {len(all_posts)} posts and {len(all_participants)} participants into final file: {final_filepath}")
            
        except Exception as e:
            logger.error(f"Error merging worker files: {e}")
    
    def get_account_info(self) -> Dict[str, Any]:
        """Get information about configured accounts"""
        return {
            "total_accounts": len(self.accounts),
            "accounts": [
                {
                    "worker_id": acc.worker_id,
                    "username": acc.username,
                    "has_app_password": acc.app_password is not None
                } for acc in self.accounts
            ],
            "parallel_workers": self.parallel_workers,
            "time_division_strategy": self.time_division_strategy,
            "time_overlap_percent": self.time_overlap_percent
        } 