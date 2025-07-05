"""
Handles saving data to files robustly.
"""

import json
from pathlib import Path
import logging
from typing import Any, Set

logger = logging.getLogger(__name__)

class DataSaver:
    """Handles creating directories and saving data to files."""

    def save_json(self, data: Any, filepath: Path):
        """Saves data to a JSON file, creating parent directories if they don't exist."""
        try:
            # Ensure the directory exists before trying to write the file
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Provide a more descriptive log message
            count_str = f"{len(data)} items" if isinstance(data, list) else "data"
            logger.info(f"Successfully saved {count_str} to {filepath}")
        except (IOError, TypeError) as e:
            logger.error(f"Error saving data to {filepath}: {e}")

    def save_discovered_users(self, users: Set[str], filepath: Path):
        """Reads, updates, and writes the central list of discovered users."""
        if not users:
            return

        existing_users: Set[str] = set()
        if filepath.exists():
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content:
                        existing_users.update(json.loads(content))
            except (IOError, json.JSONDecodeError) as e:
                logger.warning(f"Could not read or parse existing discovered users file at {filepath}: {e}. Starting fresh.")
        
        new_users = users - existing_users
        if not new_users:
            logger.info(f"No new users to add to the discovered list. Total remains {len(existing_users)}.")
            return

        updated_users = existing_users.union(users)
        
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(sorted(list(updated_users)), f, ensure_ascii=False, indent=2)
            logger.info(f"Updated discovered users file with {len(new_users)} new users. Total: {len(updated_users)}")
        except IOError as e:
            logger.error(f"Error saving discovered users to {filepath}: {e}")
