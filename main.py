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
from src.utils.data_saver import DataSaver

# --- Configuration ---
# Read from environment variables with defaults
RATE_LIMIT_DELAY = float(os.getenv('RATE_LIMIT_DELAY', 0.1))
DATA_DIR = os.getenv('DATA_DIR', 'data')
SEARCH_DIR = os.path.join(DATA_DIR, "search")
USERS_DIR = os.path.join(DATA_DIR, "users")
DISCOVERED_USERS_PATH = os.path.join(USERS_DIR, "discovered_users.json")

DEFAULT_KEYWORD_LIMIT = int(os.getenv('DEFAULT_KEYWORD_LIMIT', 1000))
DEFAULT_POPULAR_LIMIT = int(os.getenv('DEFAULT_POPULAR_LIMIT', 500))
DEFAULT_SUGGESTED_LIMIT = int(os.getenv('DEFAULT_SUGGESTED_LIMIT', 100))
DEFAULT_USER_POSTS_LIMIT = int(os.getenv('DEFAULT_USER_POSTS_LIMIT', 10000))

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
                                sort: Optional[str] = None) -> bool:
        """Collect posts by keyword with deduplication and user discovery."""
        try:
            logger.info(f"Collecting posts for keyword: '{keyword}'")
            posts_data = []
            cursor = None
            collected_count = 0
            post_uris_seen = set()  # Deduplication set
            topic_participants = {}  # Topic participants
            
            while limit == 0 or collected_count < limit:
                batch_limit = min(100, limit - collected_count) if limit > 0 else 100
                await asyncio.sleep(self.rate_limit_delay)
                
                search_response = self.client.search_posts(
                    keyword, batch_limit, cursor, author, domain, lang, 
                    mentions, tag, url, since, until, sort
                )
                if not search_response or not search_response.get('posts'):
                    break
                
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
                        
                        # Collect user information from post author and interactions
                        author_handle = processed_post.get('author_handle')
                        if author_handle:
                            if author_handle not in topic_participants:
                                topic_participants[author_handle] = {
                                    'handle': author_handle,
                                    'did': processed_post.get('author_did'),
                                    'displayName': processed_post.get('author_displayName', '')
                                }
                        
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
                
                collected_count = len(posts_data)
                cursor = search_response.get('cursor')
                
                if not cursor:
                    logger.info("No more pages to fetch.")
                    break
                logger.info(f"Collected {collected_count} unique posts so far for keyword '{keyword}'")

            # Save search results
            search_filepath = Path(SEARCH_DIR) / f"search_{keyword.replace(' ', '_').replace('.', '_')}.json"
            
            result_data = {
                "search_metadata": {
                    "keyword": keyword,
                    "total_results": len(posts_data),
                    "recursion_strategy": "original_only",
                    "collected_at": self._get_current_time()
                },
                "posts": posts_data,
                "topic_participants": list(topic_participants.values())
            }
            
            self.saver.save_json(result_data, search_filepath)
            logger.info(f"Successfully saved {len(posts_data)} posts and {len(topic_participants)} participants for keyword '{keyword}'")
            
            return True
        except Exception as e:
            logger.error(f"Failed to collect posts for keyword '{keyword}': {e}", exc_info=True)
            return False

    async def _process_post_view_for_search(self, post_view: dict, keyword: str) -> Optional[dict]:
        """Process a single post for keyword search with original-only recursion strategy."""
        # Reuse existing processing logic, but use special marker for keyword search
        processed_post = await self._process_post_view(post_view, "KEYWORD_SEARCH")
        if not processed_post:
            return None
            
        # Add keyword search specific fields
        processed_post['search_keyword'] = keyword
        return processed_post

    def _get_current_time(self) -> str:
        """Get current time in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()

    def collect_popular_content(self, limit: int = 500) -> bool:
        logger.warning("collect_popular_content is a placeholder.")
        return True

    async def collect_user_profile(self, handle: str) -> bool:
        """Collect and save a single user's profile."""
        try:
            logger.info(f"Collecting profile for user: {handle}")
            profile = self.client.get_user_profile(handle)
            if not profile:
                logger.error(f"Profile not found or failed to fetch for user {handle}")
                return False

            user_filepath = Path(USERS_DIR) / f"{handle.replace('.', '_')}_profile.json"
            self.saver.save_json(profile, user_filepath)
            return True
        except Exception as e:
            logger.error(f"Failed to collect profile for user {handle}: {e}")
            return False
    
    async def collect_user_posts(self, handle: str, limit: int = 10000) -> bool:
        """Collect user posts and their full, recursive interaction trees."""
        try:
            logger.info(f"Collecting posts for user: {handle}")
            posts_data = []
            cursor = None
            collected_count = 0
            
            while limit == 0 or collected_count < limit:
                batch_limit = min(100, limit - collected_count) if limit > 0 else 100
                await asyncio.sleep(self.rate_limit_delay)
                feed = self.client.get_user_feed(handle, batch_limit, cursor)
                if not feed or not feed.get('feed'):
                    break
                
                for post_view in feed['feed']:
                    post_data = await self._process_post_view(post_view, handle)
                    if post_data: # Filter out malformed posts
                        posts_data.append(post_data)

                collected_count += len(feed['feed'])
                cursor = feed.get('cursor')
                
                if not cursor:
                    logger.info("No more pages to fetch.")
                    break
                logger.info(f"Collected {collected_count} posts so far for user {handle}")

            user_filepath = Path(USERS_DIR) / f"{handle.replace('.', '_')}_posts.json"
            self.saver.save_json(posts_data, user_filepath)
            
            all_handles = self._extract_all_handles(posts_data, primary_handle=handle)
            await self._update_discovered_users(all_handles)
            
            return True
        except Exception as e:
            logger.error(f"Failed to collect posts for user {handle}: {e}", exc_info=True)
            return False

    async def _process_post_view(self, post_view: dict, current_user_handle: str = None) -> Optional[dict]:
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
        post_author_handle = author.get('handle')
        
        # Extract relationship data - this reflects how the current user interacted with this post
        is_repost, is_quote, is_reply, original_post_uri, original_post_author, parent_uri, root_uri = await self._extract_post_relations(post_view, record)
        
        # Determine if this is an original post by the current user (not a repost, quote, or reply)
        is_original_by_current_user = (not (is_repost or is_quote or is_reply) and 
                                     current_user_handle and 
                                     post_author_handle == current_user_handle)
        
        # Determine if we should get full interaction tree
        # Get full interaction tree if:
        # 1. This is an original post by the current user (not a repost, quote, or reply by current user)
        # 2. OR this is a reply/quote to an original post by the current user (recursive collection)
        # 3. OR this is a recursive collection (quotes/replies that should get full interactions)
        # 4. OR this is a keyword search and the post is original (not reply/quote/repost)
        should_get_full_interactions = (is_original_by_current_user or 
                                      (current_user_handle and 
                                       (is_reply or is_quote) and 
                                       post_author_handle != current_user_handle) or
                                      current_user_handle == "RECURSIVE_COLLECTION" or
                                      (current_user_handle == "KEYWORD_SEARCH" and 
                                       not (is_reply or is_quote or is_repost)))
        
        # For keyword search, we want to collect users from interactions but not get full recursive data
        # So we modify the logic to get basic interaction data for keyword search
        if current_user_handle == "KEYWORD_SEARCH":
            # For keyword search, get basic interaction data (counts and user lists) but not recursive replies/quotes
            should_get_full_interactions = False
            # Only get basic interactions for original posts (not replies/quotes/reposts)
            should_get_basic_interactions = not (is_reply or is_quote or is_repost)
            # For keyword search, original posts should get full recursive data (replies and quotes)
            should_get_recursive_for_original = not (is_reply or is_quote or is_repost)
        else:
            should_get_basic_interactions = False
            should_get_recursive_for_original = False
        
        if should_get_full_interactions:
            # Get full interaction tree for user's own original posts
            # This includes recursive collection of all interactions
            
            # Likes and Reposts: Get all users who liked/reposted (no recursion needed)
            likes_data = await self._get_interaction_data(self.client.get_post_likes, uri, cid, 'likes')
            reposts_data = await self._get_interaction_data(self.client.get_post_reposts, uri, cid, 'reposted_by')
            
            # Quotes: Get all quotes with their full interaction trees (recursive)
            quotes_data = await self._get_quotes_recursively(uri, cid)  # Already recursive
            
            # Replies: Get all replies with their full interaction trees (recursive)
            replies_data = await self._get_replies(uri)  # Already recursive
        elif should_get_basic_interactions:
            # For keyword search, get basic interaction data (user lists) but not recursive replies/quotes
            try:
                await asyncio.sleep(self.rate_limit_delay)
                # Get basic interaction data for user discovery
                likes_response = self.client.get_post_likes(uri=uri, cid=cid, limit=100)
                reposts_response = self.client.get_post_reposts(uri=uri, cid=cid, limit=100)
                quotes_response = self.client.get_post_quotes(uri=uri, cid=cid, limit=100)
                
                # Process user data for likes and reposts
                likes_data = []
                reposts_data = []
                if likes_response and likes_response.get('likes'):
                    for like_data in likes_response['likes']:
                        processed_like = self._process_user_data(like_data, 'likes')
                        if processed_like:
                            likes_data.append(processed_like)
                
                if reposts_response and reposts_response.get('reposted_by'):
                    for repost_data in reposts_response['reposted_by']:
                        processed_repost = self._process_user_data(repost_data, 'reposted_by')
                        if processed_repost:
                            reposts_data.append(processed_repost)
                
                # For keyword search, original posts should get recursive quotes/replies
                if should_get_recursive_for_original:
                    # Get recursive quotes and replies for original posts
                    quotes_data = await self._get_quotes_recursively(uri, cid)
                    replies_data = await self._get_replies(uri)
                else:
                    # For non-original posts, don't get recursive data
                    quotes_data = []
                    replies_data = []
                
                # Get counts
                likes_count = len(likes_data)
                reposts_count = len(reposts_data)
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

        # Debug logging
        logger.info(f"Entering _extract_post_relations with post_view type: {type(post_view)}")
        
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
        logger.info(f"Reason field: {reason}")
        if reason and isinstance(reason, dict):
            logger.info(f"Reason type: {reason.get('py_type')}")
            if reason.get('py_type') == 'app.bsky.feed.defs#reasonRepost':
                is_repost = True
                # For reposts, the original post is the one being reposted
                # The current post_view contains the original post
                post_in_view = post_view.get('post', {})
                original_post_uri = post_in_view.get('uri')
                original_post_author = post_in_view.get('author', {}).get('handle')
                logger.info(f"Detected repost: {original_post_uri} by {original_post_author}")
                # Don't check for quote if this is already a repost
                # The user's action is repost, not quote
                return is_repost, is_quote, is_reply, original_post_uri, original_post_author, parent_uri, root_uri
        
        # Check for quote (embed field in record) - only if not a repost
        embed = record.get('embed', {})
        logger.info(f"Embed field: {embed}")
        if isinstance(embed, dict):
            # Check for simple quote (app.bsky.embed.record)
            if embed.get('py_type') == 'app.bsky.embed.record':
                is_quote = True
                record_embed = embed.get('record', {})
                original_post_uri = record_embed.get('uri')
                original_post_author = record_embed.get('author', {}).get('handle')
                logger.info(f"Detected quote: {original_post_uri} by {original_post_author}")
                
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
                                logger.info(f"Detected quote with media: {original_post_uri} by {original_post_author}")
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
        logger.info(f"Reply field: {reply_field}")
        if isinstance(record, dict) and reply_field:
            is_reply = True
            reply_ref = record.get('reply', {})
            parent_uri = reply_ref.get('parent', {}).get('uri')
            root_uri = reply_ref.get('root', {}).get('uri')
            logger.info(f"Detected reply: parent={parent_uri}, root={root_uri}")

        logger.info(f"Final result: is_repost={is_repost}, is_quote={is_quote}, is_reply={is_reply}")
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

    async def _get_quotes_recursively(self, uri: str, cid: str) -> list:
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
                        processed_quote = await self._process_post_view(post_view, "RECURSIVE_COLLECTION")
                        if processed_quote:
                            quotes.append(processed_quote)
                
                cursor = response.get('cursor')
                if not cursor or not posts_list:
                    break
            except Exception as e:
                logger.warning(f"Failed to get quotes for {uri}: {e}")
                break
        return quotes

    async def _get_replies(self, uri: str) -> list:
        """Get all replies for a given post uri by fetching the thread with full interaction trees."""
        if not uri: return []
        try:
            await asyncio.sleep(self.rate_limit_delay)
            thread = self.client.get_post_thread(uri, depth=10)
            if thread and isinstance(thread, dict) and thread.get('thread'):
                return await self._extract_replies_from_thread(thread['thread'])
        except Exception as e:
            logger.warning(f"Failed to get reply thread for {uri}: {e}")
        return []

    async def _extract_replies_from_thread(self, thread_data: dict) -> list:
        """Recursively extract replies from a thread structure with full interaction trees."""
        replies = []
        if isinstance(thread_data, dict) and 'replies' in thread_data:
            for reply_view in thread_data.get('replies', []):
                if isinstance(reply_view, dict) and 'post' in reply_view:
                    # Process reply with full interaction tree (recursive)
                    # Pass a special flag to indicate this is a quote/reply that should get full interactions
                    reply_data = await self._process_post_view(reply_view, "RECURSIVE_COLLECTION")
                    if reply_data: 
                        replies.append(reply_data)
        return replies

    def collect_suggested_users(self, limit: int = 100) -> bool:
        logger.warning("collect_suggested_users is a placeholder.")
        return True

    async def _update_discovered_users(self, discovered_users: set):
        if not discovered_users:
            return
        self.saver.save_discovered_users(discovered_users, Path(DISCOVERED_USERS_PATH))

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

async def main():
    """Main function to handle command line arguments and execute data collection"""
    parser = argparse.ArgumentParser(description="Bluesky Comprehensive Data Collection Tool")
    
    parser.add_argument('--keyword', help='Search for posts containing this keyword')
    parser.add_argument('--keywords', help='Comma-separated list of keywords to search')
    parser.add_argument('--user', help='User handle to collect')
    parser.add_argument('--user-profile', action='store_true', help='Collect only user profile')
    parser.add_argument('--user-posts', action='store_true', help='Collect only user posts')
    parser.add_argument('--user-all', action='store_true', help='Collect both user profile and posts')
    parser.add_argument('--popular', action='store_true', help='Collect popular content')
    parser.add_argument('--suggested', action='store_true', help='Collect suggested users')
    parser.add_argument('--limit', type=int, help='Maximum number of items to collect (0 for unlimited)')
    
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

    if args.popular:
        limit = args.limit or DEFAULT_POPULAR_LIMIT
        tasks.append(asyncio.to_thread(collector.collect_popular_content, limit))
    
    if args.suggested:
        limit = args.limit or DEFAULT_SUGGESTED_LIMIT
        tasks.append(asyncio.to_thread(collector.collect_suggested_users, limit))
    
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

if __name__ == "__main__":
    asyncio.run(main())