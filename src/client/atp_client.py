"""
ATP (Authenticated Transfer Protocol) Client for Bluesky
Handles authentication and API interactions with Bluesky
"""

import os
import json
from typing import Optional, Dict, Any, List
from atproto import Client
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class ATPClient:
    """ATP Client for Bluesky API interactions"""
    
    def __init__(self):
        self.client = Client()
        self.username: Optional[str] = None
        self.password: Optional[str] = None
        self.app_password: Optional[str] = None
        self.host: str = "https://bsky.social"
        self.is_authenticated = False
        self.profile = None
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Define fields that we don't need for text data collection
        self.UNNEEDED_FIELDS = {
            'aspectRatio',  # Image aspect ratio - we don't need images
            'thumb',        # Thumbnail images
            'image',        # Image data
            'alt',          # Image alt text
            'embed',        # Rich media embeds (we extract text separately)
            'via',          # Via field
            'subject',      # Subject field
            'created_at',   # We use createdAt instead
            'indexed_at',   # We use indexedAt instead
        }
        
        # Define critical fields that we must preserve
        self.CRITICAL_FIELDS = {
            'uri', 'cid', 'text', 'author', 'handle', 'did', 'displayName',
            'createdAt', 'indexedAt', 'reply_count', 'repost_count', 
            'like_count', 'quote_count', 'likes', 'reposts', 'replies', 'quotes',
            'labels', 'facets', 'reply', 'tags', 'langs', 'entities'
        }
    
    def _clean_unneeded_fields(self, data: Any, path: str = "") -> Any:
        """
        Recursively clean fields that we don't need for text data collection.
        This prevents validation errors while preserving all essential data.
        """
        if isinstance(data, dict):
            cleaned = {}
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                
                # Skip unneeded fields entirely
                if key in self.UNNEEDED_FIELDS:
                    self.logger.debug(f"Removing unneeded field: {current_path}")
                    continue
                
                # Clean problematic aspectRatio fields specifically
                if key == 'aspectRatio' and isinstance(value, dict):
                    if value.get('$type') == 'app.bsky.embed.images#aspectRatio':
                        self.logger.debug(f"Removing problematic aspectRatio field: {current_path}")
                        continue
                
                # Recursively clean other fields
                cleaned[key] = self._clean_unneeded_fields(value, current_path)
            return cleaned
        elif isinstance(data, list):
            return [self._clean_unneeded_fields(item, f"{path}[{i}]") for i, item in enumerate(data)]
        else:
            return data
    
    def _make_raw_request_with_cleaning(self, method_name: str, params: dict) -> Optional[dict]:
        """
        Make a raw HTTP request and clean problematic fields before processing.
        This is a more elegant solution than the current direct HTTP approach.
        """
        try:
            import httpx
            
            # Build the URL
            url = f"{self.host}/xrpc/{method_name}"
            
            # Get session headers from the authenticated client
            headers = {}
            
            # Try multiple ways to get the access JWT
            access_jwt = None
            
            # Method 1: Direct access from session
            if hasattr(self.client, 'session') and self.client.session:
                if hasattr(self.client.session, 'access_jwt'):
                    access_jwt = self.client.session.access_jwt
                elif hasattr(self.client.session, 'data') and self.client.session.data:
                    if hasattr(self.client.session.data, 'access_jwt'):
                        access_jwt = self.client.session.data.access_jwt
            
            # Method 2: Try to get from profile if available
            if not access_jwt and self.profile:
                if hasattr(self.profile, 'access_jwt'):
                    access_jwt = self.profile.access_jwt
                elif isinstance(self.profile, dict) and 'access_jwt' in self.profile:
                    access_jwt = self.profile['access_jwt']
            
            # Method 3: Try to get from client's internal state
            if not access_jwt and hasattr(self.client, '_session'):
                if hasattr(self.client._session, 'access_jwt'):
                    access_jwt = self.client._session.access_jwt
            
            # Set Authorization header if we found the JWT
            if access_jwt:
                headers['Authorization'] = f'Bearer {access_jwt}'
                self.logger.debug(f"Using access JWT for raw HTTP request")
            else:
                self.logger.warning(f"No access JWT found for raw HTTP request")
            
            # Add User-Agent
            headers['User-Agent'] = 'Bluesky-Data-Collector/1.0'
            
            # Make direct HTTP request
            with httpx.Client() as http_client:
                response = http_client.get(url, params=params, headers=headers)
                response.raise_for_status()
                
                # Parse JSON response
                raw_data = response.json()
                
                # Clean problematic fields
                cleaned_data = self._clean_unneeded_fields(raw_data)
                
                self.logger.info(f"Raw HTTP request successful for {method_name}")
                return cleaned_data
                
        except Exception as e:
            self.logger.error(f"Failed to make raw HTTP request for {method_name}: {str(e)}")
            return None

    def _try_atp_with_fallback(self, method_name: str, params: dict, api_method) -> Optional[dict]:
        """
        Try ATP client first, fall back to raw HTTP with cleaning if validation fails.
        This provides the best of both worlds: ATP convenience when possible, raw HTTP when needed.
        """
        try:
            # First try with ATP client
            response = api_method(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            # If validation fails, use raw HTTP with cleaning
            self.logger.warning(f"ATP validation error for {method_name}: {str(e)}")
            self.logger.info(f"Falling back to raw HTTP with field cleaning for {method_name}...")
            
            fallback_result = self._make_raw_request_with_cleaning(method_name, params)
            if fallback_result:
                return fallback_result
            else:
                # If both methods fail, log the error but don't crash
                self.logger.error(f"Both ATP client and raw HTTP failed for {method_name}")
                return None

    def authenticate(self, username, password, app_password=None):
        """Authenticate with Bluesky using username and password (sync version)"""
        try:
            self.username = username
            self.password = password
            self.app_password = app_password
            
            # Try with regular password first
            try:
                profile = self.client.login(username, password)
                self.logger.info("Authentication successful with regular password")
            except Exception as e1:
                self.logger.info(f"Regular password failed: {e1}")
                # Try with app password if provided
                if app_password:
                    try:
                        profile = self.client.login(username, app_password)
                        self.logger.info("Authentication successful with app password")
                    except Exception as e2:
                        self.logger.error(f"App password also failed: {e2}")
                        raise e2
                else:
                    raise e1
            
            self.is_authenticated = True
            self.profile = profile
            return True
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            self.logger.error(f"Username: {username}")
            self.logger.error(f"Using app password: {app_password is not None}")
            self.logger.error(f"Error type: {type(e)}")
            self.logger.error(f"Error details: {str(e)}")
            self.is_authenticated = False
            return False
    
    def get_timeline(self, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.feed.get_timeline({'limit': limit, 'cursor': cursor})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get timeline: {str(e)}")
            return None
    
    def get_user_profile(self, handle: str) -> Optional[dict]:
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.actor.get_profile({'actor': handle})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get profile for {handle}: {str(e)}")
            return None
    
    def get_user_feed(self, handle: str, limit: int = 50, cursor: Optional[str] = None, 
                     since: Optional[str] = None, until: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'actor': handle, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        if since:
            params['since'] = since
        if until:
            params['until'] = until
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getAuthorFeed', params, self.client.app.bsky.feed.get_author_feed)
    
    def get_follows(self, handle: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.graph.get_follows({'actor': handle, 'limit': limit, 'cursor': cursor})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get follows for {handle}: {str(e)}")
            return None
    
    def get_followers(self, handle: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.graph.get_followers({'actor': handle, 'limit': limit, 'cursor': cursor})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get followers for {handle}: {str(e)}")
            return None
    
    def get_post_thread(self, uri: str, depth: int = 6) -> Optional[dict]:
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'uri': uri, 'depth': depth}
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getPostThread', params, self.client.app.bsky.feed.get_post_thread)
    
    def get_notifications(self, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.notification.list_notifications({'limit': limit, 'cursor': cursor})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get notifications: {str(e)}")
            return None
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get current session information."""
        if not self.is_authenticated or not self.client:
            return {"authenticated": False}
        
        session_info = {
            "authenticated": True,
            "username": self.username,
            "host": self.host
        }
        
        # Try to get session info safely
        try:
            if hasattr(self.client, 'session'):
                session_info["session"] = self.client.session
            elif hasattr(self.client, '_session'):
                session_info["session"] = self.client._session
        except Exception as e:
            session_info["session_error"] = str(e)
        
        return session_info
    
    def close(self):
        """Close the client session (sync Client doesn't need explicit close)."""
        self.is_authenticated = False
    
    def search_posts(self, query: str, limit: int = 50, cursor: Optional[str] = None, 
                    author: Optional[str] = None, domain: Optional[str] = None, 
                    lang: Optional[str] = None, mentions: Optional[str] = None,
                    tag: Optional[str] = None, url: Optional[str] = None,
                    since: Optional[str] = None, until: Optional[str] = None,
                    sort: Optional[str] = None) -> Optional[dict]:
        """Search posts with advanced filtering options using robust fallback strategy"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        # Build parameters
        params = {'q': query, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        if author:
            params['author'] = author
        if domain:
            params['domain'] = domain
        if lang:
            params['lang'] = lang
        if mentions:
            params['mentions'] = mentions
        if tag:
            params['tag'] = tag
        if url:
            params['url'] = url
        if since:
            params['since'] = since
        if until:
            params['until'] = until
        if sort:
            params['sort'] = sort
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.searchPosts', params, self.client.app.bsky.feed.search_posts)
    
    def get_suggestions(self, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            # Get handle from logged-in profile
            if hasattr(self.profile, 'handle'):
                actor = self.profile.handle
            elif isinstance(self.profile, dict) and 'handle' in self.profile:
                actor = self.profile['handle']
            else:
                self.logger.error("No handle found in profile")
                return None
                
            params = {'actor': actor, 'limit': limit}
            if cursor:
                params['cursor'] = cursor
            response = self.client.app.bsky.graph.get_suggested_follows_by_actor(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get suggested follows: {str(e)}")
            return None
    
    def get_popular_feed(self, limit: int = 50) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.feed.get_timeline({'algorithm': 'whatshot', 'limit': limit})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get popular feed: {str(e)}")
            return None
    
    def get_my_profile(self) -> Optional[dict]:
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            # Get handle from logged-in profile
            if hasattr(self.profile, 'handle'):
                actor = self.profile.handle
            elif isinstance(self.profile, dict) and 'handle' in self.profile:
                actor = self.profile['handle']
            else:
                self.logger.error("No handle found in profile")
                return None
                
            response = self.client.app.bsky.actor.get_profile({'actor': actor})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get my profile: {str(e)}")
            return None
    
    def get_post_likes(self, uri: str, cid: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'uri': uri, 'cid': cid, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getLikes', params, self.client.app.bsky.feed.get_likes)
    
    def get_post_reposts(self, uri: str, cid: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'uri': uri, 'cid': cid, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getRepostedBy', params, self.client.app.bsky.feed.get_reposted_by)
    
    def get_post_quotes(self, uri: str, cid: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        """Get quotes (reposts with comments) for a specific post"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'uri': uri, 'cid': cid, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getQuotes', params, self.client.app.bsky.feed.get_quotes)
    
    def get_posts(self, uris: List[str]) -> Optional[dict]:
        """Get multiple posts by their URIs"""
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'uris': uris}
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getPosts', params, self.client.app.bsky.feed.get_posts)

    # --- Feed Discovery and Collection Methods ---
    
    def get_suggested_feeds(self, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        """Get suggested feeds for discovery"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'limit': limit}
        if cursor:
            params['cursor'] = cursor
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getSuggestedFeeds', params, self.client.app.bsky.feed.get_suggested_feeds)
    
    def get_feed_generators(self, feeds: List[str]) -> Optional[dict]:
        """Get feed generators by their URIs"""
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.feed.get_feed_generators({'feeds': feeds})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get feed generators for {feeds}: {str(e)}")
            return None
    
    def get_feed_generator(self, feed: str) -> Optional[dict]:
        """Get a specific feed generator"""
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.feed.get_feed_generator({'feed': feed})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get feed generator {feed}: {str(e)}")
            return None
    
    def describe_feed_generator(self, feed: str) -> Optional[dict]:
        """Describe a feed generator"""
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.feed.describe_feed_generator({'feed': feed})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to describe feed generator {feed}: {str(e)}")
            return None
    
    def get_feed(self, feed: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        """Get content from a specific feed"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        params = {'feed': feed, 'limit': limit}
        if cursor:
            params['cursor'] = cursor
        
        # Use the robust fallback strategy
        return self._try_atp_with_fallback('app.bsky.feed.getFeed', params, self.client.app.bsky.feed.get_feed)
    
    def get_feed_skeleton(self, feed: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        """Get feed skeleton (lightweight version)"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            params = {'feed': feed, 'limit': limit}
            if cursor:
                params['cursor'] = cursor
            response = self.client.app.bsky.feed.get_feed_skeleton(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get feed skeleton {feed}: {str(e)}")
            return None
    
    def get_actor_feeds(self, actor: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        """Get feeds created by a specific actor"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            params = {'actor': actor, 'limit': limit}
            if cursor:
                params['cursor'] = cursor
            response = self.client.app.bsky.feed.get_actor_feeds(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get actor feeds for {actor}: {str(e)}")
            return None
    
    def get_all_suggested_feeds(self, max_feeds: int = 100) -> Optional[dict]:
        """Get all suggested feeds by making multiple API calls (up to max_feeds, or unlimited if max_feeds is 0)"""
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        
        try:
            all_feeds = []
            total_collected = 0
            api_calls_made = 0
            
            # First try the unspecced API which might have more feeds
            try:
                self.logger.info("Trying unspecced API for suggested feeds...")
                unspecced_response = self.client.app.bsky.unspecced.get_suggested_feeds({'limit': 25})
                unspecced_dict = unspecced_response.model_dump() if hasattr(unspecced_response, 'model_dump') else unspecced_response
                api_calls_made += 1
                
                if unspecced_dict and unspecced_dict.get('feeds'):
                    unspecced_feeds = unspecced_dict.get('feeds', [])
                    self.logger.info(f"Unspecced API returned {len(unspecced_feeds)} feeds")
                    
                    # Add feeds from unspecced API
                    for feed in unspecced_feeds:
                        if isinstance(feed, dict) and feed.get('uri'):
                            if not any(existing.get('uri') == feed.get('uri') for existing in all_feeds):
                                all_feeds.append(feed)
                                total_collected += 1
                else:
                    self.logger.info("Unspecced API returned no feeds")
            except Exception as e:
                self.logger.warning(f"Unspecced API failed: {e}")
            
            # Then try the regular API
            self.logger.info("Trying regular API for suggested feeds...")
            while max_feeds == 0 or total_collected < max_feeds:
                # For unlimited collection, always request 25 feeds per call
                # For limited collection, request the remaining amount
                batch_size = 25 if max_feeds == 0 else min(25, max_feeds - total_collected)
                
                response = self.client.app.bsky.feed.get_suggested_feeds({'limit': batch_size})
                # Convert response to dict if it's a Response object
                response_dict = response.model_dump() if hasattr(response, 'model_dump') else response
                api_calls_made += 1
                
                if not response_dict or not response_dict.get('feeds'):
                    break
                
                batch_feeds = response_dict.get('feeds', [])
                if not batch_feeds:
                    break  # No more feeds available
                
                # Add new feeds (avoid duplicates)
                new_feeds_added = 0
                for feed in batch_feeds:
                    if isinstance(feed, dict) and feed.get('uri'):
                        # Check if we already have this feed
                        if not any(existing.get('uri') == feed.get('uri') for existing in all_feeds):
                            all_feeds.append(feed)
                            new_feeds_added += 1
                
                total_collected = len(all_feeds)
                
                if max_feeds == 0:
                    self.logger.info(f"Collected {total_collected} unique feeds so far (unlimited mode)")
                else:
                    self.logger.info(f"Collected {total_collected}/{max_feeds} unique feeds so far")
                
                # If we got fewer feeds than requested or no new feeds were added, we've reached the end
                if len(batch_feeds) < batch_size or new_feeds_added == 0:
                    self.logger.info("No more feeds available from API")
                    break
            
            return {
                'feeds': all_feeds,
                'total_collected': len(all_feeds),
                'api_calls_made': api_calls_made
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get all suggested feeds: {str(e)}")
            return None 