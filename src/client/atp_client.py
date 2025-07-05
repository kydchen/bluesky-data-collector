"""
ATP (Authenticated Transfer Protocol) Client for Bluesky
Handles authentication and API interactions with Bluesky
"""

import os
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
    
    def get_user_feed(self, handle: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.feed.get_author_feed({'actor': handle, 'limit': limit, 'cursor': cursor})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get feed for {handle}: {str(e)}")
            return None
    
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
        try:
            response = self.client.app.bsky.feed.get_post_thread({'uri': uri, 'depth': depth})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get post thread for {uri}: {str(e)}")
            return None
    
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
        return {
            "authenticated": True,
            "username": self.username,
            "host": self.host,
            "session": self.client.session
        }
    
    def close(self):
        """Close the client session (sync Client doesn't need explicit close)."""
        self.is_authenticated = False
    
    def search_posts(self, query: str, limit: int = 50, cursor: Optional[str] = None, 
                    author: Optional[str] = None, domain: Optional[str] = None, 
                    lang: Optional[str] = None, mentions: Optional[str] = None,
                    tag: Optional[str] = None, url: Optional[str] = None,
                    since: Optional[str] = None, until: Optional[str] = None,
                    sort: Optional[str] = None) -> Optional[dict]:
        """Search posts with advanced filtering options"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
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
                
            response = self.client.app.bsky.feed.search_posts(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to search posts for '{query}': {str(e)}")
            return None
    
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
        try:
            params = {'uri': uri, 'cid': cid, 'limit': limit}
            if cursor:
                params['cursor'] = cursor
            response = self.client.app.bsky.feed.get_likes(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get likes for post {uri}: {str(e)}")
            return None
    
    def get_post_reposts(self, uri: str, cid: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            params = {'uri': uri, 'cid': cid, 'limit': limit}
            if cursor:
                params['cursor'] = cursor
            response = self.client.app.bsky.feed.get_reposted_by(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get reposts for post {uri}: {str(e)}")
            return None
    
    def get_post_quotes(self, uri: str, cid: str, limit: int = 50, cursor: Optional[str] = None) -> Optional[dict]:
        """Get quotes (reposts with comments) for a specific post"""
        if limit > 100:
            limit = 100
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            params = {'uri': uri, 'cid': cid, 'limit': limit}
            if cursor:
                params['cursor'] = cursor
            response = self.client.app.bsky.feed.get_quotes(params)
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get quotes for post {uri}: {str(e)}")
            return None
    
    def get_posts(self, uris: List[str]) -> Optional[dict]:
        """Get multiple posts by their URIs"""
        if not self.is_authenticated:
            self.logger.error("Not authenticated")
            return None
        try:
            response = self.client.app.bsky.feed.get_posts({'uris': uris})
            return response.model_dump() if hasattr(response, 'model_dump') else response
        except Exception as e:
            self.logger.error(f"Failed to get posts for URIs {uris}: {str(e)}")
            return None 