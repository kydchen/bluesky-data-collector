# Bluesky Data Collection Tool

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Status: Beta](https://img.shields.io/badge/Status-Beta-orange.svg)](https://github.com/yourusername/bluesky-data-collector)

A tool for collecting data from the Bluesky social platform using the ATP (Authenticated Transfer Protocol) API.

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/bluesky-data-collector.git
cd bluesky-data-collector

# Install dependencies
pip install -r requirements.txt

# Configure your credentials
cp config.env.example config.env
# Edit config.env with your Bluesky credentials

# Start collecting data
python main.py --keyword "python" --limit 100
```

## Features

### ✅ Implemented Features
- **Keyword-based Search**: Collect posts containing specific keywords with advanced filtering options
- **User-specific Data**: Collect user profiles and posts with full interaction trees
- **User Discovery**: Automatically discover and track users from interaction data
- **Rate Limiting**: Built-in API rate limiting to respect Bluesky's limits
- **Pagination Support**: Collect large datasets efficiently with cursor-based pagination
- **Advanced Search Filters**: Support for author, domain, language, mentions, tags, URLs, time ranges, and sorting
- **Data Deduplication**: Automatic removal of duplicate posts in search results
- **Comprehensive Logging**: Detailed logging for monitoring collection progress and debugging

### 🚧 Partially Implemented Features
- **Popular Content**: Basic placeholder (not fully implemented)
- **Suggested Users**: Basic placeholder (not fully implemented)

### 📊 Data Export
- **JSON Format**: Structured data export for further processing
- **Error Handling**: Robust error handling with graceful degradation
- **Full Interaction Trees**: Complete likes, reposts, replies, and quotes data for user posts
- **User Discovery Tracking**: Automatic discovery and tracking of users from interaction data
- **Performance Optimization**: Intelligent caching for quoted posts to reduce API calls
- **Asynchronous Processing**: Efficient async/await pattern for concurrent API operations
- **Concurrent Collection**: Support for collecting multiple data types simultaneously
- **Robust Data Saving**: Automatic directory creation and error handling for data persistence

## Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd Bluesky
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

**Key Dependencies:**
- `atproto`: Bluesky ATP (Authenticated Transfer Protocol) client
- `python-dotenv`: Environment variable management
- `aiohttp`: Asynchronous HTTP client/server
- `pandas` & `numpy`: Data processing and analysis
- `rich`: Enhanced terminal output and progress bars

4. **Configure environment variables**
```bash
cp config.env.example config.env
# Edit config.env with your Bluesky credentials and collection settings
```

## Configuration

Create a `config.env` file with your Bluesky credentials and collection settings:

```env
# Required: Your Bluesky account credentials
BLUESKY_USERNAME=your_username.bsky.social
BLUESKY_PASSWORD=your_password
BLUESKY_APP_PASSWORD=your_app_password  # Optional (if you have 2FA enabled)

# --- Data Collection Settings ---
# Default collection limits
DEFAULT_KEYWORD_LIMIT=1000
DEFAULT_POPULAR_LIMIT=500
DEFAULT_SUGGESTED_LIMIT=100
DEFAULT_USER_POSTS_LIMIT=10000

# --- API Settings ---
# Delay between API requests in seconds (e.g., 0.1 for 100ms)
RATE_LIMIT_DELAY=0.1

# --- File Paths ---
# Main directory for all collected data
DATA_DIR=data
```

### Configuration Priority

The tool uses the following priority for configuration:
1. Command line arguments (highest priority)
2. Environment variables from `config.env`
3. Default values (lowest priority)

All configuration values can be overridden via command line arguments.

## Usage

### Basic Usage

```bash
# Collect data by keyword (supports pagination)
python main.py --keyword "Bitcoin" --limit 10000

# Collect only user profile
python main.py --user "username.bsky.social" --user-profile

# Collect only user posts with full interaction trees (default: 10000 posts, use --limit 0 for unlimited)
python main.py --user "username.bsky.social" --user-posts

# Collect both user profile and posts with full interaction trees (default: 10000 posts, use --limit 0 for unlimited)
python main.py --user "username.bsky.social" --user-all
```

### Advanced Usage

```bash
# Collect multiple keywords simultaneously
python main.py --keywords "python,ai,machine learning" --limit 500

# Collect large datasets with pagination
python main.py --keyword "tech" --limit 5000

# Search with time range
python main.py --keyword "AI" --since "2024-01-01T00:00:00Z" --until "2024-12-31T23:59:59Z"

# Collect ALL posts for a user (unlimited)
python main.py --user "username.bsky.social" --user-posts --limit 0

# Collect with custom limit
python main.py --user "username.bsky.social" --user-posts --limit 5000


```

## Data Structure

### Keyword Search Data Structure

When collecting posts by keyword, the tool generates a structured dataset with the following format:

```json
{
  "search_metadata": {
    "keyword": "ai",
    "total_results": 20,
    "recursion_strategy": "original_only",
    "collected_at": "2025-07-05T20:17:17.109059"
  },
  "posts": [
    {
      "uri": "at://did:plc:example/app.bsky.feed.post/123",
      "cid": "bafyrei...",
      "author_did": "did:plc:example",
      "author_handle": "username.bsky.social",
      "text": "Post content here...",
      "url": "https://bsky.app/profile/username.bsky.social/post/123",
      "created_at": "2025-07-05T12:00:00.000Z",
      "indexed_at": "2025-07-05T12:00:01.000Z",
      "reply_count": 5,
      "repost_count": 10,
      "like_count": 25,
      "quote_count": 3,
      "is_reply": false,
      "is_repost": false,
      "is_quote": false,
      "parent_uri": null,
      "root_uri": null,
      "original_post_uri": null,
      "original_post_author": null,
      "quoted_post_info": null,
      "likes": [],
      "reposts": [],
      "replies": [],
      "quotes": [],
      "search_keyword": "ai"
    }
  ],
  "topic_participants": [
    {
      "handle": "user1.bsky.social",
      "did": "did:plc:user1",
      "displayName": "User One"
    }
  ]
}
```

#### Keyword Search Strategy: "Original Only"

The keyword search uses an "original_only" recursion strategy:

- **Original Posts**: Get full recursive interaction data (replies and quotes with their complete interaction trees)
- **Reply/Quote/Repost Posts**: Only get basic interaction counts and user lists (no recursive data)
- **User Discovery**: Collect all users from likes and reposts for topic participant analysis
- **Deduplication**: Automatically remove duplicate posts from search results

This strategy balances comprehensive data collection with performance and API efficiency.

### User Posts Data Structure

When collecting user posts with `--user-posts`, the tool generates comprehensive data including full interaction trees. Here's the detailed structure:

```json
{
  "uri": "at://did:plc:example/app.bsky.feed.post/123",
  "cid": "bafyrei...",
  "author_did": "did:plc:example",
  "author_handle": "username.bsky.social",
  "text": "Post content here...",
  "url": "https://bsky.app/profile/username.bsky.social/post/123",
  "created_at": "2025-07-02T12:00:00.000Z",
  "indexed_at": "2025-07-02T12:00:01.000Z",
  "reply_count": 5,
  "repost_count": 10,
  "like_count": 25,
  "quote_count": 3,
  "is_reply": false,
  "is_repost": false,
  "is_quote": false,
  "parent_uri": null,
  "root_uri": null,
  "original_post_uri": null,
  "original_post_author": null,
  "quoted_post_info": null,
  "likes": [
    {
      "did": "did:plc:user1",
      "handle": "user1.bsky.social",
      "displayName": "User One",
      "indexedAt": "2025-07-02T12:30:00.000Z",
      "labels": [],
      "interaction_type": "likes"
    }
  ],
  "reposts": [
    {
      "did": "did:plc:user2",
      "handle": "user2.bsky.social",
      "displayName": "User Two",
      "indexedAt": "2025-07-02T13:00:00.000Z",
      "labels": [],
      "interaction_type": "reposted_by"
    }
  ],
  "replies": [
    {
      "uri": "at://did:plc:user3/app.bsky.feed.post/456",
      "cid": "bafyrei...",
      "author_did": "did:plc:user3",
      "author_handle": "user3.bsky.social",
      "text": "Reply content...",
      "url": "https://bsky.app/profile/user3.bsky.social/post/456",
      "created_at": "2025-07-02T14:00:00.000Z",
      "indexed_at": "2025-07-02T14:00:01.000Z",
      "reply_count": 0,
      "repost_count": 0,
      "like_count": 2,
      "quote_count": 0,
      "is_reply": true,
      "is_repost": false,
      "is_quote": false,
      "parent_uri": "at://did:plc:example/app.bsky.feed.post/123",
      "root_uri": "at://did:plc:example/app.bsky.feed.post/123",
      "original_post_uri": null,
      "original_post_author": null,
      "quoted_post_info": null,
      "likes": [...],
      "reposts": [...],
      "replies": [...],
      "quotes": [...]
    }
  ],
  "quotes": [
    {
      "uri": "at://did:plc:user4/app.bsky.feed.post/789",
      "cid": "bafyrei...",
      "author_did": "did:plc:user4",
      "author_handle": "user4.bsky.social",
      "text": "Quote content...",
      "url": "https://bsky.app/profile/user4.bsky.social/post/789",
      "created_at": "2025-07-02T15:00:00.000Z",
      "indexed_at": "2025-07-02T15:00:01.000Z",
      "reply_count": 1,
      "repost_count": 0,
      "like_count": 5,
      "quote_count": 0,
      "is_reply": false,
      "is_repost": false,
      "is_quote": true,
      "parent_uri": null,
      "root_uri": null,
      "original_post_uri": "at://did:plc:example/app.bsky.feed.post/123",
      "original_post_author": "username.bsky.social",
      "quoted_post_info": {
        "author_handle": "username.bsky.social",
        "author_did": "did:plc:example",
        "text": "Original post content...",
        "created_at": "2025-07-02T12:00:00.000Z",
        "uri": "at://did:plc:example/app.bsky.feed.post/123"
      },
      "likes": [...],
      "reposts": [...],
      "replies": [...],
      "quotes": [...]
    }
  ]
}
```

### Field Descriptions

#### Post Metadata
- **uri**: Unique identifier for the post
- **cid**: Content identifier (hash)
- **author_did**: Author's decentralized identifier
- **author_handle**: Author's username
- **text**: Post content
- **url**: Web URL for the post
- **created_at**: When the post was created
- **indexed_at**: When the post was indexed by Bluesky

#### Engagement Counts
- **reply_count**: Number of replies to this post
- **repost_count**: Number of reposts of this post
- **like_count**: Number of likes on this post
- **quote_count**: Number of quotes of this post

#### Post Type Indicators (Relative to Target User)
- **is_reply**: True if this post is a reply by the target user to another post
- **is_repost**: True if this post is a repost by the target user of another post
- **is_quote**: True if this post is a quote by the target user of another post

**Note**: These fields indicate the relationship between the post and the user whose data we're collecting, not the post's inherent properties.

#### Relationship Fields
- **parent_uri**: URI of the immediate parent post (for replies) - the post this reply is responding to
- **root_uri**: URI of the root/original post in the thread (for replies) - the first post in the conversation chain
- **original_post_uri**: URI of the original post being reposted/quoted (for reposts/quotes)
- **original_post_author**: Handle of the original post author (for reposts/quotes)
- **quoted_post_info**: Complete details about the quoted post (for quotes only), including:
  - **author_handle**: Handle of the quoted post's author
  - **author_did**: DID of the quoted post's author
  - **text**: Content of the quoted post
  - **created_at**: When the quoted post was created
  - **uri**: URI of the quoted post

#### Interaction Data
- **likes**: Array of users who liked the post (with user details)
- **reposts**: Array of users who reposted the post (with user details)
- **replies**: Array of reply posts (with full interaction trees for user posts, empty for keyword search)
- **quotes**: Array of quote posts (with full interaction trees for user posts, empty for keyword search)

#### User Data in Interactions
Each user in likes/reposts arrays contains optimized, essential fields only:
- **did**: User's decentralized identifier
- **handle**: User's username
- **displayName**: User's display name (normalized from API's display_name)
- **indexedAt**: When the user was indexed (normalized from API's indexed_at)
- **labels**: Any labels applied to the user
- **interaction_type**: Type of interaction ("likes" or "reposted_by")

**Note**: User data is automatically processed to handle different API response structures (actor field vs direct user data) and normalized for consistency.

### File Organization

```
data/
├── search/                 # Keyword-based search results
│   ├── search_bitcoin.json
│   ├── search_ai.json
│   ├── search_python.json
│   └── search_machine_learning.json
├── users/                  # User-specific data
│   ├── username_bsky_social_profile.json
│   ├── username_bsky_social_posts.json
│   └── discovered_users.json    # Automatically discovered users from interactions
├── popular_feed.json       # Popular content data (placeholder)
└── suggested_users.json    # Suggested users data (placeholder)
```

## API Rate Limits

The tool respects Bluesky's API rate limits:
- **Authentication**: 3000 requests per 5 minutes
- **Content Retrieval**: 3000 requests per 5 minutes
- **Search Operations**: 1000 requests per 5 minutes

The tool automatically implements rate limiting (100ms delay between requests) and pagination to collect large datasets efficiently.

## Pagination Support

All collection methods support pagination:
- **Keyword Search**: Collects posts in batches of 100 with cursor-based pagination
- **User Data**: Collects user posts with pagination (default: 10000, use --limit 0 for unlimited)

### Data Collection Limits

- **Default Limits**: 
  - User posts: 10,000 posts (can be overridden with --limit 0 for unlimited)
  - Keyword search: 1,000 posts
  - Popular content: 500 posts (placeholder)
  - Suggested users: 100 users (placeholder)

- **API Batch Size**: All methods use batch size of 100 per API request for optimal performance

- **User Posts Collection**: 
  - Uses `posts_with_replies` filter by default (includes user's posts and replies)
  - May collect more posts than profile shows due to inclusion of replies and other content types
  - Supports unlimited collection with `--limit 0` parameter
  - **Full Interaction Trees**: Includes complete likes, reposts, replies, and quotes data for each post

### User Discovery Feature

The tool automatically discovers and tracks users from interaction data:

- **Automatic Discovery**: When collecting user posts, the tool automatically extracts all user handles from:
  - Users who liked the posts
  - Users who reposted the posts  
  - Users who replied to the posts
  - Users who quoted the posts
  - Authors of quoted posts

- **Persistent Tracking**: Discovered users are saved to `data/users/discovered_users.json` and updated incrementally
- **Duplicate Prevention**: The system prevents duplicate entries and only adds new users
- **Centralized Storage**: All discovered users are stored in a single JSON file for easy access and analysis

## Data Privacy & Ethics

- **Respect Privacy**: Only collect publicly available data
- **Rate Limiting**: Respect API limits and server resources
- **Data Usage**: Use collected data responsibly and ethically
- **Compliance**: Follow Bluesky's Terms of Service

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool is for educational and research purposes. Users are responsible for complying with Bluesky's Terms of Service and applicable laws when using this tool.

## Support

For issues and questions:
- Create an issue on GitHub
- Check the documentation
- Review the examples in the README

### Error Handling and Logging

The tool provides comprehensive error handling and logging:

- **Graceful Degradation**: Individual API failures don't stop the entire collection process
- **Detailed Logging**: Progress updates, error messages, and success confirmations
- **Exception Tracking**: Failed operations are logged with full error details
- **Success Reporting**: Summary of successful vs failed operations at completion 
