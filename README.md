# Bluesky Data Collection Tool

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Status: Beta](https://img.shields.io/badge/Status-Beta-orange.svg)](https://github.com/kydchen/bluesky-data-collector)

A comprehensive tool for collecting data from the Bluesky social platform using the ATP (Authenticated Transfer Protocol) API.

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/kydchen/bluesky-data-collector.git
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

### ✅ Core Features
- **Keyword-based Search**: Collect posts containing specific keywords with advanced filtering options
- **User Data Collection**: Collect user profiles and posts with full interaction trees
- **User Discovery**: Automatically discover and track users from interaction data
- **Batch Processing**: Process all discovered users efficiently with skip-existing option
- **Feed Discovery & Collection**: Discover and collect content from Bluesky's suggested feeds (limited quantity)
- **Parallel Collection**: Multi-account, multi-threaded data collection with automatic time window division

### 🔧 Technical Features
- **Rate Limiting**: Built-in API rate limiting to respect Bluesky's limits
- **Pagination Support**: Collect large datasets efficiently with cursor-based pagination
- **Batch Processing**: Save data every 100 posts to reduce memory usage and enable resume functionality
- **Resume Capability**: Automatically resume interrupted collections from the last saved point
- **Advanced Search Filters**: Support for author, domain, language, mentions, tags, URLs, time ranges, and sorting
- **Data Deduplication**: Automatic removal of duplicate posts in search results
- **Comprehensive Logging**: Detailed logging for monitoring collection progress and debugging
- **Error Handling**: Robust error handling with graceful degradation
- **Asynchronous Processing**: Efficient async/await pattern for concurrent API operations
- **Robust API Strategy**: Intelligent fallback mechanism that uses ATP client when possible, falls back to direct HTTP with field cleaning when validation fails
- **Multi-Account Support**: Use multiple Bluesky accounts for parallel data collection
- **Automatic Time Division**: Intelligently divide time ranges across multiple accounts

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


4. **Configure environment variables**
```bash
cp config.env.example config.env
# Edit config.env with your Bluesky credentials and collection settings
```

## Configuration

Create a `config.env` file with your Bluesky credentials and collection settings:

```env
# --- Single Account Configuration (Legacy) ---
# Required: Your Bluesky account credentials
BLUESKY_USERNAME=your_username.bsky.social
BLUESKY_PASSWORD=your_password
BLUESKY_APP_PASSWORD=your_app_password  # Optional (if you have 2FA enabled)

# --- Multi-Account Configuration (Recommended for Parallel Collection) ---
# Format: account1_username,account2_username,account3_username
MULTI_ACCOUNT_USERNAMES=account1.bsky.social,account2.bsky.social,account3.bsky.social

# Format: account1_password,account2_password,account3_password
MULTI_ACCOUNT_PASSWORDS=password1,password2,password3

# Format: account1_app_password,account2_app_password,account3_app_password (optional)
MULTI_ACCOUNT_APP_PASSWORDS=app_password1,app_password2,app_password3

# --- Parallel Collection Settings ---
# Number of parallel workers (should match number of accounts)
PARALLEL_WORKERS=4

# Time window division strategy: "equal", "overlap", "custom", "weighted"
TIME_DIVISION_STRATEGY=weighted

# Overlap percentage for overlap strategy (0-50)
TIME_OVERLAP_PERCENT=10

# Custom time windows (ISO format, comma-separated)
# Format: start1-end1,start2-end2,start3-end3
CUSTOM_TIME_WINDOWS=2025-07-01T00:00:00Z-2025-07-10T23:59:59Z,2025-07-11T00:00:00Z-2025-07-20T23:59:59Z,2025-07-21T00:00:00Z-2025-07-31T23:59:59Z

# --- Data Collection Settings ---
# Default collection limits
DEFAULT_KEYWORD_LIMIT=1000
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

### 1. Keyword-based Search

```bash
# Basic keyword search
python main.py --keyword "python" --limit 1000

# Advanced search with filters
python main.py --keyword "AI" --author "username.bsky.social" --lang "en" --since "2024-01-01T00:00:00Z"

# Multiple keywords simultaneously
python main.py --keywords "python,ai,machine learning" --limit 500

# Search with time range and sorting
python main.py --keyword "tech" --since "2024-01-01T00:00:00Z" --until "2024-12-31T23:59:59Z" --sort "latest"
```

### 2. Parallel Collection

```bash
# Show configured accounts for parallel collection
python main.py --show-accounts

# Parallel keyword search (uses multiple accounts)
python main.py --parallel-keyword "Bitcoin" --limit 1000 --since "2025-07-01T00:00:00Z" --until "2025-07-31T23:59:59Z"

# Parallel multiple keywords search
python main.py --parallel-keywords "python,ai,blockchain" --limit 500

# Parallel user posts collection (with cursor-based distribution)
python main.py --parallel-user "username.bsky.social" --limit 5000

# Parallel user posts collection (unlimited - all posts)
python main.py --parallel-user "username.bsky.social" --limit 0

# Parallel batch processing (profiles and posts for all discovered users)
python main.py --parallel-batch-all --skip-existing
```

### 3. User Data Collection

```bash
# Collect only user profile
python main.py --user "username.bsky.social" --user-profile

# Collect only user posts with full interaction trees
python main.py --user "username.bsky.social" --user-posts

# Collect both user profile and posts
python main.py --user "username.bsky.social" --user-all

# Collect ALL posts for a user (unlimited)
python main.py --user "username.bsky.social" --user-posts --limit 0
```

### 4. Batch Processing Workflow

```bash
# Step 1: Discover users through keyword search
python main.py --keyword "Bitcoin" --limit 1000

# Step 2: Batch collect profiles for all discovered users
python main.py --batch-profiles --skip-existing

# Step 3: Batch collect posts for all discovered users
python main.py --batch-posts --skip-existing

# Step 4: Or collect both profiles and posts at once
python main.py --batch-all --skip-existing
```

### 5. Feed Discovery (Limited Feature)

```bash
# Get suggested feeds (limited to ~30-50 feeds)
python main.py --feeds --feeds-limit 0

# Collect content from a specific feed
python main.py --feed "at://did:plc:example/app.bsky.feed.generator/feed-name"

# Collect content from all suggested feeds
python main.py --all-feeds --feeds-limit 10 --posts-per-feed 500
```

**Note**: Feed discovery is limited by Bluesky API - typically only 30-50 suggested feeds are available, unlike the web interface which can access more.

## Parallel Collection

The parallel collection feature allows you to use multiple Bluesky accounts simultaneously to collect data much faster. This is especially useful for large-scale data collection projects.

### Setup Multi-Account Configuration

1. **Register multiple Bluesky accounts** (recommended: 3-5 accounts)
2. **Configure accounts in config.env**:
```env
MULTI_ACCOUNT_USERNAMES=account1.bsky.social,account2.bsky.social,account3.bsky.social
MULTI_ACCOUNT_PASSWORDS=password1,password2,password3
MULTI_ACCOUNT_APP_PASSWORDS=app_password1,app_password2,app_password3
PARALLEL_WORKERS=4
```

### Time Division Strategies

The parallel collector automatically divides time ranges across multiple accounts:

#### 1. Equal Division (Default)
- Divides time range into equal segments
- Each account collects from a non-overlapping time period
- Best for historical data collection where post density is unknown or assumed to be even.

#### 2. Weighted Division (Recommended)
- **Problem**: Post density on Bluesky is higher in recent times. Equal time division leads to unbalanced workloads.
- **Solution**: Assigns shorter, more recent time windows to some workers and longer, older time windows to others.
- **Result**: Each worker gets a more balanced number of posts to process, leading to more efficient overall collection. Ideal for long-term historical data collection.

#### 3. Overlap Division
- Creates overlapping time windows
- Ensures no data is missed at boundaries
- Useful for comprehensive collection where boundary data is critical.

#### 4. Custom Division
- Define specific time windows for each account
- Maximum flexibility for complex scenarios
- Configure in config.env:
  ```env
  CUSTOM_TIME_WINDOWS=2025-07-01T00:00:00Z-2025-07-10T23:59:59Z,2025-07-11T00:00:00Z-2025-07-20T23:59:59Z,2025-07-21T00:00:00Z-2025-07-31T23:59:59Z
  ```

### Parallel Collection Examples

```bash
# Check account configuration
python main.py --show-accounts

# Parallel keyword search (uses multiple accounts)
python main.py --parallel-keyword "Bitcoin" --limit 1000 --since "2025-07-01T00:00:00Z" --until "2025-07-31T23:59:59Z"

# Parallel multiple keywords search
python main.py --parallel-keywords "python,ai,blockchain" --limit 500

# Parallel user posts collection (with time-based distribution)
python main.py --parallel-user "username.bsky.social" --limit 5000

# Parallel user posts collection (unlimited - all posts)
python main.py --parallel-user "username.bsky.social" --limit 0

# Parallel batch processing (profiles and posts for all discovered users)
python main.py --parallel-batch-all --skip-existing
```

### How Parallel Collection Works

#### Keyword Search Parallel Collection
- **Time Division**: Each account collects from a different time window
- **No Duplicates**: Time windows are non-overlapping (or controlled overlap)
- **Efficient**: Each account works on a different time period
- **Default Time Range**: If not specified, automatically uses 2024-02-01 (Bluesky launch) to current date
- **Limit Handling**: 
  - For unlimited collection (`--limit 0`): Each worker collects unlimited data from its time window
  - For limited collection: Total limit is divided among workers (e.g., limit 1000 with 4 workers = 250 per worker)
- **Batch Processing**: Each worker saves data every 100 posts and merges upon completion

#### Parallel Collection Workflow
1. **Batch Collection**: Each worker saves data every 100 posts to individual batch files
   - `search_keyword_worker_0_batch_1.json` (posts 1-100)
   - `search_keyword_worker_0_batch_2.json` (posts 101-200)
   - `search_keyword_worker_0_batch_3.json` (posts 201-300)
   - etc.

2. **Worker Merge**: When each worker completes, it merges all its batch files into a single worker file
   - `search_keyword_worker_0.json` (all posts from worker 0)

3. **Final Merge**: After all workers complete, all worker files are merged into the final result
   - `search_keyword.json` (all posts from all workers, deduplicated)

This ensures data safety, enables resume functionality, and provides clear progress tracking.

#### User Posts Parallel Collection
- **Time Division**: Each account collects from a different time window (default: 2024-02-01 to today)
- **Smart Distribution**: System automatically divides time range across accounts
- **No Duplicates**: Each account collects from different time periods
- **Perfect for Unlimited Collection**: Works especially well with `--limit 0`
- **Limit Handling**: Same as keyword search - unlimited or divided limits per worker
- **Batch Processing**: Each worker saves data every 100 posts and merges upon completion

**Example**: If collecting posts from 2024-02-01 to 2025-07-05 with 3 accounts:
- Account 1: 2024-02-01 to 2024-07-01
- Account 2: 2024-07-01 to 2025-01-01  
- Account 3: 2025-01-01 to 2025-07-05

This ensures no duplicate collection and maximum efficiency!

#### Batch Processing Parallel Collection
- **User Distribution**: Each account processes a different subset of discovered users
- **Profiles**: Each account collects profiles for assigned users
- **Posts**: Each account collects posts for assigned users (with time-based distribution per user)
- **Concurrent Safety**: File locking and atomic operations ensure safe concurrent updates to `discovered_users.json`
- **Batch Processing**: Each worker saves data every 100 posts and merges upon completion

### Performance Benefits

- **Speed**: 3-5x faster collection with multiple accounts
- **Efficiency**: Automatic time division prevents duplicate work
- **Reliability**: If one account fails, others continue working
- **Scalability**: Easy to add more accounts for even faster collection

### Best Practices

1. **Account Management**: Use dedicated accounts for data collection
2. **Rate Limiting**: Each account respects Bluesky's rate limits independently
3. **Time Windows**: Use appropriate time division strategy for your use case
4. **Monitoring**: Check account status regularly with `--show-accounts`

## Data Structure

### Keyword Search Data Structure

When collecting posts by keyword, the tool generates a structured dataset:

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
      "author_displayName": "User Display Name",
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
      "likes": [...],
      "reposts": [...],
      "replies": [...],
      "quotes": [...],
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

### User Posts Data Structure

When collecting user posts with `--user-posts`, the tool generates comprehensive data including full interaction trees:

```json
{
  "uri": "at://did:plc:example/app.bsky.feed.post/123",
  "cid": "bafyrei...",
  "author_did": "did:plc:example",
  "author_handle": "username.bsky.social",
  "author_displayName": "User Display Name",
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
  "reposts": [...],
  "replies": [...],
  "quotes": [...]
}
```

### Field Descriptions

#### Post Metadata
- **uri**: Unique identifier for the post
- **cid**: Content identifier (hash)
- **author_did**: Author's decentralized identifier
- **author_handle**: Author's username
- **author_displayName**: Author's display name
- **text**: Post content
- **url**: Web URL for the post
- **created_at**: When the post was created
- **indexed_at**: When the post was indexed by Bluesky

#### Engagement Counts
- **reply_count**: Number of replies to this post
- **repost_count**: Number of reposts of this post
- **like_count**: Number of likes on this post
- **quote_count**: Number of quotes of this post

#### Post Type Indicators
- **is_reply**: True if this post is a reply by the target user to another post
- **is_repost**: True if this post is a repost by the target user of another post
- **is_quote**: True if this post is a quote by the target user of another post

#### Relationship Fields
- **parent_uri**: URI of the immediate parent post (for replies)
- **root_uri**: URI of the root/original post in the thread (for replies)
- **original_post_uri**: URI of the original post being reposted/quoted
- **original_post_author**: Handle of the original post author
- **quoted_post_info**: Complete details about the quoted post (for quotes only)

#### Interaction Data
- **likes**: Array of users who liked the post (with user details)
- **reposts**: Array of users who reposted the post (with user details)
- **replies**: Array of reply posts (with full interaction trees for user posts)
- **quotes**: Array of quote posts (with full interaction trees for user posts)

### File Organization

```
data/
├── keywords/              # Keyword-based search results
│   ├── search_bitcoin.json
│   ├── search_ai.json
│   └── search_python.json
├── users/                  # User-specific data
│   ├── profiles/           # User profile data
│   │   ├── username_bsky_social_profile.json
│   │   └── another_user_profile.json
│   ├── posts/              # User posts data
│   │   ├── username_bsky_social_posts.json
│   │   └── another_user_posts.json
│   ├── discovered_users.json           # Global discovered users from all operations
│   ├── discovered_users_bitcoin.json   # Users discovered from Bitcoin keyword search
│   ├── discovered_users_ai.json        # Users discovered from AI keyword search
│   └── discovered_users_python.json    # Users discovered from Python keyword search
└── feeds/                  # Feed-based data collection
    ├── suggested_feeds.json     # Discovered feed generators
    └── feed_did_plc_example_app_bsky_feed_generator_feed_name.json
```

## API Rate Limits

The tool respects Bluesky's API rate limits:
- **Authentication**: 3000 requests per 5 minutes
- **Content Retrieval**: 3000 requests per 5 minutes
- **Search Operations**: 1000 requests per 5 minutes

The tool automatically implements rate limiting (100ms delay between requests) and pagination to collect large datasets efficiently.

## Data Collection Limits

- **Default Limits**: 
  - User posts: 10,000 posts (can be overridden with --limit 0 for unlimited)
  - Keyword search: 1,000 posts

- **API Batch Size**: All methods use batch size of 100 per API request for optimal performance
- **Save Batch Size**: Data is saved every 100 posts to reduce memory usage and enable resume functionality

- **User Posts Collection**: 
  - Uses `posts_with_replies` filter by default (includes user's posts and replies)
  - Supports unlimited collection with `--limit 0` parameter
  - **Full Interaction Trees**: Includes complete likes, reposts, replies, and quotes data for each post

### Resume Functionality

The tool automatically supports resuming interrupted collections:

- **Temporary Files**: During collection, data is saved to `*_temp.json` files
- **Automatic Resume**: If interrupted, the tool will automatically resume from the last saved point
- **Cursor Tracking**: API cursors are saved to enable precise resume functionality
- **No Data Loss**: Interrupted collections can be resumed without losing progress

### Concurrency Safety

The tool handles concurrent operations safely:

- **File Locking**: Uses file locks to prevent concurrent access conflicts  
- **Atomic Operations**: File updates use atomic write operations to prevent corruption
- **Safe Reading**: Multiple processes can safely read the same files simultaneously
- **Batch Processing**: Batch operations are designed to work safely with concurrent updates
- **Multi-File Consistency**: Keyword-specific and global discovered users files are updated atomically to maintain consistency

### User Discovery Feature

The tool automatically discovers and tracks users from interaction data:

- **Automatic Discovery**: When collecting user posts, the tool automatically extracts all user handles from:
  - Users who liked the posts
  - Users who reposted the posts  
  - Users who replied to the posts
  - Users who quoted the posts
  - Authors of quoted posts

- **Dual-Level Tracking**: 
  - **Global Discovery**: All discovered users are saved to `data/users/discovered_users.json`
  - **Keyword-Specific Discovery**: Users discovered during keyword searches are also saved to `data/users/discovered_users_{keyword}.json`
  - **Feed Discovery**: Users discovered during feed collection are only added to the global file

- **Persistent Tracking**: Discovered users files are updated incrementally with each collection operation
- **Duplicate Prevention**: The system prevents duplicate entries and only adds new users
- **Thread-Safe Operations**: Uses file locking and atomic operations to ensure data consistency in multi-threaded environments
- **Community Analysis**: Keyword-specific files enable focused analysis of topic-specific communities

### Batch Processing Workflow

The tool supports a systematic workflow for community analysis:

1. **Initial Discovery**: Use keyword search to discover topic participants
   ```bash
   python main.py --keyword "Bitcoin" --limit 1000
   ```
   This creates both `discovered_users.json` (global) and `discovered_users_Bitcoin.json` (keyword-specific)

2. **Topic-Specific Analysis**: Analyze keyword-specific communities
   ```bash
   # Users discovered from Bitcoin keyword search are in:
   # data/users/discovered_users_Bitcoin.json
   
   # Compare different topic communities:
   python main.py --keyword "Ethereum" --limit 1000
   # Creates: data/users/discovered_users_Ethereum.json
   ```

3. **Batch User Collection**: Automatically collect profiles and posts for all discovered users
   ```bash
   python main.py --batch-all --skip-existing
   ```
   Uses the global `discovered_users.json` which contains users from all keyword searches

4. **Community Analysis**: Analyze the collected data to understand the community structure

**Features**:
- **Dual-Level Discovery**: Global file for batch processing, keyword-specific files for focused analysis
- **Skip Existing**: Use `--skip-existing` to avoid re-collecting data for users already processed
- **Selective Collection**: Choose to collect only profiles (`--batch-profiles`) or only posts (`--batch-posts`)
- **Automatic Rate Limiting**: Built-in delays between API calls to respect rate limits
- **Progress Tracking**: Detailed logging of collection progress and results

## Technical Architecture

### Robust API Strategy

The tool implements an intelligent fallback mechanism to handle API validation issues:

1. **Primary Method**: Uses the official ATP client for type safety and convenience
2. **Fallback Method**: When validation fails (e.g., due to problematic fields like `aspectRatio`), automatically falls back to direct HTTP requests with field cleaning
3. **Field Management**: 
   - **Preserved Fields**: Keeps essential data like `labels`, `facets`, `reply`, `tags`, `langs`, `entities`
   - **Cleaned Fields**: Removes problematic fields like `aspectRatio`, `thumb`, `image`, `alt`, `embed`
4. **Seamless Operation**: Users don't need to worry about underlying API issues - the tool handles them automatically

This strategy ensures maximum data collection success while maintaining data integrity and avoiding validation errors.

## Data Privacy & Ethics

- **Respect Privacy**: Only collect publicly available data
- **Rate Limiting**: Respect API limits and server resources
- **Data Usage**: Use collected data responsibly and ethically
- **Compliance**: Follow Bluesky's Terms of Service

## Testing

The project includes a test script to verify the robust API strategy:

```bash
# Test the robust fallback mechanism
python test_robust_strategy.py
```

This script tests various API methods to ensure they work correctly with the fallback strategy.

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
