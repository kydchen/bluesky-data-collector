# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.4] - 2025-07-09

### Added
- **Keyword-Specific User Discovery**: Enhanced user discovery system to create keyword-specific discovered users files
  - `discovered_users_{keyword}.json` files are now created for each keyword search
  - Global `discovered_users.json` continues to be maintained for all discovered users
  - Supports both single-threaded and parallel collection modes
  - Maintains full thread-safety with existing file locking mechanisms

### Changed
- **Enhanced User Discovery Logic**: Extended `_update_discovered_users` method to support optional keyword parameter
  - Keyword searches now update both global and keyword-specific discovered users files
  - Other operations (user posts, feed collection) continue to update only the global file
  - Parallel collection properly updates keyword-specific files during worker merge phase

### Technical Details
- Keyword-specific files follow the naming pattern: `data/users/discovered_users_{keyword}.json`
- All existing multi-threading safety mechanisms are preserved
- Atomic file operations ensure data consistency across concurrent operations
- Compatible with all existing batch processing workflows

## [1.2.3] - 2025-07-05

### Fixed
- **Critical Recursion Bug**: Fixed a critical issue where the collector could get stuck in an infinite recursion loop when processing posts with circular references (e.g., A replies to B, B quotes A). This primarily affected unlimited collections (`--limit 0`), causing them to hang indefinitely without saving temporary files. The fix implements recursion path tracking to detect and break cycles, ensuring all collection processes complete successfully.
- **Unified Batch Processing**: Applied the efficient, incremental batch saving and resume logic (previously only in keyword search) to all User Post and Batch Processing collection methods. This fixes data redundancy, improves I/O performance, and ensures reliable resume functionality across all parallel collection modes.

## [1.2.2] - 2025-07-05

### Added
- **Parallel Batch Processing**: Complete parallel processing for discovered users
  - `--parallel-batch-all`: Process all discovered users with multiple accounts simultaneously
  - Automatic user distribution across accounts for maximum efficiency
  - Concurrent profile and posts collection with skip-existing support
  - Enhanced result reporting with detailed statistics
- **Enhanced User Posts Parallel Collection**: Added batch processing support for user posts collection
  - User posts parallel collection now uses the same batch-based workflow as keyword search
  - Each worker saves data every 100 posts to individual batch files
  - Workers merge their batch files into worker-specific files upon completion
  - Provides consistent data safety and resume functionality across all parallel collection types

### Changed
- **Improved Parallel User Posts Collection**: Replaced cursor-based distribution with time-based distribution
  - Now uses time windows (default: 2024-02-01 to today) instead of cursor distribution
  - Eliminates limitations of cursor-based approach
  - Provides true parallel collection with no duplicate work
  - Better performance and more predictable results
- **Fixed Parallel Collection Limit Handling**: Corrected limit distribution for unlimited collection
  - Fixed bug where `--limit 0` (unlimited) was incorrectly divided among workers
  - Now each worker gets unlimited limit when total limit is 0
  - Limited collection still properly divides total limit among workers
- **Completely Redesigned Parallel Collection Workflow**: Implemented proper batch-based collection system
  - Each worker now saves data every 100 posts to individual batch files
  - Workers merge their batch files into worker-specific files upon completion
  - Final merge combines all worker files into the complete result
  - Ensures data safety, enables resume functionality, and provides clear progress tracking
  - Eliminates the previous issue where workers would overwrite each other's data
- **Updated Documentation**: Enhanced README with accurate parallel collection information
  - Added complete parallel collection examples
  - Updated workflow descriptions to reflect current implementation
  - Clarified limit handling and batch processing mechanisms
  - Fixed outdated references to cursor-based distribution
- **Removed Unused Dependencies**: Cleaned up requirements.txt by removing unused `rich` library
- **Cleaned Up Test Files**: Removed unnecessary test files for cleaner project structure

### Added
- **Parallel Collection System**: Multi-account, multi-threaded data collection for significantly improved performance
  - Support for multiple Bluesky accounts working simultaneously
  - Automatic time window division across accounts
  - Three time division strategies: equal, overlap, and custom
  - Parallel keyword search with `--parallel-keyword` and `--parallel-keywords`
  - Parallel user posts collection with `--parallel-user` (now using time-based distribution)
  - Account configuration management with `--show-accounts`
- **Multi-Account Configuration**: Flexible account management system
  - Support for up to 10+ accounts in parallel
  - Automatic fallback to single account if multi-account not configured
  - Thread-safe account authentication and management
  - Independent rate limiting per account

### Changed
- **Enhanced Performance**: Parallel collection provides 3-5x speed improvement
  - Automatic workload distribution across accounts
  - Intelligent time range division to prevent duplicate work
  - Concurrent API requests while respecting rate limits
- **Improved Configuration**: Extended config.env with parallel collection settings
  - Multi-account credential management
  - Time division strategy configuration
  - Parallel worker count settings

### Technical Details
- Parallel collection uses asyncio for efficient concurrent operations
- Time windows are automatically calculated based on account count and strategy
- Each account maintains independent API client and session
- Robust error handling ensures one account failure doesn't stop others

## [1.1.0] - 2025-07-05

### Added
- **Robust API Strategy**: Intelligent fallback mechanism for handling API validation issues
  - Automatic fallback from ATP client to direct HTTP requests when validation fails
  - Smart field cleaning to preserve essential data while removing problematic fields
  - Seamless operation across all API methods (search, user feeds, feed content, interactions)

### Changed
- **API Client Architecture**: Completely refactored API client to use robust fallback strategy
  - All API methods now use intelligent fallback mechanism
  - Improved error handling and validation error recovery
  - Better field management to preserve important data while avoiding validation issues

### Technical Details
- Robust API strategy handles validation errors gracefully without data loss
- Field cleaning preserves essential metadata (labels, facets, reply, tags, langs, entities) while removing problematic fields

## [1.0.1] - 2025-07-05

### Added
- **Feed Discovery & Collection**: Discover and collect content from Bluesky's suggested feeds
  - `--feeds`: Collect Bluesky's suggested feed generators (limited to ~30-50 feeds)
  - `--feed`: Collect content from a specific feed URI
  - `--all-feeds`: Collect content from all suggested feeds (limited by API capabilities)
- **Feed-based User Discovery**: Automatically discover users from feed content and interactions
- **Feed Content Processing**: Process feed posts with source tracking and participant analysis
- **Batch Processing Commands**: New commands for efficient processing of discovered users
  - `--batch-profiles`: Collect profiles for all discovered users
  - `--batch-posts`: Collect posts for all discovered users  
  - `--batch-all`: Collect both profiles and posts for all discovered users
  - `--skip-existing`: Skip users whose data already exists
- **Robust API Strategy**: Intelligent fallback mechanism for handling API validation issues
  - Automatic fallback from ATP client to direct HTTP requests when validation fails
  - Smart field cleaning to preserve essential data while removing problematic fields
  - Seamless operation across all API methods (search, user feeds, feed content, interactions)

### Changed
- **Enhanced User Discovery**: Improved user discovery logic to include participants from keyword search and feed collection
- **File Organization**: Restructured data directories for better organization
  - Moved `suggested_feeds.json` to `data/feeds/` directory
  - Separated user profiles and posts into dedicated subdirectories
- **Recursion Strategy**: Unified recursion strategy across keyword search and feed collection using "original_only" approach
- **Display Name Support**: Fixed and improved display name extraction for topic participants
- **API Client Architecture**: Completely refactored API client to use robust fallback strategy
  - All API methods now use intelligent fallback mechanism
  - Improved error handling and validation error recovery
  - Better field management to preserve important data while avoiding validation issues

### Technical Details
- Feed collection is limited by Bluesky API capabilities (typically 30-50 suggested feeds)
- Batch processing includes automatic rate limiting and progress tracking
- User discovery now works across all collection methods (keyword search, feed collection, user posts)
- Improved error handling and logging for batch operations
- Robust API strategy handles validation errors gracefully without data loss
- Field cleaning preserves essential metadata (labels, facets, reply, tags, langs, entities) while removing problematic fields

## [1.0.0] - 2025-07-05

### Added
- **Keyword-based Search**: Collect posts containing specific keywords with advanced filtering
- **User Data Collection**: Collect user profiles and posts with full interaction trees
- **User Discovery**: Automatically discover and track users from interaction data
- **Advanced Search Filters**: Support for author, domain, language, mentions, tags, URLs, time ranges, and sorting
- **Data Deduplication**: Automatic removal of duplicate posts in search results
- **Rate Limiting**: Built-in API rate limiting to respect Bluesky's limits
- **Pagination Support**: Collect large datasets efficiently with cursor-based pagination
- **Comprehensive Logging**: Detailed logging for monitoring collection progress and debugging
- **Error Handling**: Robust error handling with graceful degradation
- **Performance Optimization**: Intelligent caching for quoted posts to reduce API calls
- **Asynchronous Processing**: Efficient async/await pattern for concurrent API operations
- **Concurrent Collection**: Support for collecting multiple data types simultaneously
- **Robust Data Saving**: Automatic directory creation and error handling for data persistence

### Features
- **"Original Only" Recursion Strategy**: For keyword search, original posts get full recursive data while replies/quotes/reposts get basic data only
- **Full Interaction Trees**: Complete likes, reposts, replies, and quotes data for user posts
- **User Discovery Tracking**: Automatic discovery and tracking of users from interaction data
- **JSON Export**: Structured data export for further processing
- **Command Line Interface**: Easy-to-use CLI with comprehensive options

### Technical Details
- Built with Python 3.8+
- Uses ATP (Authenticated Transfer Protocol) API
- Supports all major operating systems
- MIT License for open source use

## [Unreleased]

### Planned Features
- Enhanced data analysis tools
- Web interface for data visualization 
