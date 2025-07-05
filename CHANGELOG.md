# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-01-05

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
- Popular content collection (placeholder)
- Suggested users collection (placeholder)
- Enhanced data analysis tools
- Web interface for data visualization 