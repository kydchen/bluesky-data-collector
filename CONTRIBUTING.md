# Contributing to Bluesky Data Collection Tool

Thank you for your interest in contributing to the Bluesky Data Collection Tool! This document provides guidelines and information for contributors.

## How to Contribute

### Reporting Issues

Before creating bug reports, please check the existing issues to see if the problem has already been reported. When creating a bug report, please include:

- A clear and descriptive title
- Steps to reproduce the problem
- Expected behavior
- Actual behavior
- Environment details (OS, Python version, etc.)
- Any relevant error messages or logs

### Suggesting Enhancements

If you have a suggestion for a new feature or improvement:

- Check if the feature has already been requested
- Provide a clear description of the proposed feature
- Explain why this feature would be useful
- Include any relevant use cases

### Code Contributions

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/your-feature-name`
3. **Make your changes**: Follow the coding standards below
4. **Test your changes**: Ensure all tests pass and the tool works as expected
5. **Commit your changes**: Use clear, descriptive commit messages
6. **Push to your fork**: `git push origin feature/your-feature-name`
7. **Create a Pull Request**: Provide a clear description of your changes

## Coding Standards

### Python Code Style

- Follow PEP 8 style guidelines
- Use meaningful variable and function names
- Add comments for complex logic
- Keep functions focused and reasonably sized
- Use type hints where appropriate

### Documentation

- Update README.md if you add new features
- Add docstrings to new functions and classes
- Include examples for new functionality

### Testing

- Test your changes thoroughly
- Ensure the tool works with different input scenarios
- Test error handling and edge cases

## Development Setup

1. Clone your fork: `git clone https://github.com/kydchen/bluesky-data-collector.git`
2. Create a virtual environment: `python -m venv venv`
3. Activate the environment: `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Create your config file: `cp config.env.example config.env`
6. Edit `config.env` with your Bluesky credentials

## Code of Conduct

- Be respectful and inclusive
- Focus on the code and technical discussions
- Help others learn and contribute
- Follow the project's coding standards

## License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

## Questions?

If you have questions about contributing, please open an issue or contact the maintainers.

Thank you for contributing to the Bluesky Data Collection Tool! 
