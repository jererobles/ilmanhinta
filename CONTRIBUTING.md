# Contributing to Ilmanhinta

Thanks for your interest in contributing! This document provides guidelines for contributing to the project.

## Development Setup

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and install**:
   ```bash
   git clone https://github.com/yourusername/ilmanhinta.git
   cd ilmanhinta
   uv pip install -e ".[dev]"
   ```

3. **Set up pre-commit hooks**:
   ```bash
   pre-commit install
   ```

## Code Standards

### Style Guide

- Use **Ruff** for linting and formatting (configured in pyproject.toml)
- Maximum line length: 100 characters
- Use type hints everywhere
- Follow PEP 8 naming conventions

### Type Checking

- All code must pass **mypy** strict mode
- Use Pydantic v2 for data validation
- Avoid `Any` types where possible

### Documentation

- Add docstrings to all public functions/classes
- Keep docstrings concise (not javadoc novels)
- Include type information in docstrings only when it adds clarity

Example:
```python
def fetch_data(start_time: datetime, end_time: datetime) -> list[DataPoint]:
    """Fetch data from API for the given time range."""
    ...
```

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src/ilmanhinta --cov-report=html

# Run specific test
pytest tests/test_models.py::test_fingrid_data_point -v
```

### Writing Tests

- Place tests in `tests/` directory
- Use descriptive test names: `test_<function>_<scenario>`
- Aim for >80% code coverage
- Use fixtures for shared test data

## Pull Request Process

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write code
   - Add tests
   - Update documentation

3. **Run quality checks**:
   ```bash
   # Lint
   ruff check .

   # Format
   ruff format .

   # Type check
   mypy src/ilmanhinta

   # Test
   pytest tests/ -v
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add amazing feature"
   ```

   Use conventional commit messages:
   - `feat:` - New feature
   - `fix:` - Bug fix
   - `docs:` - Documentation changes
   - `refactor:` - Code refactoring
   - `test:` - Adding tests
   - `chore:` - Maintenance tasks

5. **Push and create PR**:
   ```bash
   git push origin feature/your-feature-name
   ```

   Then open a pull request on GitHub.

## Code Review

All submissions require review. We use GitHub pull requests for this purpose.

- Address all review comments
- Keep PRs focused and reasonably sized
- Update tests and docs as needed

## Questions?

Feel free to open an issue for questions or discussions!
