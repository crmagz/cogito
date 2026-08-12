"""Connector-specific errors that never expose upstream response bodies."""


class GitHubConnectorError(RuntimeError):
    """Raised when a GitHub operation cannot safely return a result."""
