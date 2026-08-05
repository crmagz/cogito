import pytest

from cogito_worker.readonly_mcp import catalog_read, mcp_port


def test_catalog_read_returns_only_fixed_readonly_data() -> None:
    assert catalog_read() == {
        "catalog_version": "1.0.0",
        "capabilities": ["governed_mcp_validation"],
        "read_only": True,
    }


def test_mcp_port_accepts_the_chart_configured_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_MCP_PORT", "9000")

    assert mcp_port() == 9000


@pytest.mark.parametrize("value", ("invalid", "0", "65536"))
def test_mcp_port_rejects_unsafe_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("COGITO_MCP_PORT", value)

    with pytest.raises(ValueError, match="COGITO_MCP_PORT"):
        mcp_port()
