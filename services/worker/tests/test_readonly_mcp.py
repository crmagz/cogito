from cogito_worker.readonly_mcp import catalog_read


def test_catalog_read_returns_only_fixed_readonly_data() -> None:
    assert catalog_read() == {
        "catalog_version": "1.0.0",
        "capabilities": ["governed_mcp_validation"],
        "read_only": True,
    }
