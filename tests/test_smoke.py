"""Smoke test: the flow_node package imports cleanly."""


def test_package_imports():
    import flow_node

    assert flow_node is not None