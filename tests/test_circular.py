import os
import pytest
from pyln.testing.fixtures import *

plugin_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "circular"))

def test_circular_starts(node_factory):
    l1 = node_factory.get_node(options={"plugin": plugin_path})
    plugins = l1.rpc.plugin_list()["plugins"]
    assert any("circular" in p["name"] for p in plugins)
