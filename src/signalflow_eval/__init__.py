"""Offline evaluation against the generator's scenario ledger.

This package is deliberately outside ``signalflow``: detection code never imports it (enforced by
``tests/unit/test_isolation.py``), and it only reads what detection already wrote.
"""
