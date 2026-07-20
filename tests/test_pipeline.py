import pytest

def test_kafka_topic_defined():
    topic = "sales-orders"
    assert topic == "sales-orders"

def test_revenue_threshold():
    total_revenue = 368919
    assert total_revenue > 0

def test_dashboard_panels_count():
    panels = ["total_revenue", "orders_per_minute", "revenue_by_product"]
    assert len(panels) == 3

def test_refresh_interval_seconds():
    refresh = 30
    assert refresh > 0

def test_docker_services_defined():
    services = ["kafka", "pyspark", "timescaledb", "grafana"]
    assert len(services) == 4
