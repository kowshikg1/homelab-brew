from fastapi.testclient import TestClient

from src.api.app import app

client = TestClient(app)


def test_health_endpoint_returns_ok():
    response = client.get('/api/health')
    assert response.status_code == 200
    assert response.json() == {'status': 'ok'}


def test_services_endpoint_returns_payload_shape(monkeypatch):
    monkeypatch.setenv('JELLYFIN_URL', 'http://jellyfin.local:8096')
    monkeypatch.setenv('IMMICH_URL', 'http://immich.local:2283')
    monkeypatch.setenv('MQTT_TELEGRAM_URL', 'http://mqtt.local:9999')

    response = client.get('/api/services')

    assert response.status_code == 200
    payload = response.json()
    assert 'version' in payload
    assert 'categories' in payload
    assert 'services' in payload
    assert isinstance(payload['services'], list)
    assert 'urls' in payload['services'][0]
    assert 'status' in payload['services'][0]
