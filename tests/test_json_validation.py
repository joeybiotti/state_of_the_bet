import pytest
from utils.utils import validate_json


@pytest.mark.parametrize('json_data, expected', [
    ({"id": 1, "data": "test message"}, True),
    ({"id": "wrong_type", "data": "test message"}, False),
    ({"data": "missing id"}, False),
    ({"id": 1, "data": None}, True),  # Allows nulls but warns
])
def test_validate_json(json_data, expected):
    schema = {'id': int, 'data': (str, type(None))}
    if expected:
        validate_json(json_data, schema)
    else: 
        with pytest.raises((ValueError, TypeError)):
            validate_json(json_data,schema)