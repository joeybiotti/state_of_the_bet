import pytest
from utils.utils import validate_json


@pytest.mark.parametrize('json_data, expected', [
    ({"id": 1, "data": "test message"}, True),
    ({"id": "wrong_type", "data": "test message"}, False),
    ({"data": "missing id"}, False),
    ({"id": 1, "data": None}, True),  # Allows nulls but warns
])
def test_validate_json(json_data, expected):
    """Test JSON validation against schema with type checks."""
    schema = {'id': int, 'data': (str, type(None))}
    if expected:
        validate_json(json_data, schema)
    else:
        with pytest.raises((ValueError, TypeError)):
            validate_json(json_data, schema)


@pytest.mark.parametrize('json_data, expected', [
    ({"id": 1, "data": "test message"}, True),  # Valid
    ({"id": 2}, False),  # Missing "data"
    ({"data": "test message"}, False)  # Missing "id"
])
def test_required_fields(json_data, expected):
    """Test that required fields are enforced in JSON validation."""
    schema = {'id': int, 'data': str}
    if expected:
        is_valid, _ = validate_json(json_data, schema)
        assert is_valid is True
    else:
        with pytest.raises(ValueError, match='Required field missing: .*'):
            validate_json(json_data, schema)


@pytest.mark.parametrize('json_data, expected', [
    ({'id': 1, 'data': 'test message'}, True),  # Valid
    ({'id': 'one', 'data': 'test message'}, False),  # Wrong data type for 'id'
    ({'id': 1, 'data': 123}, False)  # Wrong data type for 'data'
])
def test_data_type_enforcements(json_data, expected):
    """Test that data types are enforced for each field."""
    schema = {'id': int, 'data': str}
    if expected:
        is_valid, _ = validate_json(json_data, schema)
        assert is_valid is True
    else:
        with pytest.raises(TypeError, match='Incorrect data type for .*'):
            validate_json(json_data, schema)


@pytest.mark.parametrize("json_data, expected", [
    ({"id": 1, "data": "test message"}, True),  # Valid
    ({"id": None, "data": "test message"}, False),  # "id" shouldn't be null
    ({"id": 1, "data": None}, True),  # Allow nullable "data"
])
def test_null_value_handling(json_data, expected):
    """Test handling of null values in JSON fields."""
    schema = {'id': int, 'data': (str, type(None))}  # Allow null for 'data'
    if expected:
        assert validate_json(json_data, schema)[0] is True
    else:
        with pytest.raises((TypeError, ValueError), match='Incorrect data type for .*|Required field missing: .*'):
            validate_json(json_data, schema)
