def validate_json(data, schema):
    '''Validates JSON against required schema'''
    if not isinstance(data, dict):
        raise ValueError('JSON must be an object')
    
    for field, expected_type in schema.items():
        if field not in data: 
            raise ValueError(f'Required field missing: {field}')
        if not isinstance(data[field], expected_type):
            raise TypeError(f'Incorrect data type for {field}. Expected {expected_type}')
    return True, 'Valid JSON'
