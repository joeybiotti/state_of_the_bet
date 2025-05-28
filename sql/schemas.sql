CREATE TABLE markets (
    market_id serial PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE contracts (
    contract_id serial PRIMARY KEY,
    market_id INT REFERENCES markets (market_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    current_price DECIMAL(5, 2),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE prices (
    prices_id serial PRIMARY KEY,
    contract_id INT REFERENCES contracts (contract_id) ON DELETE CASCADE,
    price DECIMAL(5, 2),
    TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Automatic timestamp updates
CREATE FUNCTION update_timestamp() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update timestamp on row modification 
CREATE TRIGGER contracts_update_trigger
BEFORE UPDATE ON contracts
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();