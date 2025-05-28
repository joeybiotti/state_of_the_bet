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
    price DECIMAL(10, 2),
    prices_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

CREATE TRIGGER prices_update_trigger
BEFORE UPDATE ON prices
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

-- Indexes 
CREATE INDEX idx_contracts_market_id ON contracts(market_id);

CREATE INDEX idx_prices_contract_id ON prices(contract_id);

CREATE INDEX idx_contracts_updated_date ON contracts(updated_date);

CREATE INDEX idx_prices_timestamp ON prices(prices_timestamp);