-- ============================================
-- ETHval Historical Data Tables
-- Supabase SQL Editor에서 실행하세요
-- ============================================

-- 1. ETH 가격 데이터
CREATE TABLE IF NOT EXISTS historical_eth_price (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT NOT NULL,
    open DECIMAL(18, 2),
    high DECIMAL(18, 2),
    low DECIMAL(18, 2),
    close DECIMAL(18, 2),
    volume DECIMAL(24, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_eth_price_date ON historical_eth_price(date);
CREATE INDEX idx_eth_price_timestamp ON historical_eth_price(timestamp);

-- 2. Ethereum TVL
CREATE TABLE IF NOT EXISTS historical_ethereum_tvl (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT NOT NULL,
    tvl DECIMAL(24, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_eth_tvl_date ON historical_ethereum_tvl(date);

-- 3. L2 TVL (체인별)
CREATE TABLE IF NOT EXISTS historical_l2_tvl (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    timestamp BIGINT NOT NULL,
    chain VARCHAR(50) NOT NULL,
    tvl DECIMAL(24, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, chain)
);

CREATE INDEX idx_l2_tvl_date ON historical_l2_tvl(date);
CREATE INDEX idx_l2_tvl_chain ON historical_l2_tvl(chain);

-- 4. Protocol Fees
CREATE TABLE IF NOT EXISTS historical_protocol_fees (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT NOT NULL,
    fees DECIMAL(24, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_protocol_fees_date ON historical_protocol_fees(date);

-- 5. Staking Data
CREATE TABLE IF NOT EXISTS historical_staking (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    staked_eth DECIMAL(24, 8),
    validator_count INTEGER,
    apr DECIMAL(8, 4),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_staking_date ON historical_staking(date);

-- 6. Gas & Burn Data
CREATE TABLE IF NOT EXISTS historical_gas_burn (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    avg_gas_price_gwei DECIMAL(18, 9),
    total_gas_used DECIMAL(24, 0),
    transaction_count INTEGER,
    eth_burned DECIMAL(24, 8),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_gas_burn_date ON historical_gas_burn(date);

-- 7. Active Addresses
CREATE TABLE IF NOT EXISTS historical_active_addresses (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    active_addresses INTEGER,
    new_addresses INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_active_addresses_date ON historical_active_addresses(date);

-- 8. ETH Supply (현재 스냅샷)
CREATE TABLE IF NOT EXISTS historical_eth_supply (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    eth_supply DECIMAL(24, 8),
    eth2_staking DECIMAL(24, 8),
    burnt_fees DECIMAL(24, 8),
    withdrawn_total DECIMAL(24, 8),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_eth_supply_date ON historical_eth_supply(date);

-- 9. 데이터 수집 상태 추적
CREATE TABLE IF NOT EXISTS data_collection_status (
    id BIGSERIAL PRIMARY KEY,
    dataset_name VARCHAR(50) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, success, partial, failed
    record_count INTEGER,
    date_from DATE,
    date_to DATE,
    last_error TEXT,
    last_warning TEXT,
    last_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 초기 데이터셋 상태 삽입
INSERT INTO data_collection_status (dataset_name, status) VALUES
    ('eth_price', 'pending'),
    ('ethereum_tvl', 'pending'),
    ('l2_tvl', 'pending'),
    ('protocol_fees', 'pending'),
    ('staking_data', 'pending'),
    ('gas_burn', 'pending'),
    ('active_addresses', 'pending'),
    ('eth_supply', 'pending')
ON CONFLICT (dataset_name) DO NOTHING;

-- 10. 수집 로그
CREATE TABLE IF NOT EXISTS data_collection_logs (
    id BIGSERIAL PRIMARY KEY,
    dataset_name VARCHAR(50) NOT NULL,
    log_type VARCHAR(20) NOT NULL, -- info, warning, error
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_collection_logs_dataset ON data_collection_logs(dataset_name);
CREATE INDEX idx_collection_logs_type ON data_collection_logs(log_type);
CREATE INDEX idx_collection_logs_created ON data_collection_logs(created_at DESC);

-- ============================================
-- RLS (Row Level Security) 설정
-- ============================================

-- 모든 테이블에 대해 public read 허용
ALTER TABLE historical_eth_price ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_ethereum_tvl ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_l2_tvl ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_protocol_fees ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_staking ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_gas_burn ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_active_addresses ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_eth_supply ENABLE ROW LEVEL SECURITY;
ALTER TABLE data_collection_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE data_collection_logs ENABLE ROW LEVEL SECURITY;

-- Public read policies
CREATE POLICY "Public read eth_price" ON historical_eth_price FOR SELECT USING (true);
CREATE POLICY "Public read ethereum_tvl" ON historical_ethereum_tvl FOR SELECT USING (true);
CREATE POLICY "Public read l2_tvl" ON historical_l2_tvl FOR SELECT USING (true);
CREATE POLICY "Public read protocol_fees" ON historical_protocol_fees FOR SELECT USING (true);
CREATE POLICY "Public read staking" ON historical_staking FOR SELECT USING (true);
CREATE POLICY "Public read gas_burn" ON historical_gas_burn FOR SELECT USING (true);
CREATE POLICY "Public read active_addresses" ON historical_active_addresses FOR SELECT USING (true);
CREATE POLICY "Public read eth_supply" ON historical_eth_supply FOR SELECT USING (true);
CREATE POLICY "Public read collection_status" ON data_collection_status FOR SELECT USING (true);
CREATE POLICY "Public read collection_logs" ON data_collection_logs FOR SELECT USING (true);

-- Service role (GitHub Actions)만 write 가능
-- 이 정책들은 service_role key를 사용할 때 자동으로 bypass됨

-- ============================================
-- Helper Functions
-- ============================================

-- 날짜 범위로 데이터 조회하는 함수
CREATE OR REPLACE FUNCTION get_historical_data(
    p_table_name TEXT,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_limit INTEGER DEFAULT 1000
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    result JSONB;
    query TEXT;
BEGIN
    query := format('SELECT jsonb_agg(row_to_json(t)) FROM (SELECT * FROM %I WHERE 1=1', p_table_name);
    
    IF p_start_date IS NOT NULL THEN
        query := query || format(' AND date >= %L', p_start_date);
    END IF;
    
    IF p_end_date IS NOT NULL THEN
        query := query || format(' AND date <= %L', p_end_date);
    END IF;
    
    query := query || format(' ORDER BY date DESC LIMIT %s) t', p_limit);
    
    EXECUTE query INTO result;
    RETURN COALESCE(result, '[]'::jsonb);
END;
$$;

-- 데이터 수집 상태 업데이트 함수
CREATE OR REPLACE FUNCTION update_collection_status(
    p_dataset_name TEXT,
    p_status TEXT,
    p_record_count INTEGER DEFAULT NULL,
    p_date_from DATE DEFAULT NULL,
    p_date_to DATE DEFAULT NULL,
    p_error TEXT DEFAULT NULL,
    p_warning TEXT DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    UPDATE data_collection_status
    SET 
        status = p_status,
        record_count = COALESCE(p_record_count, record_count),
        date_from = COALESCE(p_date_from, date_from),
        date_to = COALESCE(p_date_to, date_to),
        last_error = CASE WHEN p_error IS NOT NULL THEN p_error ELSE last_error END,
        last_warning = CASE WHEN p_warning IS NOT NULL THEN p_warning ELSE last_warning END,
        last_run_at = NOW(),
        updated_at = NOW()
    WHERE dataset_name = p_dataset_name;
END;
$$;

-- ============================================
-- 완료 메시지
-- ============================================
-- 이 스크립트를 Supabase SQL Editor에서 실행하면
-- 모든 테이블과 인덱스가 생성됩니다.
-- 
-- 다음 단계:
-- 1. GitHub Actions에서 SUPABASE_URL과 SUPABASE_SERVICE_KEY 설정
-- 2. data-collector.js 실행
-- ============================================
