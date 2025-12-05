-- ============================================
-- ETHval Schema Migration
-- 기존 테이블에 source 컬럼 추가
-- ============================================

-- Staking 테이블에 컬럼 추가
ALTER TABLE historical_staking 
ADD COLUMN IF NOT EXISTS total_staked_eth DECIMAL(24, 8);

ALTER TABLE historical_staking 
ADD COLUMN IF NOT EXISTS total_validators INTEGER;

ALTER TABLE historical_staking 
ADD COLUMN IF NOT EXISTS avg_apr DECIMAL(8, 4);

ALTER TABLE historical_staking 
ADD COLUMN IF NOT EXISTS source VARCHAR(50);

-- Gas & Burn 테이블에 컬럼 추가
ALTER TABLE historical_gas_burn 
ADD COLUMN IF NOT EXISTS eth_burnt DECIMAL(24, 8);

ALTER TABLE historical_gas_burn 
ADD COLUMN IF NOT EXISTS source VARCHAR(50);

-- Active Addresses 테이블에 컬럼 추가
ALTER TABLE historical_active_addresses 
ADD COLUMN IF NOT EXISTS source VARCHAR(50);

-- ETH Supply 테이블에 컬럼 추가
ALTER TABLE historical_eth_supply 
ADD COLUMN IF NOT EXISTS source VARCHAR(50);

-- ============================================
-- Fear & Greed 테이블 추가 (새 테이블)
-- ============================================
CREATE TABLE IF NOT EXISTS historical_fear_greed (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    value INTEGER,
    classification VARCHAR(20),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fear_greed_date ON historical_fear_greed(date);

-- RLS 설정
ALTER TABLE historical_fear_greed ENABLE ROW LEVEL SECURITY;

-- Read policy (이미 존재하면 무시)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies 
        WHERE tablename = 'historical_fear_greed' 
        AND policyname = 'Public read fear_greed'
    ) THEN
        CREATE POLICY "Public read fear_greed" ON historical_fear_greed FOR SELECT USING (true);
    END IF;
END $$;

-- 데이터셋 상태 추가
INSERT INTO data_collection_status (dataset_name, status) VALUES
    ('fear_greed', 'pending')
ON CONFLICT (dataset_name) DO NOTHING;

-- ============================================
-- 새 테이블들 추가 (v2 - 전체 차트 데이터)
-- ============================================

-- DEX Volume
CREATE TABLE IF NOT EXISTS historical_dex_volume (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    volume DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dex_volume_date ON historical_dex_volume(date);

-- Stablecoins
CREATE TABLE IF NOT EXISTS historical_stablecoins (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    total_mcap DECIMAL(24, 2),
    usdt_mcap DECIMAL(24, 2),
    usdc_mcap DECIMAL(24, 2),
    dai_mcap DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stablecoins_date ON historical_stablecoins(date);

-- ETH/BTC Ratio
CREATE TABLE IF NOT EXISTS historical_eth_btc (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    ratio DECIMAL(18, 8),
    eth_price DECIMAL(18, 2),
    btc_price DECIMAL(18, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_eth_btc_date ON historical_eth_btc(date);

-- Funding Rate
CREATE TABLE IF NOT EXISTS historical_funding_rate (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    funding_rate DECIMAL(18, 8),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_funding_rate_date ON historical_funding_rate(date);

-- Exchange Reserve
CREATE TABLE IF NOT EXISTS historical_exchange_reserve (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    reserve_eth DECIMAL(24, 8),
    reserve_usd DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_exchange_reserve_date ON historical_exchange_reserve(date);

-- ETH Dominance
CREATE TABLE IF NOT EXISTS historical_eth_dominance (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    eth_dominance DECIMAL(8, 4),
    btc_dominance DECIMAL(8, 4),
    total_mcap DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_eth_dominance_date ON historical_eth_dominance(date);

-- Blob Data
CREATE TABLE IF NOT EXISTS historical_blob_data (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    blob_count INTEGER,
    blob_gas_used DECIMAL(24, 0),
    blob_fee_eth DECIMAL(24, 8),
    blob_fee_usd DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_blob_data_date ON historical_blob_data(date);

-- Lending TVL
CREATE TABLE IF NOT EXISTS historical_lending_tvl (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    total_tvl DECIMAL(24, 2),
    aave_tvl DECIMAL(24, 2),
    compound_tvl DECIMAL(24, 2),
    maker_tvl DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_lending_tvl_date ON historical_lending_tvl(date);

-- Volatility
CREATE TABLE IF NOT EXISTS historical_volatility (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    volatility_30d DECIMAL(8, 4),
    volatility_7d DECIMAL(8, 4),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_volatility_date ON historical_volatility(date);

-- NVT Ratio
CREATE TABLE IF NOT EXISTS historical_nvt (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    nvt_ratio DECIMAL(18, 4),
    market_cap DECIMAL(24, 2),
    tx_volume_usd DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_nvt_date ON historical_nvt(date);

-- Transactions
CREATE TABLE IF NOT EXISTS historical_transactions (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    tx_count INTEGER,
    tx_volume_eth DECIMAL(24, 8),
    tx_volume_usd DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON historical_transactions(date);

-- ============================================
-- RLS 설정 for new tables
-- ============================================
ALTER TABLE historical_dex_volume ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_stablecoins ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_eth_btc ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_funding_rate ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_exchange_reserve ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_eth_dominance ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_blob_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_lending_tvl ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_volatility ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_nvt ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_transactions ENABLE ROW LEVEL SECURITY;

-- Read policies
DO $$ BEGIN CREATE POLICY "Public read dex_volume" ON historical_dex_volume FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read stablecoins" ON historical_stablecoins FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read eth_btc" ON historical_eth_btc FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read funding_rate" ON historical_funding_rate FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read exchange_reserve" ON historical_exchange_reserve FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read eth_dominance" ON historical_eth_dominance FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read blob_data" ON historical_blob_data FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read lending_tvl" ON historical_lending_tvl FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read volatility" ON historical_volatility FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read nvt" ON historical_nvt FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read transactions" ON historical_transactions FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 새 데이터셋 상태 추가
INSERT INTO data_collection_status (dataset_name, status) VALUES
    ('dex_volume', 'pending'),
    ('stablecoins', 'pending'),
    ('eth_btc', 'pending'),
    ('funding_rate', 'pending'),
    ('exchange_reserve', 'pending'),
    ('eth_dominance', 'pending'),
    ('blob_data', 'pending'),
    ('lending_tvl', 'pending'),
    ('volatility', 'pending'),
    ('nvt', 'pending'),
    ('transactions', 'pending')
ON CONFLICT (dataset_name) DO NOTHING;

-- ============================================
-- v3: 추가 테이블들 (L2, Protocol, APR 등)
-- ============================================

-- L2 Transactions
CREATE TABLE IF NOT EXISTS historical_l2_transactions (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    timestamp BIGINT,
    chain VARCHAR(50) NOT NULL,
    tx_count INTEGER,
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, chain)
);
CREATE INDEX IF NOT EXISTS idx_l2_tx_date ON historical_l2_transactions(date);

-- L2 Active Addresses
CREATE TABLE IF NOT EXISTS historical_l2_addresses (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    timestamp BIGINT,
    chain VARCHAR(50) NOT NULL,
    active_addresses INTEGER,
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, chain)
);
CREATE INDEX IF NOT EXISTS idx_l2_addr_date ON historical_l2_addresses(date);

-- Protocol TVL
CREATE TABLE IF NOT EXISTS historical_protocol_tvl (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    timestamp BIGINT,
    protocol VARCHAR(50) NOT NULL,
    tvl DECIMAL(24, 2),
    chain VARCHAR(50) DEFAULT 'Ethereum',
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, protocol, chain)
);
CREATE INDEX IF NOT EXISTS idx_protocol_tvl_date ON historical_protocol_tvl(date);

-- Staking APR
CREATE TABLE IF NOT EXISTS historical_staking_apr (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    lido_apr DECIMAL(8, 4),
    rocketpool_apr DECIMAL(8, 4),
    cbeth_apr DECIMAL(8, 4),
    avg_apr DECIMAL(8, 4),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staking_apr_date ON historical_staking_apr(date);

-- ETH in DeFi
CREATE TABLE IF NOT EXISTS historical_eth_in_defi (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    eth_locked DECIMAL(24, 8),
    eth_locked_usd DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_eth_defi_date ON historical_eth_in_defi(date);

-- Global Market Cap
CREATE TABLE IF NOT EXISTS historical_global_mcap (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    total_mcap DECIMAL(24, 2),
    eth_mcap DECIMAL(24, 2),
    btc_mcap DECIMAL(24, 2),
    eth_dominance DECIMAL(8, 4),
    btc_dominance DECIMAL(8, 4),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_global_mcap_date ON historical_global_mcap(date);

-- DEX by Protocol
CREATE TABLE IF NOT EXISTS historical_dex_by_protocol (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    timestamp BIGINT,
    protocol VARCHAR(50) NOT NULL,
    volume DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, protocol)
);
CREATE INDEX IF NOT EXISTS idx_dex_protocol_date ON historical_dex_by_protocol(date);

-- Network Stats
CREATE TABLE IF NOT EXISTS historical_network_stats (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    timestamp BIGINT,
    block_count INTEGER,
    avg_block_time DECIMAL(8, 4),
    difficulty DECIMAL(32, 0),
    hash_rate DECIMAL(24, 2),
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_network_stats_date ON historical_network_stats(date);

-- RLS for new tables
ALTER TABLE historical_l2_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_l2_addresses ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_protocol_tvl ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_staking_apr ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_eth_in_defi ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_global_mcap ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_dex_by_protocol ENABLE ROW LEVEL SECURITY;
ALTER TABLE historical_network_stats ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN CREATE POLICY "Public read l2_transactions" ON historical_l2_transactions FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read l2_addresses" ON historical_l2_addresses FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read protocol_tvl" ON historical_protocol_tvl FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read staking_apr" ON historical_staking_apr FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read eth_in_defi" ON historical_eth_in_defi FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read global_mcap" ON historical_global_mcap FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read dex_by_protocol" ON historical_dex_by_protocol FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE POLICY "Public read network_stats" ON historical_network_stats FOR SELECT USING (true); EXCEPTION WHEN duplicate_object THEN NULL; END $$;

INSERT INTO data_collection_status (dataset_name, status) VALUES
    ('l2_transactions', 'pending'),
    ('l2_addresses', 'pending'),
    ('protocol_tvl', 'pending'),
    ('staking_apr', 'pending'),
    ('eth_in_defi', 'pending'),
    ('global_mcap', 'pending'),
    ('dex_by_protocol', 'pending'),
    ('network_stats', 'pending')
ON CONFLICT (dataset_name) DO NOTHING;

-- 완료 확인
SELECT 'Migration v3 completed successfully!' as status;
