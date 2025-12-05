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

-- 완료 확인
SELECT 'Migration completed successfully!' as status;
