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

-- 완료 확인
SELECT 'Migration completed successfully!' as status;
