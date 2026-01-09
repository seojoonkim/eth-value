# ETHval - Ethereum Intrinsic Value Dashboard

이더리움 온체인 데이터를 수집, 분석하여 내재가치를 시각화하는 풀스택 웹 애플리케이션.

## 기술 스택

- **Frontend**: Vanilla HTML/JS, Chart.js, Supabase Client SDK
- **Backend**: Node.js 18+, GitHub Actions (서버리스)
- **Database**: Supabase (PostgreSQL)
- **APIs**: Dune Analytics, Etherscan, CryptoQuant, Anthropic Claude

## 프로젝트 구조

```
ethval/
├── .github/workflows/     # GitHub Actions 워크플로우
│   ├── collect-data.yml   # 메인 데이터 수집 (매일 2회)
│   └── collect-historical-data.yml
├── scripts/
│   ├── data-collector.js  # 메인 데이터 수집기 (v7.4+)
│   └── backfill-gas-price.js
├── index.html             # 메인 대시보드 UI
├── admin.html             # 관리자 패널
└── package.json
```

## 명령어

```bash
npm install              # 의존성 설치
npm run collect          # 데이터 수집 실행
npm run backfill-gas     # 가스 가격 히스토리 백필
```

## 환경 변수

```
SUPABASE_URL
SUPABASE_SERVICE_KEY
DUNE_API_KEY
ETHERSCAN_API_KEY
ANTHROPIC_API_KEY
CRYPTOQUANT_API_KEY
```

## 데이터 카테고리 (7개 섹션, 39-40개 지표)

1. **Investor Sentiment**: Realized Price, MVRV, Fear & Greed, Funding Rate 등
2. **Market Position**: ETH/BTC Ratio, ETH Dominance, NVT Ratio 등
3. **Supply Dynamics**: Staking Yield, Staked ETH, ETH Burned/Issued 등
4. **Network Demand**: Gas Price, Gas Utilization, Network Fees 등
5. **User Activity**: New Addresses, Active Addresses, Transactions 등
6. **Locked Capital**: L1/L2 TVL, DeFi Lending TVL, Stablecoin Supply 등
7. **Settlement Volume**: Total Volume, DEX Volume, Bridge Volume 등

## 주요 패턴 및 규칙

- **Result wrapper 패턴**: 일관된 에러 핸들링 (`ok`, `skip`, `warn`, `fail`)
- **병렬 API 호출**: 성능 최적화를 위한 async/parallel 실행
- **불완전 데이터 필터링**: 진행 중인 날짜 데이터 제외
- **다국어 지원**: EN, KO, JP, ZH
- **테마**: 다크/라이트 모드 지원

## 배포

- **Frontend**: GitHub Pages (빌드 불필요, 정적 호스팅)
- **Data Collection**: GitHub Actions cron (UTC 04:30, 12:00)
- **도메인**: CNAME 파일로 설정

## 코드 스타일

- 한국어 주석 사용
- 이모지 기반 로깅 (GitHub Actions 출력용)
- 비동기 패턴 적극 활용

---

## 12개 밸류에이션 모델

ETHval은 4개 카테고리로 구분된 12개 밸류에이션 모델을 사용하여 이더리움의 내재가치(Composite Fair Value)를 산출합니다. 각 모델의 결과를 단순 평균하여 종합 적정가치를 계산합니다.

### 모델 계산 상수 (`MODEL_CONSTANTS`)

```javascript
const MODEL_CONSTANTS = {
    TVL_MULTIPLE: 7,        // TVL 배수
    L2_WEIGHT: 2,           // L2 TVL 가중치
    L2_MULTIPLE: 6,         // L2 생태계 배수
    PS_RATIO: 25,           // P/S 배수
    DISCOUNT_RATE: 0.09,    // DCF 할인율 (9%)
    GROWTH_RATE: 0.03,      // DCF 성장률 (3%)
    RISK_FREE_RATE: 4.5,    // 무위험수익률 (4.5%)
    RISK_PREMIUM: 1.5,      // 스테이킹 리스크 프리미엄 (1.5%)
    STABLES_RATIO: 0.28,    // 스테이블코인/App Capital 비율 (28%)
    LOST_ETH_PCT: 0.03,     // 분실 ETH 비율 (3%)
    ETH_VELOCITY: 6,        // ETH 화폐 유통속도
    ECOSYSTEM_VELOCITY: 150, // 생태계 유통속도
    METCALFE_COEF: 2,       // 멧칼프 계수
    METCALFE_EXP: 1.5       // 멧칼프 지수
};
```

---

### 카테고리 1: Traditional Finance (TradFi)

전통 금융 방법론을 크립토에 적용. ETH를 수익 창출 자산으로 취급.

#### 1. Staking DCF
- **공식**: `Price × (1 + APR) ÷ (Discount - Growth)`
- **데이터 소스**: Lido API (실시간 stETH APR)
- **논거**: 스테이킹 보상을 영구 현금흐름으로 취급하는 전통 DCF 분석. 9% 할인율 = 4.5% 무위험수익률 + 4.5% 크립토 주식 리스크 프리미엄. 3% 영구 성장률 적용. 높은 리스크 프리미엄은 변동성, 규제, 스마트 컨트랙트 리스크를 반영.
- **한계**: 할인율과 성장률 가정에 매우 민감

#### 2. P/S Ratio (25x)
- **공식**: `(L1Fees + BlobFees) × 365 × PSRatio ÷ Supply`
- **데이터 소스**: Token Terminal (프로토콜 수수료), Dune Analytics (Blob 수수료)
- **논거**: L1 프로토콜은 "순이익"이 없고 모든 수수료가 검증자에게 흘러가므로 P/E가 아닌 P/S가 업계 표준 (Token Terminal 방식). 25배 배수는 성장 기술주 밸류에이션 (SaaS 10-40배 범위) 반영.
- **한계**: 25배 배수는 SaaS 섹터에서 차용; 크립토 프로토콜은 다른 배수가 적절할 수 있음

#### 3. Fee Yield
- **공식**: `(L1Fees + BlobFees) × 365 ÷ APR ÷ Supply`
- **데이터 소스**: 프로토콜 수수료 + Lido stETH APR
- **논거**: ETH를 수익률 채권처럼 취급하여 실시간 스테이킹 APR로 적정가치를 역산. 이더리움이 연간 X 수수료를 생성하고 현재 스테이킹 수익률이 Y%면, 내재 시가총액 = X ÷ Y%. TradFi 애널리스트들이 선호하는 방식.
- **한계**: 안정적인 수익률 환경 가정; 실제 스테이킹 수익률은 변동성 있음

#### 4. Validator Economics
- **공식**: `Price × (Target ÷ APR)`
- **데이터 소스**: Lido API (현재 APR)
- **논거**: 목표 수익률 6% = 미국 10년물 국채 (~4.5%) + 스테이킹 리스크 프리미엄 (~1.5%). DCF의 4.5%보다 낮은 프리미엄은 스테이킹의 낮은 리스크 프로필 반영: 예측 가능한 검증자 보상, 비영구적 손실 없음, 프로토콜 수준 보안. 현재 APR이 목표보다 낮으면 저평가.
- **한계**: 6% 목표 수익률은 주관적; 투자자 리스크 성향에 따라 크게 다름

---

### 카테고리 2: On-chain Asset Value

네트워크에 잠긴/정산된/보안된 자산 기반 가치 평가. 이더리움을 정산 레이어로 취급.

#### 5. TVL Multiple
- **공식**: `TVL × Multiple ÷ Supply`
- **데이터 소스**: DefiLlama (L1 TVL)
- **논거**: DeFi 예치금 기준 가치 평가. 7배 배수는 역사적 MC/TVL 비율에서 도출. 네트워크 보안(시가총액)이 온체인 자산 가치에 비례해야 한다는 전제.
- **한계**: TVL에는 레버리지, 재귀적 예치, 프로토콜 간 이중 계산이 포함될 수 있음

#### 6. App Capital
- **공식**: `AppCapital ÷ Supply`
- **데이터 소스**: DefiLlama (스테이블코인 공급량)
- **논거**: App Capital = 모든 온체인 자산 (스테이블코인, ERC-20, NFT, RWA, 브릿지 자산). 직접 데이터가 없어 2021년 이후 안정적인 28% 스테이블코인 비율로 추정. App Capital은 시가총액의 하한선 역할 - 네트워크 보안(MC)이 모든 정산 자산 가치를 뒷받침해야 함. TVL(DeFi만)과 달리 이더리움의 전체 정산 레이어 역할을 포착.
- **한계**: 스테이블코인이 App Capital의 28%라는 비율 가정

---

### 카테고리 3: Network Effects

사용자 채택, 트랜잭션 활동, 생태계 성장에서 가치 포착. 채택에 따라 비선형적으로 가치 성장.

#### 7. Metcalfe's Law
- **공식**: `Coef × TVL^Exp ÷ Supply`
- **데이터 소스**: DefiLlama (TVL을 네트워크 활동 프록시로 사용)
- **논거**: 네트워크 가치는 활성 사용자/노드 수의 제곱에 비례. 원래 통신용으로 개발되었으며, 학술 연구자들(Alabi 2017, Peterson 2018)에 의해 BTC/ETH에 대해 실증 검증됨. 1.5 지수(선형과 제곱 사이)는 실제 네트워크 마찰 반영.
- **한계**: DAU-가치 계수가 크게 다름; 1.5 지수가 모든 조건에 맞지 않을 수 있음

#### 8. ETH Monetary (MV=PQ)
- **공식**: `(L1 ETH Vol + L2 ETH Vol) × 365 ÷ ETH Velocity ÷ Supply`
- **데이터 소스**: Dune Analytics (L1/L2 ETH 전송량)
- **논거**: 교환 방정식(MV=PQ)으로 ETH를 화폐로 측정. L1/L2의 네이티브 ETH 전송만 계산 - 스테이블코인, DeFi 토큰 제외. ETH의 직접적 화폐 기능을 이더리움의 광범위한 정산 활동과 분리. 유통속도 6은 ETH의 가치저장 특성 반영: 28% 스테이킹 잠금, ETF 후 기관 축적, 장기 보유자 행동. USD M1 유통속도(~5.5배) 및 학술 연구(연간 4-6배)와 일치.
- **한계**: 유통속도 6은 ETH 전송 데이터에서 경험적으로 도출, USD M1(~5.5배)에 벤치마크

#### 9. Ecosystem Settlement (MV=PQ)
- **공식**: `(L1 Total Vol + L2 Total Vol) × 365 ÷ Ecosystem Velocity ÷ Supply`
- **데이터 소스**: Dune Analytics (L1/L2 전체 토큰 전송량)
- **논거**: MV=PQ로 이더리움을 정산 인프라로 측정. 생태계의 모든 토큰 활동 계산: 스테이블코인, DeFi 토큰, NFT - 네이티브 ETH만이 아님. 글로벌 정산 레이어로서 이더리움의 총 경제 처리량 포착. 유통속도 150은 스테이블코인 회전율(~80배/년, McKinsey)에서 도출, 고빈도 DeFi 스왑으로 상향 조정, 봇/MEV 제거(Visa 연구 70-93%)로 하향 조정. ETH Monetary보다 높은 유통속도는 더 빠르게 움직이는 토큰 경제 반영.
- **한계**: 유통속도 150 = 스테이블코인 회전율(~80/년) × 2배 DeFi 배수, BlockSci 4배 자체 회전 조정으로 검증

#### 10. L2 Ecosystem
- **공식**: `(TVL + L2×Weight) × Multiple ÷ Supply`
- **데이터 소스**: DefiLlama (L1 TVL + L2 TVL)
- **논거**: 이더리움 L2 스케일링 생태계(Arbitrum, Optimism, Base, zkSync 등)에서 가치 포착. L2 TVL에 2배 가중치 적용 - L2 활동이 이더리움 메인넷에 정산되어 블록스페이스를 소비하고 EIP-1559를 통해 ETH를 소각하기 때문. 이더리움의 가치가 L1을 넘어 전체 롤업 중심 로드맵과 모듈러 블록체인 테시스로 확장됨을 인식.
- **한계**: 2배 L2 가중치 배수는 경험적 근거 없는 자체 설정

---

### 카테고리 4: Supply Scarcity

감소된 유효 유통 공급량 기반 가격 책정. ETH의 상당 부분이 구조적으로 잠김(스테이킹, DeFi, 분실 코인).

#### 11. Staking Scarcity
- **공식**: `Price × √(Supply ÷ (Supply - Staked))`
- **데이터 소스**: Dune Analytics (스테이킹된 ETH)
- **논거**: 스테이킹으로 유통 공급량이 감소할 때 희소성 프리미엄 적용. 더 많은 ETH가 검증자에 잠길수록(현재 ~28%) 유동 공급량이 줄어들어 이론적으로 가격 압력 증가. 제곱근 함수는 극단적 밸류에이션을 완화하면서 희소성 효과 포착. 이 모델은 ETH의 PoS 전환 후 Merge 이후 관련성 증가.
- **한계**: 이 대시보드용으로 개발된 자체 모델. 피어 리뷰나 학술 검증 없음

#### 12. Liquidity Premium
- **공식**: `Price × √(Supply ÷ Liquid Float)`
- **데이터 소스**: 스테이킹 + DeFi TVL + L2 TVL + 분실 ETH(3%)
- **논거**: Amihud의 유동성 프리미엄 이론 기반: 거래 가능 ETH가 감소하면 시장 유동성 감소가 상승 가격 압력 생성. Liquid Float = 공급량 - 모든 잠긴 ETH(스테이킹, DeFi, L2 브릿지, 분실/휴면 지갑). 모든 비유동 공급을 포함하므로 Staking Scarcity보다 포괄적.
- **한계**: 유동 스테이킹(stETH)은 거래 가능하므로 비유동성을 부분적으로 상쇄

---

### 데이터 흐름 요약

| 입력 데이터 | 데이터 소스 | 사용 모델 |
|------------|-----------|---------|
| ETH 가격 | CryptoQuant | 모든 모델 |
| 유통 공급량 | Etherscan | 모든 모델 |
| L1 TVL | DefiLlama | TVL Multiple, Metcalfe, L2 Ecosystem |
| L2 TVL | DefiLlama | L2 Ecosystem, Liquidity Premium |
| 스테이킹된 ETH | Dune Analytics | Staking Scarcity, Liquidity Premium |
| 스테이킹 APR | Lido API | DCF, Fee Yield, Validator Economics |
| 프로토콜 수수료 | Token Terminal | P/S, Fee Yield |
| Blob 수수료 | Dune Analytics | P/S, Fee Yield |
| 스테이블코인 공급 | DefiLlama | App Capital |
| L1 ETH 전송량 | Dune Analytics | ETH Monetary |
| L2 ETH 전송량 | Dune Analytics | ETH Monetary |
| L1 전체 볼륨 | Dune Analytics | Ecosystem Settlement |
| L2 전체 볼륨 | Dune Analytics | Ecosystem Settlement |
| DeFi 내 ETH | DefiLlama | Liquidity Premium |

---

### 종합 적정가치 계산

```javascript
// 활성화된 모델들의 단순 평균
const avgPrice = sum(enabledModelPrices) / count(enabledModels);

// 판정 기준
if (avgDiff > 15%)  → "UNDERVALUED" (저평가)
if (avgDiff < -15%) → "OVERVALUED" (고평가)
else                → "FAIR VALUE" (적정가치)
```

### 참고 문헌

- Peterson (2018): Metcalfe's Law as a Model for Bitcoin's Value
- Alabi (2017): Digital Blockchain Networks Appear to be Following Metcalfe's Law
- Burniske: Cryptoasset Valuations (MV=PQ Framework)
- Pernice et al. (2020): Cryptocurrencies and the Velocity of Money
- Amihud & Mendelson: Liquidity and Asset Prices
- McKinsey (2025): Stablecoin Velocity Research
- Token Terminal: Price-to-Earnings Ratio Methodology
