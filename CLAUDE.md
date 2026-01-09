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
