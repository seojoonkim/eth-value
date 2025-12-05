# ETHval Data Collection 설정 가이드

## 🚀 빠른 설정 (3단계)

### 1️⃣ Supabase SQL 실행
1. Supabase Dashboard → SQL Editor
2. `supabase-schema.sql` 내용 복사 & 붙여넣기
3. Run 클릭

### 2️⃣ GitHub Secrets 설정
GitHub 저장소 (https://github.com/seojoonkim/eth-value) → Settings → Secrets and variables → Actions

| Secret | 값 | 필수 |
|--------|-----|------|
| `SUPABASE_URL` | `https://rliwxedrifwxbudcutqs.supabase.co` | ✅ |
| `SUPABASE_SERVICE_KEY` | Supabase Dashboard → Settings → API → service_role (secret) | ✅ |
| `ETHERSCAN_API_KEY` | https://etherscan.io/apis 에서 발급 | 권장 |

### 3️⃣ GitHub Actions 실행
1. https://github.com/seojoonkim/eth-value/actions
2. "Collect Historical Data" 클릭
3. "Run workflow" 클릭

---

## 📁 파일 구조

```
eth-value/
├── index.html           # 메인 (admin 링크 추가됨)
├── admin.html           # 데이터 수집 상태 모니터링
├── supabase-schema.sql  # DB 테이블 생성 스크립트
├── scripts/
│   └── data-collector.js  # GitHub Actions에서 실행
└── .github/workflows/
    └── collect-historical-data.yml  # 매일 KST 15:00 자동 실행
```

---

## ⏰ 자동 실행

- **매일 KST 15:00 (UTC 06:00)** 자동 실행
- Actions 탭에서 수동 실행도 가능

---

## 🔐 Admin 페이지

- URL: `https://ethval.com/admin.html`
- 기본 비밀번호: `ethvaladmin`

---

## ⚠️ 수동 수집 필요

Admin 페이지에서 **Partial** 상태인 경우:

| 데이터 | 소스 |
|--------|------|
| Active Addresses | [Dune Analytics](https://dune.com/browse/dashboards?q=ethereum%20active%20addresses) |
| Staking Data | [beaconcha.in](https://beaconcha.in/charts) |
