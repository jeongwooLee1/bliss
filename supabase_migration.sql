-- ========================================
-- Bliss v2.62.0 - 다회권 + 알림톡 마이그레이션
-- Supabase SQL Editor에서 실행하세요
-- ========================================

-- 1) services 테이블에 다회권 컬럼 추가
ALTER TABLE services ADD COLUMN IF NOT EXISTS is_package boolean DEFAULT false;
ALTER TABLE services ADD COLUMN IF NOT EXISTS pkg_count integer DEFAULT 0;
ALTER TABLE services ADD COLUMN IF NOT EXISTS pkg_price_f numeric DEFAULT 0;
ALTER TABLE services ADD COLUMN IF NOT EXISTS pkg_price_m numeric DEFAULT 0;

-- 2) 고객 다회권 보유 테이블
CREATE TABLE IF NOT EXISTS customer_packages (
  id text PRIMARY KEY,
  business_id text NOT NULL,
  customer_id text NOT NULL,
  service_id text NOT NULL,
  service_name text DEFAULT '',
  total_count integer NOT NULL DEFAULT 0,
  used_count integer NOT NULL DEFAULT 0,
  purchased_at timestamp with time zone DEFAULT now(),
  expires_at timestamp with time zone,
  note text DEFAULT '',
  created_at timestamp with time zone DEFAULT now()
);

-- RLS 비활성화 (개발용 - 프로덕션에서는 정책 설정 필요)
ALTER TABLE customer_packages ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all" ON customer_packages FOR ALL USING (true) WITH CHECK (true);

-- 3) 알림 로그 테이블 (향후 알림톡/SMS 발송 기록)
CREATE TABLE IF NOT EXISTS notification_logs (
  id text PRIMARY KEY,
  business_id text NOT NULL,
  customer_id text,
  reservation_id text,
  type text DEFAULT 'alimtalk',
  template text DEFAULT '',
  phone text DEFAULT '',
  message text DEFAULT '',
  status text DEFAULT 'pending',
  sent_at timestamp with time zone,
  created_at timestamp with time zone DEFAULT now()
);

ALTER TABLE notification_logs ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all" ON notification_logs FOR ALL USING (true) WITH CHECK (true);

-- 4) 인덱스
CREATE INDEX IF NOT EXISTS idx_cp_customer ON customer_packages(customer_id);
CREATE INDEX IF NOT EXISTS idx_cp_business ON customer_packages(business_id);
CREATE INDEX IF NOT EXISTS idx_nl_business ON notification_logs(business_id);
