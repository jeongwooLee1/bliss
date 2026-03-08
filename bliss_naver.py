"""
bliss_naver.py — 네이버 예약 → Bliss 자동 동기화 (v3)

upsert 전략:
  - 존재하는 rid → Naver API 최신 데이터로 덮어씌움
  - Bliss에서 수동 설정한 필드(room_id, staff_id, cust_id)만 보존
    
중복 방지:
  - processed_mails.json: 재시작 후 재처리 방지
  - queued_set: 큐 중복 방지
  - 파싱 실패 시 processed에 추가 안 함 → 재시도 가능
"""

import imaplib, email, re, requests, json, logging, os, time, threading, string, random
from queue import Queue, Empty
from email.header import decode_header
from playwright.sync_api import sync_playwright

# ─── 설정 ─────────────────────────────────────────────────────────────────────
GMAIL_USER         = "housewaxing@gmail.com"
GMAIL_APP_PASSWORD = "swqb mqhr qznp ljjd"
SUPABASE_URL       = "https://dpftlrsuqxqqeouwbfjd.supabase.co"
SUPABASE_KEY       = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
                      ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRwZnRscnN1cXhxcWVvdXdiZmpkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE5MDU4MjQsImV4cCI6MjA4NzQ4MTgyNH0"
                      ".iydEkjtPjZ0jXpUUPJben4IWWneDqLomv-HDlcFayE4")
BUSINESS_ID        = "biz_khvurgshb"
SINCE_DATE         = "01-Jan-2025"
BEFORE_DATE        = None
TEST_BIZ_FILTER    = None

from config import NAVER_ID, NAVER_PW, SESSION_FILE

# ─── 로깅 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bliss_naver")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

# ─── 작업 큐 ──────────────────────────────────────────────────────────────────
task_queue: Queue = Queue()
queued_set: set   = set()
queued_lock       = threading.Lock()

fail_counts: dict = {}
FAIL_COUNTS_FILE  = "fail_counts.json"
PROCESSED_FILE    = "processed_mails.json"

def load_fail_counts():
    global fail_counts
    try:
        if os.path.exists(FAIL_COUNTS_FILE):
            fail_counts = json.load(open(FAIL_COUNTS_FILE))
    except:
        fail_counts = {}

def save_fail_counts():
    try:
        json.dump(fail_counts, open(FAIL_COUNTS_FILE, "w"))
    except:
        pass

# ─── 영구 processed set ───────────────────────────────────────────────────────
_processed_persistent: set = set()

def load_processed():
    global _processed_persistent
    try:
        if os.path.exists(PROCESSED_FILE):
            _processed_persistent = set(str(x) for x in json.load(open(PROCESSED_FILE)))
            log.info(f"처리 기록 로드: {len(_processed_persistent)}건")
    except:
        _processed_persistent = set()

def save_processed(uid_str: str):
    _processed_persistent.add(uid_str)
    try:
        json.dump(list(_processed_persistent), open(PROCESSED_FILE, "w"))
    except:
        pass

def is_processed(uid) -> bool:
    return str(uid) in _processed_persistent

# ─── 캐시 ─────────────────────────────────────────────────────────────────────
_branches: dict = {}
_services: list = []

def load_cache():
    global _branches, _services
    r = requests.get(f"{SUPABASE_URL}/rest/v1/branches?select=*", headers=HEADERS, timeout=10)
    _branches = {b["naver_biz_id"]: b for b in r.json() if b.get("naver_biz_id")}
    r2 = requests.get(f"{SUPABASE_URL}/rest/v1/services?select=*&business_id=eq.{BUSINESS_ID}", headers=HEADERS, timeout=10)
    _services = r2.json() if r2.ok else []
    log.info(f"캐시 로드: 지점 {len(_branches)}개 / 서비스 {len(_services)}개")
    _build_bid_to_biz()

# ─── 고객 매칭 ──────────────────────────────────────────────────────────────
def find_cust_by_phone(phone: str, business_id: str):
    """전화번호로 고객 조회 - 매칭되면 cust dict 반환, 없으면 None"""
    if not phone or len(phone) < 8:
        return None
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/customers?select=id,name,phone,gender&business_id=eq.{business_id}&phone=eq.{phone}&limit=1",
            headers=HEADERS, timeout=10
        )
        rows = r.json() if r.ok else []
        return rows[0] if rows else None
    except Exception as e:
        log.warning(f"고객 조회 실패: {e}")
        return None

# ─── Supabase helpers ─────────────────────────────────────────────────────────
def _gen_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))

# Bliss에서 수동으로 설정하는 필드 → Naver API로 덮어쓰지 않음
BLISS_PRESERVE_FIELDS = {
    "room_id",        # 타임라인에서 배정한 룸
    "staff_id",       # 담당 직원
    "cust_id",        # 고객 연동 ID
    "selected_tags",  # Bliss 서비스태그
    "memo",           # 직원 메모 (스크래퍼가 덮어쓰지 않음)
    "is_new_cust",    # 최초 등록 시에만 설정, 이후 덮어쓰지 않음
}

def db_upsert(rid: str, data: dict):
    # reservation_id(rid)를 단일 키로 사용
    # 이름/연락처 등 다른 필드는 일치 기준으로 사용하지 않음
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/reservations"
        f"?reservation_id=eq.{rid}"
        "&select=id,room_id,staff_id,cust_id,selected_tags",
        headers=HEADERS, timeout=10
    )
    existing = r.json()

    if existing:
        # ── 업데이트: Naver API 최신 데이터로 전부 덮어씌움 ──
        row = existing[0]
        # naver_cancelled 상태인 예약을 confirmed로 되살리지 않음
        # 단, naver_cancelled_dt(취소일시)가 실제로 있을 때만 차단
        # (취소일시 없이 잘못 naver_cancelled로 저장된 케이스는 복원 허용)
        if row.get("status") == "naver_cancelled" and data.get("status") == "confirmed":
            cancelled_dt = (row.get("naver_cancelled_dt") or "").strip()
            if cancelled_dt:
                log.info(f"  ⏭  #{rid} 이미 naver_cancelled (취소일시:{cancelled_dt}) → confirmed 재생성 차단")
                return
            else:
                log.info(f"  ⚠️  #{rid} naver_cancelled이지만 취소일시 없음 → confirmed 복원 허용")
        # PRESERVE_FIELDS는 Bliss에서 수동 설정한 값 보존
        # 단, cust_id가 비어있는 경우엔 스크래퍼 매칭값으로 채움
        existing_cust_id = (row.get("cust_id") or "").strip()
        update = {k: v for k, v in data.items() if k not in BLISS_PRESERVE_FIELDS}
        if not existing_cust_id and data.get("cust_id"):
            update["cust_id"] = data["cust_id"]
            update["is_new_cust"] = False
        if update:
            requests.patch(
                f"{SUPABASE_URL}/rest/v1/reservations?id=eq.{row['id']}",
                headers=HEADERS, json=update, timeout=10
            )
            log.info(f"  ✅ #{rid} 업데이트")
        else:
            log.info(f"  ⏭  #{rid} 변경사항 없음")

    else:
        # ── 신규 등록 ──
        new_row = {
            "id":               _gen_id(),
            "business_id":      BUSINESS_ID,
            "type":             "reservation",
            "is_schedule":      False,
            "is_new_cust":      True,
            "selected_tags":    [],
            "repeat":           "none",
            "repeat_until":     "",
            "repeat_group_id":  "",
            "room_id":          "",
            "cust_id":          "",
            "staff_id":         "",
            "service_id":       "",
            "source":           "naver",
            "reservation_id":   rid,
            **data,
        }
        requests.post(f"{SUPABASE_URL}/rest/v1/reservations", headers=HEADERS, json=new_row, timeout=10)
        log.info(f"  ✅ #{rid} 신규 등록")


def db_cancel(rid: str):
    """취소 메일 처리: 이미 있으면 상태 변경, 없으면 스크래핑 후 취소 상태로 등록"""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/reservations?reservation_id=eq.{rid}&select=id,status",
        headers=HEADERS, timeout=10
    )
    rows = r.json()
    if rows:
        cur_status = rows[0].get("status", "")
        if cur_status == "naver_cancelled":
            log.info(f"  ⏭  #{rid} 이미 취소 상태")
            return
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/reservations?id=eq.{rows[0]['id']}",
            headers=HEADERS, json={"status": "naver_cancelled"}, timeout=10
        )
        log.info(f"  ✅ #{rid} 취소 처리")
    else:
        # DB에 없음 → 스크래핑해서 취소 상태로 저장 (취소 메일이 접수 메일보다 먼저 도착한 경우)
        log.warning(f"  #{rid} DB 없음 → 스크래핑 후 취소 등록")
        # scraper_thread에서 action=cancel로 처리하므로 여기선 최소 레코드만
        db_upsert(rid, {
            "status": "naver_cancelled",
            "bid": "", "date": "", "time": "", "dur": 0,
            "cust_name": "", "cust_phone": "", "cust_gender": "",
            "selected_services": [], "memo": "",
            "is_scraping_done": False,
        })


# ─── 이메일 ───────────────────────────────────────────────────────────────────
def _dmh(s: str) -> str:
    if not s: return ""
    parts = decode_header(s)
    out = []
    for b, enc in parts:
        if isinstance(b, bytes):
            out.append(b.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(str(b))
    return "".join(out)

def _get_body(msg) -> str:
    plain = html = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            pl = part.get_payload(decode=True)
            if not pl: continue
            text = pl.decode(part.get_content_charset() or "utf-8", errors="replace")
            if ct == "text/plain" and not plain: plain = text
            elif ct == "text/html" and not html: html = text
    else:
        pl = msg.get_payload(decode=True)
        if pl:
            text = pl.decode(msg.get_content_charset() or "utf-8", errors="replace")
            if msg.get_content_type() == "text/plain": plain = text
            else: html = text
    return plain if plain else html

def _extract_rid(subj: str, body: str) -> str:
    """예약번호 추출 — 다양한 패턴. 변경 메일의 경우 신규예약내역 번호 우선"""
    # 변경 메일: 신규예약내역 섹션의 예약번호 우선 추출
    if "변경" in subj:
        m = re.search(r"신규예약내역.*?예약번호\s*[\|]?\s*(\d{7,12})", body, re.DOTALL)
        if m: return m.group(1)
        # 본문 앞부분(신규예약)의 첫 번째 예약번호
        m = re.search(r"예약번호\s*[\|]?\s*(\d{7,12})", body)
        if m: return m.group(1)

    patterns = [
        r"예약\s*번\s*호\s*[:\s]*(\d{7,12})",
        r"예약번호[:\s]*#?(\d{7,12})",
        r"bookingId[:\s=]*(\d{7,12})",
        r"booking_id[:\s=]*(\d{7,12})",
        r"/bookings/(\d{7,12})",
        r"#(\d{9,12})\b",
        r"\b(\d{10,12})\b",
    ]
    for pat in patterns[:4]:
        m = re.search(pat, subj)
        if m: return m.group(1)
    for pat in patterns:
        m = re.search(pat, body)
        if m: return m.group(1)
    return None

def _extract_old_rid(body: str) -> str:
    """변경 메일에서 구예약번호(예약히스토리내역) 추출"""
    # 예약히스토리내역 섹션의 예약번호
    m = re.search(r"예약히스토리내역.*?예약번호\s*[\|]?\s*(\d{7,12})", body, re.DOTALL)
    if m: return m.group(1)
    # 두 번째로 나오는 예약번호
    all_rids = re.findall(r"예약번호\s*[\|]?\s*(\d{7,12})", body)
    if len(all_rids) >= 2:
        return all_rids[1]
    return None

def _extract_biz_id(subj: str, body: str) -> str:
    """지점명으로 biz_id 추출 — 제목+본문 앞부분"""
    combined = subj + " " + body[:500]
    for nbid, br in _branches.items():
        short     = br.get("short", "")
        name      = br.get("name", "")
        short_base = short.replace("점", "")
        name_base  = re.sub(r"^하우스왁싱\s*", "", name)
        for kw in [short, name, short_base, name_base]:
            if kw and kw in combined:
                return nbid
    if len(_branches) == 1:
        return list(_branches.keys())[0]
    return None

def parse_email(subj: str, body: str):
    """메일 → (rid, biz_id, action, old_rid)"""
    rid    = _extract_rid(subj, body)
    biz_id = _extract_biz_id(subj, body)

    if   "취소" in subj: action = "cancel"
    elif "확정" in subj: action = "confirm"
    elif "변경" in subj: action = "change"
    elif any(w in subj for w in ("접수", "신규", "신청")): action = "new"
    else: action = "unknown"

    # 변경 메일: 구예약번호 추출
    old_rid = _extract_old_rid(body) if action == "change" else None

    return rid, biz_id, action, old_rid

# ─── Playwright helpers ───────────────────────────────────────────────────────
def _human_type(page, selector, text):
    el = page.query_selector(selector)
    if not el: return
    el.click(); time.sleep(0.2)
    for ch in text:
        page.type(selector, ch, delay=random.uniform(60, 160))

def _is_logged_in(page):
    url = page.url
    return "login" not in url and "nid.naver" not in url


# ─── 범용 네이버 폼 파싱 헬퍼 ─────────────────────────────────────────────────
def _parse_forms(snap, item):
    """
    네이버 예약 폼 응답 범용 파싱.
    업체/버전마다 다른 구조를 모두 커버:
    - snapshotJson.customFormInputJson  (일반적)
    - snapshotJson.questionFormInputJson (일부 업체)
    - item.customFormInputJson           (snapshotJson 없을 때)
    - JSON string / list 둘 다 처리
    - 키 조합: title/value, question/answer, label/input 등
    """
    def _extract(raw):
        if raw is None:
            return []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                return []
        if not isinstance(raw, list):
            return []
        result = []
        TITLE_KEYS = ("title", "question", "label", "name", "questionTitle", "itemTitle")
        VALUE_KEYS = ("value", "answer", "input", "response", "userInput", "answerValue")
        for f in raw:
            if not isinstance(f, dict):
                continue
            title = next((f[k] for k in TITLE_KEYS if f.get(k)), None)
            value = next((f[k] for k in VALUE_KEYS if f.get(k)), None)
            if title and value:
                result.append({"title": str(title).strip(), "value": str(value).strip()})
        return result

    seen = set()
    combined = []
    for raw in [
        snap.get("customFormInputJson"),
        snap.get("questionFormInputJson"),
        snap.get("requestFormInputJson"),
        item.get("customFormInputJson"),
        item.get("questionFormInputJson"),
    ]:
        for entry in _extract(raw):
            key = (entry["title"], entry["value"])
            if key not in seen:
                seen.add(key)
                combined.append(entry)
    return combined



def _build_request_msg(raw: dict) -> str:
    """
    네이버 예약의 모든 항목을 JSON 배열로 저장.
    형식: [{"label": "시술메뉴", "value": "음모왁싱"}, ...]
    - 지점/예약마다 항목이 달라도 있는 그대로 저장
    - 주차 안내문 같은 운영 공지는 제외
    """
    import json as _json

    SKIP_PATTERNS = (
        "차량번호 기재",
        "주차장이 대부분 만차",
        "주차시간 예상하셔서",
    )

    items = []

    # 0. 상품명 (bizItemName)
    biz_item = (raw.get("biz_item_name") or "").strip()
    if biz_item:
        items.append({"label": "상품", "value": biz_item})

    # 1. 시술메뉴
    services = raw.get("services") or []
    if services:
        items.append({"label": "시술메뉴", "value": ", ".join(services)})

    # 2. 유입경로
    area = (raw.get("area_name") or "").strip()
    if area:
        items.append({"label": "유입경로", "value": area})

    # 3. 고객 직접 요청사항 (requestMessage) - 줄별로 파싱
    req = (raw.get("request_msg") or "").strip()
    if req:
        for line in req.split("\n"):
            line = line.strip()
            if not line:
                continue
            if any(p in line for p in SKIP_PATTERNS):
                colon_idx = line.rfind(": ")
                if colon_idx != -1:
                    answer = line[colon_idx + 2:].strip()
                    if answer:
                        items.append({"label": "주차여부", "value": answer})
            else:
                colon_idx = line.rfind(": ")
                if colon_idx != -1:
                    label = line[:colon_idx].strip()
                    value = line[colon_idx + 2:].strip()
                    if label and value:
                        items.append({"label": label, "value": value})
                else:
                    items.append({"label": "요청", "value": line})

    # 4. 폼 응답 (업체가 설정한 모든 질문)
    seen = set()
    for form in (raw.get("forms") or []):
        title = (form.get("title") or "").strip()
        value = (form.get("value") or "").strip()
        if not title or not value:
            continue
        key = (title, value)
        if key in seen:
            continue
        seen.add(key)
        # 이미 위에서 처리된 항목(시술메뉴, 유입경로 등) 중복 제거
        already = any(it["label"] == title and it["value"] == value for it in items)
        if already:
            continue
        if any(p in title for p in SKIP_PATTERNS):
            items.append({"label": "주차여부", "value": value})
        else:
            items.append({"label": title, "value": value})

    return _json.dumps(items, ensure_ascii=False)


# ─── Naver API 스크래핑 ────────────────────────────────────────────────────────
def scrape_reservation(biz_id: str, rid: str):
    from datetime import timezone, timedelta, datetime

    try:
        session_data = json.load(open(SESSION_FILE))
        cookies = {c["name"]: c["value"] for c in session_data.get("cookies", [])}
    except Exception as e:
        log.error(f"  세션 로드 실패: {e}")
        return None

    url = f"https://partner.booking.naver.com/api/businesses/{biz_id}/bookings/{rid}"
    hdrs = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Referer": f"https://partner.booking.naver.com/bizes/{biz_id}/booking-list-view",
        "Accept": "application/json",
    }

    try:
        r = requests.get(url, cookies=cookies, headers=hdrs, timeout=15)
    except Exception as e:
        log.error(f"  API 요청 실패: {e}")
        return None

    if r.status_code in (401, 403):
        log.warning("  API 세션 만료")
        return None
    if r.status_code == 404:
        log.warning(f"  #{rid} 예약 없음 (404)")
        return None
    if r.status_code != 200:
        log.error(f"  API 오류: {r.status_code}")
        return None

    item = r.json()

    STATUS_MAP = {
        "AB00": "confirmed",       "AB01": "pending",
        "AB02": "naver_cancelled", "AB03": "naver_cancelled",
        "RC08": "confirmed",       "RC01": "pending",
        "RC02": "naver_cancelled", "RC03": "naver_cancelled",
    }
    status = STATUS_MAP.get(item.get("bookingStatusCode", ""), "confirmed")

    # 날짜/시간 UTC→KST
    start_date = item.get("startDate", "")
    start_time = ""
    snap = item.get("snapshotJson") or {}
    sdt = snap.get("startDateTime", "")
    if sdt:
        try:
            dt_utc = datetime.strptime(sdt[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            dt_kst = dt_utc.astimezone(timezone(timedelta(hours=9)))
            start_date = dt_kst.strftime("%Y-%m-%d")
            start_time = dt_kst.strftime("%H:%M")
        except:
            pass

    # 서비스명
    services = []
    for opt in (item.get("bookingOptionJson") or []):
        nm = opt.get("name", "").strip()
        if nm: services.append(nm)
    if not services and item.get("bizItemName"):
        services.append(item["bizItemName"])

    return {
        "name":               item.get("name", ""),
        "phone":              item.get("phone", ""),
        "email":              item.get("email", ""),
        "date":               start_date,
        "time":               start_time,
        "status":             status,
        "services":           services,
        "deposit":            item.get("price", 0) or 0,
        "total_price":        item.get("totalPrice", 0) or 0,
        "coupon_price":       item.get("couponPrice", 0) or 0,
        "refund_price":       item.get("refundPrice", 0) or 0,
        "is_npay":            item.get("isNPayUsed", False),
        "npay_method":        item.get("nPayChargedName", ""),
        # 선불결제: payments에 PRE+PAID가 있고 npay_method(결제수단)가 있을 때만 결제 완료
        "is_prepaid":         (
            bool(item.get("nPayChargedName")) and
            any(
                p.get("moment") == "PRE" and p.get("status") == "PAID"
                for p in (item.get("payments") or [])
            )
        ),
        "request_msg":        item.get("requestMessage", ""),
        "owner_comment":      item.get("ownerCommentBody", ""),
        "is_completed":       item.get("isCompleted", False),
        "visit_count":        item.get("completedCount", 0),
        "no_show_count":      item.get("noShowCount", 0),
        "is_blacklist":       item.get("isBlacklist", False),
        "biz_item_name":      item.get("bizItemName", ""),
        "area_name":          item.get("areaName", ""),
        "forms":              _parse_forms(snap, item),
        "reg_datetime":       item.get("regDateTime", ""),
        "confirmed_datetime": item.get("confirmedDateTime", ""),
        "cancelled_datetime": item.get("cancelledDateTime", ""),
    }


# ─── 스크래퍼 스레드 ──────────────────────────────────────────────────────────
def _process_one(rid, biz_id, action, old_rid):
    """예약 1건 처리. task_done()은 finally에서 정확히 1회 호출."""
    log.info(f"▶ #{rid}  biz={biz_id}  action={action}")

    with queued_lock:
        queued_set.discard(rid)

    try:

        # 변경 액션: 구예약 naver_changed 처리
        if action == "change" and old_rid:
            try:
                old_r = requests.get(
                    f"{SUPABASE_URL}/rest/v1/reservations?reservation_id=eq.{old_rid}&select=id,status",
                    headers=HEADERS, timeout=10
                )
                old_rows = old_r.json()
                if old_rows:
                    old_status = old_rows[0].get("status", "")
                    if old_status != "naver_changed":
                        requests.patch(
                            f"{SUPABASE_URL}/rest/v1/reservations?reservation_id=eq.{old_rid}",
                            headers={**HEADERS, "Prefer": "return=minimal"},
                            json={"status": "naver_changed"},
                            timeout=10
                        )
                        log.info(f"  🔄 변경: 구예약 #{old_rid} → naver_changed")
                    else:
                        log.info(f"  ⏭  구예약 #{old_rid} 이미 naver_changed")
                else:
                    log.info(f"  ℹ️  구예약 #{old_rid} DB에 없음 (무시)")
            except Exception as e:
                log.error(f"  구예약 처리 오류: {e}")

        # 취소 액션: DB에 이미 있으면 바로 취소 처리 후 스크래핑 스킵
        if action == "cancel":
            try:
                existing_r = requests.get(
                    f"{SUPABASE_URL}/rest/v1/reservations?reservation_id=eq.{rid}&select=id,status",
                    headers=HEADERS, timeout=10
                )
                existing = existing_r.json()
                if existing:
                    db_cancel(rid)
                    return  # finally → task_done() 후 루프 continue
                # 없으면 아래 스크래핑으로 진행해서 저장 후 취소 상태로
            except Exception as e:
                log.error(f"  취소 확인 오류: {e}")
                return  # finally → task_done() 후 루프 continue

        # ── 스크래핑 ──
        raw = scrape_reservation(biz_id, rid)

        if not raw:
            fail_counts[rid] = fail_counts.get(rid, 0) + 1
            save_fail_counts()
            if fail_counts[rid] >= 3:
                log.warning(f"  #{rid} 3회 실패 → 영구 스킵")
            else:
                log.warning(f"  #{rid} 스크래핑 실패 ({fail_counts[rid]}/3)")
            return  # finally → task_done()

        # 성별 추론
        svc_str = " ".join(raw.get("services", []))
        gender = "M" if "남)" in svc_str else ("F" if "여)" in svc_str else "")

        # 지점 ID
        bid = ""
        for nbid, br in _branches.items():
            if str(nbid) == str(biz_id):
                bid = br.get("id", "")
                break
        if not bid:
            try:
                rb = requests.get(
                    f"{SUPABASE_URL}/rest/v1/branches?naver_biz_id=eq.{biz_id}&select=id",
                    headers=HEADERS, timeout=5
                )
                rows = rb.json()
                if rows:
                    bid = rows[0]["id"]
                    _branches[biz_id] = rows[0]
            except:
                pass

        # 상태 결정
        if action == "cancel":
            status = "naver_cancelled"
        else:
            status = raw.get("status", "confirmed")

        # 고객 전화번호로 기존 고객 조회
        phone = raw.get("phone", "")
        visit_count = raw.get("visit_count", 0)
        matched_cust = find_cust_by_phone(phone, BUSINESS_ID) if phone else None
        if matched_cust:
            matched_cust_id = matched_cust["id"]
            is_new = False
            log.info(f"  고객 매칭: {matched_cust['name']} ({phone}) → cust_id={matched_cust_id}")
        else:
            matched_cust_id = None
            is_new = visit_count == 0

        db_data = {
            "bid":                bid,
            "cust_id":            matched_cust_id or "",
            "cust_name":          raw.get("name", ""),
            "cust_phone":         raw.get("phone", ""),
            "cust_email":         raw.get("email", ""),
            "cust_gender":        gender,
            "date":               raw.get("date", ""),
            "time":               raw.get("time", ""),
            "dur":                45,
            "status":             status,
            "selected_services":  [],
            "is_new_cust":        is_new,
            "request_msg":        _build_request_msg(raw),
            "owner_comment":      raw.get("owner_comment", ""),
            "is_prepaid":         raw.get("is_prepaid", False),
            "npay_method":        raw.get("npay_method", ""),
            "total_price":        raw.get("total_price", 0),
            "visit_count":        raw.get("visit_count", 0),
            "no_show_count":      raw.get("no_show_count", 0),
            "naver_reg_dt":       raw.get("reg_datetime", ""),
            "naver_confirmed_dt": raw.get("confirmed_datetime", ""),
            "naver_cancelled_dt": raw.get("cancelled_datetime", ""),
            "is_scraping_done":   True,
        }

        db_upsert(rid, db_data)

        ai_analyze_reservation(
            rid=rid,
            request_msg=db_data.get("request_msg", ""),
            owner_comment=db_data.get("owner_comment", ""),
            cust_name=db_data.get("cust_name", ""),
            visit_count=db_data.get("visit_count", 0),
            is_prepaid=db_data.get("is_prepaid", False),
            is_new_cust=db_data.get("is_new_cust", False),
        )

    except Exception as e:
        log.error(f"  #{rid} 처리 오류: {e}", exc_info=True)
    finally:
        task_queue.task_done()  # get() 이후 무조건 1회 호출


def scraper_thread():
    log.info("스크래퍼 스레드 시작")
    while True:
        try:
            rid, biz_id, action, old_rid = task_queue.get(timeout=5)
        except Empty:
            continue
        _process_one(rid, biz_id, action, old_rid)


# ─── AI 자동 분석 ─────────────────────────────────────────────────────────────
_ai_settings_cache = {"key": None, "tags": None, "services": None, "loaded_at": 0}

def _load_ai_settings():
    """Supabase businesses.memo에서 Gemini key 및 태그/서비스 목록 로드 (5분 캐시)"""
    import time
    now = time.time()
    if _ai_settings_cache["key"] and now - _ai_settings_cache["loaded_at"] < 300:
        return _ai_settings_cache

    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/businesses?id=eq.{BUSINESS_ID}&select=memo",
            headers=HEADERS, timeout=10
        )
        rows = r.json()
        memo = {}
        try: memo = json.loads((rows[0].get("memo") or "{}")) if rows else {}
        except: pass
        gemini_key = memo.get("gemini_key", "")
        ai_rules   = memo.get("ai_rules") or []  # 앱에서 등록한 커스텀 규칙
    except Exception as e:
        log.warning(f"AI settings 로드 실패: {e}")
        return _ai_settings_cache

    if not gemini_key:
        return _ai_settings_cache

    # 태그/서비스 목록 로드
    try:
        tags_r = requests.get(
            f"{SUPABASE_URL}/rest/v1/service_tags?business_id=eq.{BUSINESS_ID}&use_yn=eq.true&schedule_yn=eq.false&select=id,name",
            headers=HEADERS, timeout=10
        )
        svcs_r = requests.get(
            f"{SUPABASE_URL}/rest/v1/services?business_id=eq.{BUSINESS_ID}&select=id,name,dur",
            headers=HEADERS, timeout=10
        )
        tags = tags_r.json()
        svcs = svcs_r.json()
    except Exception as e:
        log.warning(f"태그/서비스 로드 실패: {e}")
        tags, svcs = [], []

    _ai_settings_cache.update({
        "key":      gemini_key,
        "tags":     tags,
        "services": svcs,
        "ai_rules": ai_rules,
        "loaded_at": now,
    })
    return _ai_settings_cache


def ai_analyze_reservation(rid: str, request_msg: str, owner_comment: str,
                           cust_name: str = "", visit_count: int = 0,
                           is_prepaid: bool = False, is_new_cust: bool = False):
    """스크래핑 완료 후 Gemini로 태그/서비스 자동 분석 → selected_tags 업데이트"""

    # 시스템 태그 ID (절대 AI가 제거하지 않음)
    NEW_CUST_TAG_ID   = "lggzktc9f"  # 신규
    PREPAID_TAG_ID    = "27v4vg5uh"  # 예약금완료
    SYSTEM_TAG_IDS    = {NEW_CUST_TAG_ID, PREPAID_TAG_ID}

    settings = _load_ai_settings()
    api_key = settings.get("key")
    if not api_key:
        return  # Gemini key 미설정 → 스킵

    # ── 현재 DB 상태 가져오기 (기존 태그/서비스 보존용) ──────────────────────
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/reservations"
            f"?reservation_id=eq.{rid}"
            f"&select=selected_tags,selected_services",
            headers=HEADERS, timeout=10
        )
        existing = r.json()
        existing_tags = list(existing[0].get("selected_tags") or []) if existing else []
        existing_svcs = list(existing[0].get("selected_services") or []) if existing else []
    except Exception as e:
        log.warning(f"  #{rid} 기존 태그 조회 실패: {e}")
        existing_tags, existing_svcs = [], []

    # JSON 배열 형식이면 텍스트로 변환
    if request_msg and request_msg.strip().startswith("["):
        try:
            import json as _json
            items = _json.loads(request_msg)
            request_msg = "\n".join(f"{it['label']}: {it['value']}" for it in items if it.get('value'))
        except Exception:
            pass

    naver_text = "\n".join(filter(None, [request_msg, owner_comment])).strip()

    # ── AI 분석 (텍스트 있을 때만) ──────────────────────────────────────────
    ai_tag_ids = []
    ai_svc_ids = []
    ai_gender  = ""

    if naver_text and naver_text != "-대화없음-":
        tags = settings.get("tags") or []
        svcs = settings.get("services") or []

        tag_list = ", ".join(f'"{t["id"]}":"{t["name"]}"' for t in tags)
        svc_list = ", ".join(f'"{s["id"]}":"{s["name"]}"' for s in svcs)

        # 커스텀 규칙 블록 (앱과 동일한 형식)
        ai_rules = settings.get("ai_rules") or []
        custom_rules_block = ""
        if ai_rules:
            rules_str = "\n".join(f"{i+1}. {r}" for i, r in enumerate(ai_rules))
            custom_rules_block = f"\n[추가 판단 규칙 - 아래 규칙을 기본 기준보다 우선 적용하세요]\n{rules_str}"

        prompt = (
            f"당신은 왁싱샵/미용실 예약 정보를 분석하는 AI입니다.\n"
            f"아래 네이버 예약 고객 정보를 분석하여 적합한 태그와 시술상품을 선택하세요.\n"
            f"마크다운 없이 순수 JSON만 출력하세요.\n\n"
            f"[태그 목록] {tag_list}\n"
            f"[시술상품 목록] {svc_list}\n\n"
            f"[기본 판단 기준]\n"
            f"- 주차 언급 → \"주차\" 태그\n"
            f"- 임산부/산모 → \"산모님\" 태그\n"
            f"- 커플룸 요청 → \"커플룸\" 태그\n"
            f"- 남자 관리사 요청 → \"남자선생님\" 태그\n"
            f"- 시술메뉴 내용으로 적합한 시술상품 선택\n"
            f"- \"신규\" 태그, \"예약금완료\" 태그는 선택하지 마세요. 이 태그들은 시스템이 자동 처리합니다."
            f"{custom_rules_block}\n\n"
            f"[예약 기본 정보]\n"
            f"- 고객명: {cust_name or '미상'}\n"
            f"- 방문횟수: {visit_count}회\n\n"
            f"[고객 요청 / 업체 메모]\n"
            f"{naver_text}\n\n"
            f"응답 형식:\n"
            f'{{"matchedTagIds":["태그id1"],"matchedServiceIds":["시술id1"],"gender":"F 또는 M 또는 빈문자열","reason":"선택 이유"}}'
        )

        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}",
                json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0}},
                timeout=30
            )
            if resp.ok:
                text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                text = text.replace("```json", "").replace("```", "").strip()
                result = json.loads(text)
                ai_tag_ids = result.get("matchedTagIds") or []
                ai_svc_ids = result.get("matchedServiceIds") or []
                ai_gender  = result.get("gender", "").strip().upper()
                if ai_gender not in ("F", "M"):
                    ai_gender = ""
                log.info(f"  🤖 Gemini 응답 ok: tags={ai_tag_ids} svcs={ai_svc_ids} gender={ai_gender}")
            else:
                err_msg = f"Gemini API 오류 #{rid}: {resp.status_code} {resp.text[:200]}"
                log.warning(err_msg)
                _report_heartbeat(last_error=err_msg)  # server_logs에 기록
        except Exception as e:
            err_msg = f"AI 분석 실패 #{rid}: {e}"
            log.warning(err_msg)
            _report_heartbeat(last_error=err_msg)

    # ── 최종 태그 계산 ────────────────────────────────────────────────────────
    # 기존 시스템 태그 보존 + AI 태그 추가 (중복 제거)
    system_tags = [t for t in existing_tags if t in SYSTEM_TAG_IDS]
    merged_tags = list(dict.fromkeys(system_tags + ai_tag_ids))  # 순서 유지, 중복 제거

    # is_new_cust면 신규 태그 자동 추가
    if is_new_cust and NEW_CUST_TAG_ID not in merged_tags:
        merged_tags.append(NEW_CUST_TAG_ID)

    # is_prepaid면 예약금완료 태그 자동 추가
    if is_prepaid and PREPAID_TAG_ID not in merged_tags:
        merged_tags.append(PREPAID_TAG_ID)

    # ── 최종 서비스 계산 ──────────────────────────────────────────────────────
    # AI 결과를 항상 사용 (match_services 가공 제거됨)
    final_svcs = ai_svc_ids

    # ── DB 업데이트 ──────────────────────────────────────────────────────────
    update = {}
    # 태그: 시스템태그 + AI태그 + is_prepaid 반영
    update["selected_tags"] = merged_tags
    # 서비스: AI 결과 저장
    update["selected_services"] = final_svcs
    # 성별: AI가 판단했을 때만 덮어씀
    if ai_gender:
        update["cust_gender"] = ai_gender
    # dur: AI 매칭된 서비스 시간 합산
    if final_svcs:
        try:
            svcs_all = settings.get("services") or []
            svc_map = {s["id"]: s for s in svcs_all}
            total_dur = sum(svc_map[sid].get("dur", 0) for sid in final_svcs if sid in svc_map)
            if total_dur > 0:
                update["dur"] = total_dur
        except Exception:
            pass

    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/reservations?reservation_id=eq.{rid}",
            headers=HEADERS, json=update, timeout=10
        )
        log.info(f"  🤖 #{rid} AI 분석 완료: tags={merged_tags} svcs={final_svcs} prepaid={is_prepaid}")
    except Exception as e:
        log.warning(f"  #{rid} AI 결과 저장 실패: {e}")


# ─── 미스크래핑 예약 폴링 ─────────────────────────────────────────────────────

# bid → naver_biz_id 캐시 (최초 1회 load_cache에서 채움)
_bid_to_biz: dict = {}

def _build_bid_to_biz():
    """branches 테이블에서 bid→naver_biz_id 매핑 생성"""
    global _bid_to_biz
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/branches?select=id,naver_biz_id",
            headers=HEADERS, timeout=10
        )
        if r.ok:
            _bid_to_biz = {b["id"]: b["naver_biz_id"] for b in r.json() if b.get("naver_biz_id")}
            log.info(f"bid→biz_id 매핑 로드: {len(_bid_to_biz)}개")
    except Exception as e:
        log.warning(f"bid→biz_id 로드 실패: {e}")

def poll_unscraped():
    """is_scraping_done=False 이거나 cust_name이 비어있는 naver 예약을 재스크래핑"""
    if not _bid_to_biz:
        _build_bid_to_biz()
    try:
        # is_scraping_done=False 인 것
        r1 = requests.get(
            f"{SUPABASE_URL}/rest/v1/reservations"
            f"?source=eq.naver&is_scraping_done=eq.false"
            f"&select=id,reservation_id,bid&limit=50",
            headers=HEADERS, timeout=10
        )
        rows1 = r1.json() if r1.ok else []
        # cust_name이 비어있는 것 (is_scraping_done=True라도)
        r2 = requests.get(
            f"{SUPABASE_URL}/rest/v1/reservations"
            f"?source=eq.naver&cust_name=eq.&is_scraping_done=eq.true"
            f"&select=id,reservation_id,bid&limit=50",
            headers=HEADERS, timeout=10
        )
        rows2 = r2.json() if r2.ok else []
        # request_msg 있는데 selected_tags 비어있는 것 (AI 분석 안 된 것)
        r3 = requests.get(
            f"{SUPABASE_URL}/rest/v1/reservations"
            f"?source=eq.naver&is_scraping_done=eq.true"
            f"&selected_tags=eq.%5B%5D&not.request_msg=eq."
            f"&select=id,reservation_id,bid&limit=50",
            headers=HEADERS, timeout=10
        )
        rows3 = r3.json() if r3.ok else []
        # 중복 제거 후 합치기
        seen = set()
        rows = []
        for row in rows1 + rows2 + rows3:
            if row["id"] not in seen:
                seen.add(row["id"])
                rows.append(row)
    except Exception as e:
        log.warning(f"미스크래핑 폴링 실패: {e}")
        return

    if not rows:
        return

    log.info(f"미스크래핑 예약 {len(rows)}건 발견 → 큐 추가")
    for row in rows:
        rid = str(row.get("reservation_id", ""))
        bid = row.get("bid", "")
        biz_id = _bid_to_biz.get(bid, "")
        if not rid or not biz_id:
            log.warning(f"  #{row['id']} rid={rid} biz_id={biz_id} → 스킵 (정보 부족)")
            continue
        with queued_lock:
            if rid in queued_set:
                continue
            if fail_counts.get(rid, 0) >= 3:
                log.warning(f"  #{rid} 영구 스킵 (실패 {fail_counts[rid]}회)")
                continue
            queued_set.add(rid)
            task_queue.put((rid, biz_id, "scrape", None))
        log.info(f"  → 큐 추가: #{rid} biz={biz_id}")


# ─── Gmail IMAP 스레드 ────────────────────────────────────────────────────────


def gmail_thread():
    log.info("Gmail 스레드 시작")

    while True:
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            mail.select("INBOX")
            log.info("Gmail 연결됨")

            # 초기 전체 스캔 (SINCE_DATE 이후 미처리 메일)
            criteria = f'(FROM "navercorp" SINCE {SINCE_DATE})'
            if BEFORE_DATE:
                criteria = f'(FROM "navercorp" SINCE {SINCE_DATE} BEFORE {BEFORE_DATE})'
            s, ids = mail.uid("search", None, criteria)
            if s == "OK" and ids[0]:
                uid_list = ids[0].split()
                log.info(f"초기 스캔: {len(uid_list)}개 메일 (미처리: {sum(1 for u in uid_list if not is_processed(u.decode()))}건)")
                for uid in uid_list:
                    uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
                    if not is_processed(uid_str):
                        _proc_uid(mail, uid, uid_str)

            log.info("IMAP 폴링 대기 중...")

            while True:
                try:
                    mail.noop()
                except:
                    break

                # UNSEEN 메일 체크
                s, ids = mail.uid("search", None, '(FROM "navercorp" UNSEEN)')
                if s == "OK" and ids[0]:
                    for uid in ids[0].split():
                        uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
                        if not is_processed(uid_str):
                            _proc_uid(mail, uid, uid_str)

                time.sleep(5)

        except Exception as e:
            log.error(f"Gmail 오류: {e}")
            time.sleep(10)
        finally:
            try: mail.logout()
            except: pass


def _proc_uid(mail, uid, uid_str: str):
    s, d = mail.uid("fetch", uid, "(RFC822)")
    if s != "OK": return

    msg = email.message_from_bytes(d[0][1])
    fa  = msg.get("From", "")
    if "navercorp" not in fa and "naver" not in fa.lower():
        return

    subj = _dmh(msg["Subject"])
    body = _get_body(msg)
    log.info(f"메일 수신: {subj}")

    rid, biz_id, action, old_rid = parse_email(subj, body)

    if not rid:
        log.warning(f"  예약번호 파싱 실패 → 미처리 (재시작 시 재시도): {subj}")
        return  # processed에 추가 안 함 → 재시도 가능

    if not biz_id:
        log.warning(f"  지점 파싱 실패 → 미처리 (재시작 시 재시도): {subj}")
        return  # processed에 추가 안 함 → 재시도 가능

    # 파싱 성공 시에만 processed 저장
    save_processed(uid_str)

    if TEST_BIZ_FILTER and biz_id != TEST_BIZ_FILTER:
        return

    with queued_lock:
        if rid in queued_set:
            log.info(f"  #{rid} 이미 큐에 있음 → 스킵")
            return
        if fail_counts.get(rid, 0) >= 3:
            log.warning(f"  #{rid} 영구 스킵 (실패 {fail_counts[rid]}회)")
            return
        queued_set.add(rid)
        task_queue.put((rid, biz_id, action, old_rid))

    if old_rid:
        log.info(f"  → 큐 추가: #{rid}  biz={biz_id}  action={action}  구예약=#{old_rid}")
    else:
        log.info(f"  → 큐 추가: #{rid}  biz={biz_id}  action={action}")


# ─── 메인 ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 50)
    log.info("Bliss × 네이버 예약 동기화 v3 시작")
    log.info("=" * 50)

    load_cache()
    load_fail_counts()
    load_processed()

    t1 = threading.Thread(target=scraper_thread, daemon=True, name="scraper")
    t1.start()
    time.sleep(2)

    t2 = threading.Thread(target=gmail_thread, daemon=True, name="gmail")
    t2.start()


    while True:
        time.sleep(20)

        # ── 미스크래핑 예약 폴링 (1분마다) ──
        if int(time.time()) % 60 < 20:
            poll_unscraped()

        # ── Watchdog: 죽은 스레드 자동 재시작 ──
        if not t1.is_alive():
            log.error("⚠️  스크래퍼 스레드 DEAD → 재시작")
            t1 = threading.Thread(target=scraper_thread, daemon=True, name="scraper")
            t1.start()

        if not t2.is_alive():
            log.error("⚠️  Gmail 스레드 DEAD → 재시작")
            t2 = threading.Thread(target=gmail_thread, daemon=True, name="gmail")
            t2.start()

        scraper_st = 'alive' if t1.is_alive() else 'DEAD'
        gmail_st   = 'alive' if t2.is_alive() else 'DEAD'
        q_size     = task_queue.qsize()

        log.info(f"상태: 큐 대기 {q_size}건 | scraper={scraper_st} | gmail={gmail_st}")

        # Supabase server_logs 업데이트 (Claude 모니터링용)
        try:
            import socket, datetime
            hostname = socket.gethostname()
            try:
                local_ip = socket.gethostbyname(socket.getfqdn())
            except Exception:
                local_ip = "unknown"
            # 서버 구분: Oracle=10.0.0.x, naver-sync=그 외
            if local_ip.startswith("10.0.0."):
                server_label = "oracle(158.179.174.30)"
            else:
                server_label = "naver-sync(27.1.36.102)"
            server_id = f"bliss-naver-{server_label}"
            requests.post(
                f"{SUPABASE_URL}/rest/v1/server_logs",
                headers={**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
                json={
                    "id": server_id,
                    "server": server_label,
                    "scraper_status": scraper_st,
                    "gmail_status": gmail_st,
                    "queue_size": q_size,
                    "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
                },
                timeout=5
            )
        except Exception as _e:
            log.debug(f"server_logs 업데이트 실패: {_e}")


