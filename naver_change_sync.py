#!/usr/bin/env python3
"""
Bliss × 네이버 Gmail IMAP IDLE 동기화
────────────────────────────────────
원칙:
  - 이름/연락처는 scraper(naver_sync.py)만 채움
  - 이메일은 상태/날짜/시간 등 메타정보만 처리
  - 신규/변경 수신 시 scrape_queue.json에 즉시 추가
  - 이름/연락처가 이미 있는 경우 절대 덮어쓰지 않음
"""
import imaplib, email, re, requests, json, logging, os, time, random, string
from email.header import decode_header
from datetime import datetime, timedelta, timezone

GMAIL_USER = "housewaxing@gmail.com"
GMAIL_APP_PASSWORD = "swqb mqhr qznp ljjd"
SUPABASE_URL = "https://dpftlrsuqxqqeouwbfjd.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRwZnRscnN1cXhxcWVvdXdiZmpkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE5MDU4MjQsImV4cCI6MjA4NzQ4MTgyNH0.iydEkjtPjZ0jXpUUPJben4IWWneDqLomv-HDlcFayE4"
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "return=minimal"}
PROCESSED_FILE = "processed_gmail_uids.json"
GMAIL_FOLDER = '"[Gmail]/&yATMtLz0rQDVaA-"'
BUSINESS_ID = "biz_khvurgshb"
SCRAPE_QUEUE_FILE = "/home/ubuntu/naver-sync/scrape_queue.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("naver_change_sync.log", encoding="utf-8")])
log = logging.getLogger(__name__)

# ─── 공통 유틸 ───
_branches = None
def get_branches():
    global _branches
    if _branches is None:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/branches?select=*", headers=HEADERS)
        r.raise_for_status()
        _branches = {b["naver_biz_id"]: b for b in r.json() if b.get("naver_biz_id")}
        log.info(f"branches loaded: {len(_branches)}")
    return _branches

def gen_id(n=9):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

def kst_now():
    return datetime.now(timezone(timedelta(hours=9))).strftime("%m.%d %H:%M")

def db_get(rid):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/reservations?select=*&reservation_id=eq.{rid}", headers=HEADERS)
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else None

def db_patch(row_id, data):
    requests.patch(f"{SUPABASE_URL}/rest/v1/reservations?id=eq.{row_id}", headers=HEADERS, json=data).raise_for_status()

def load_processed():
    try:
        with open(PROCESSED_FILE) as f: return set(json.load(f))
    except: return set()

def save_processed(p):
    with open(PROCESSED_FILE, "w") as f: json.dump(list(p), f)

def dmh(s):
    parts = decode_header(s or "")
    out = []
    for b, enc in parts:
        if isinstance(b, bytes): out.append(b.decode(enc or "utf-8", errors="replace"))
        else: out.append(b)
    return "".join(out)

def get_body(msg):
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct in ("text/plain", "text/html"):
                pl = part.get_payload(decode=True)
                if pl: body += pl.decode(part.get_content_charset() or "utf-8", errors="replace")
    else:
        pl = msg.get_payload(decode=True)
        if pl: body = pl.decode(msg.get_content_charset() or "utf-8", errors="replace")
    return body

def clean_html(body):
    c = re.sub(r'<[^>]+>', ' ', body)
    return re.sub(r'\s+', ' ', c)

# ─── 큐 관리 ───
def queue_add(rid, biz_id=None):
    """scrape_queue.json에 rid 추가 (naver_sync.py가 즉시 처리)"""
    try:
        try:
            with open(SCRAPE_QUEUE_FILE) as f: queue = json.load(f)
        except: queue = []
        # 중복 제거
        queue = [q for q in queue if q.get("rid") != rid]
        queue.append({
            "rid": rid,
            "biz_id": biz_id,
            "added_at": datetime.now(timezone(timedelta(hours=9))).isoformat()
        })
        with open(SCRAPE_QUEUE_FILE, "w") as f: json.dump(queue, f, ensure_ascii=False)
        log.info(f"  → scrape_queue 추가: #{rid}")
    except Exception as e:
        log.warning(f"  scrape_queue 추가 실패: {e}")

def get_biz_id_from_branch(branch):
    """branch dict에서 naver_biz_id 반환"""
    if not branch: return None
    branches = get_branches()
    for biz_id, br in branches.items():
        if br.get("id") == branch.get("id"):
            return biz_id
    return None

# ─── 파싱 함수 ───
def parse_change(body):
    c = clean_html(body)
    nr, oldr = None, None
    m = re.search(r'신규예약내역.*?예약\s*번\s*호\s*[:\s]*(\d{7,12})', c)
    if m: nr = m.group(1)
    m2 = re.search(r'예약취소내역.*?예약\s*번\s*호\s*[:\s]*(\d{7,12})', c)
    if m2: oldr = m2.group(1)
    if not nr or not oldr:
        a = re.findall(r'예약\s*번\s*호\s*[:\s]*(\d{7,12})', c)
        if len(a) >= 2: nr = nr or a[0]; oldr = oldr or a[1]
    cm = re.search(r'예약자명\s*[:\s]*(\S+)', c)
    cust_name = cm.group(1).rstrip("님") if cm else None
    return {"new_rid": nr, "old_rid": oldr, "cust_name": cust_name}

def parse_confirm(body):
    c = clean_html(body)
    m = re.search(r'예약\s*번\s*호\s*[:\s]*(\d{7,12})', c)
    return {"rid": m.group(1) if m else None}

def parse_new_reservation(body):
    c = clean_html(body)
    result = {}
    m = re.search(r'예약\s*번\s*호\s*[:\s]*(\d{7,12})', c)
    result["reservation_id"] = m.group(1) if m else None
    # 이름/연락처: 있으면 저장, 마스킹이면 None (scraper가 채움)
    m = re.search(r'예약자명\s*[:\s]*(\S+)', c)
    name = m.group(1).rstrip("님") if m else None
    result["cust_name"] = name if name and "*" not in name else None
    m = re.search(r'(?:연락처|전화번호|핸드폰)\s*[:\s]*([\d\-]+)', c)
    if m:
        phone = re.sub(r'-', '', m.group(1))
        result["cust_phone"] = phone if len(phone) >= 10 and "*" not in phone else None
    else:
        result["cust_phone"] = None
    # 날짜/시간
    m = re.search(r'이용일시\s*(\d{4})[.\-/]\s*(\d{1,2})[.\-/]\s*(\d{1,2})', c)
    result["date"] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else None
    m = re.search(r'이용일시\s*.+?(오전|오후)?\s*(\d{1,2})\s*:\s*(\d{2})', c)
    if m:
        h = int(m.group(2)); mi = m.group(3); ampm = m.group(1)
        if ampm == '오후' and h < 12: h += 12
        elif ampm == '오전' and h == 12: h = 0
        result["time"] = f"{h:02d}:{mi}"
    else:
        result["time"] = None
    # 선택메뉴
    sm = re.search(r'선택메뉴\s*(.+?)(?:\s*매장방문|\s*요청사항|\s*결제상태|\s*$)', c)
    if sm:
        items = re.findall(r'([\uAC00-\uD7A3][\uAC00-\uD7A3\s]*?)\s*[\d,]+\s*원', sm.group(1))
        result["selected_menu"] = [i.strip() for i in items if i.strip()]
    else:
        result["selected_menu"] = []
    # 지점명
    m = re.search(r'(?:사업장|업체명|매장[명]?)\s*[:\s]*(.+?)(?:\s*예약|\s*시술|\s*$)', c)
    result["biz_name"] = m.group(1).strip() if m else None
    if not result["biz_name"]:
        m = re.search(r'하우스왁싱\s+(\S+)', c)
        result["biz_name"] = m.group(1).strip() if m else None
    return result

# ─── DB 처리 함수 ───
def find_branch(biz_name):
    if not biz_name: return None
    branches = get_branches()
    for biz_id, br in branches.items():
        short = br.get("short", "")
        if short and short == biz_name: return br
    for biz_id, br in branches.items():
        short = br.get("short", "")
        if short and (short in biz_name or biz_name in short): return br
    for biz_id, br in branches.items():
        if biz_name in br.get("name","") or br.get("name","") in biz_name: return br
    return None

def upsert_reservation(parsed, status="pending", memo_suffix=None):
    """
    예약번호 기준으로 upsert:
    - 없으면 새로 생성
    - 있으면 상태/날짜/시간만 업데이트 (이름/연락처 절대 건드리지 않음)
    """
    rid = parsed.get("reservation_id")
    if not rid: return None

    existing = db_get(rid)
    kst = kst_now()

    if existing:
        # 이름/연락처 건드리지 않음. 상태/날짜/시간만 업데이트
        update = {}
        if parsed.get("date") and not existing.get("date"): update["date"] = parsed["date"]
        if parsed.get("time") and not existing.get("time"): update["time"] = parsed["time"]
        if status and existing.get("status") != status: update["status"] = status
        if update:
            db_patch(existing["id"], update)
            log.info(f"  updated: #{rid} {update}")
        else:
            log.info(f"  no change: #{rid}")
        return existing["id"]
    else:
        # 신규 생성 - 이름/연락처는 있으면 저장 (마스킹이면 None)
        biz_name = parsed.get("biz_name") or ""
        branch = find_branch(biz_name)
        row = {
            "id": gen_id(),
            "business_id": BUSINESS_ID,
            "bid": branch["id"] if branch else "",
            "room_id": "", "cust_id": "",
            "cust_name": parsed.get("cust_name") or "",
            "cust_phone": parsed.get("cust_phone") or "",
            "cust_gender": "", "staff_id": "", "service_id": "",
            "date": parsed.get("date") or "",
            "time": parsed.get("time") or "",
            "dur": 0, "status": status, "memo": "",
            "type": "reservation", "is_schedule": False, "is_new_cust": True,
            "selected_tags": [], "selected_services": [],
            "repeat": "none", "repeat_until": "", "repeat_group_id": "",
            "reservation_id": rid, "source": "naver",
            "is_scraping_done": False  # 반드시 False → bliss_naver.py 스크래퍼가 채움
        }
        # 선택메뉴 서비스 매칭
        selected_menus = parsed.get("selected_menu") or []
        if selected_menus:
            svc_r = requests.get(f"{SUPABASE_URL}/rest/v1/services?select=id,name,dur&business_id=eq.{BUSINESS_ID}", headers=HEADERS)
            svc_list = svc_r.json() if svc_r.ok else []
            SYNONYMS = {"음모왁싱": "브라질리언", "브라질리언왁싱": "브라질리언"}
            matched_ids, total_dur = [], 0
            for menu in selected_menus:
                mc = SYNONYMS.get(menu.strip(), menu.strip())
                for svc in svc_list:
                    if svc["name"] in mc or mc in svc["name"]:
                        matched_ids.append(svc["id"]); total_dur += svc.get("dur") or 0; break
            if matched_ids:
                # selected_services는 bliss_naver.py AI 분석이 담당 → 여기서 세팅 안 함
                row["dur"] = total_dur  # 시간만 계산
                log.info(f"  시간 계산: {selected_menus} -> dur={total_dur}분 (서비스는 AI 담당)")
        requests.post(f"{SUPABASE_URL}/rest/v1/reservations", headers=HEADERS, json=row).raise_for_status()
        br_name = branch["short"] if branch else "?"
        log.info(f"  created: #{rid} [{br_name}] {parsed.get('date','')} {parsed.get('time','')} name={row['cust_name'] or '(미정)'}")
        return row["id"]

def handle_change(r, subj):
    """변경 이메일: 구 예약 → 새 rid로 이관, 이름/연락처 보존"""
    old_rid, new_rid = r["old_rid"], r["new_rid"]
    log.info(f"  #{old_rid} → #{new_rid}")

    # 구 예약 조회
    old_row = db_get(old_rid)

    # 새 rid로 이미 존재하는지 확인
    new_row = db_get(new_rid)

    kst = kst_now()

    if new_row:
        # 변경 이메일 = 네이버 측 확정 의미 → confirmed로 처리
        # (변경 후 별도 확정 이메일이 오지 않으므로)
        prev_status = new_row.get("status","")
        new_status = "confirmed" if prev_status not in ("cancelled","no_show") else prev_status
        db_patch(new_row["id"], {
            "status": new_status,
            "naver_confirmed_dt": datetime.now(timezone(timedelta(hours=9))).isoformat(),
        })
        log.info(f"  #{new_rid} 이미 존재 → status={new_status} 으로 변경")
    elif old_row:
        # 구 예약 정보를 새 rid로 복사 (이름/연락처 보존)
        import uuid
        new_rec = dict(old_row)
        new_rec["reservation_id"] = new_rid
        new_rec["id"] = "nv_" + uuid.uuid4().hex[:12]
        new_rec["is_scraping_done"] = False  # bliss_naver.py 스크래퍼가 채움
        new_rec["status"] = "confirmed"  # 변경 이메일 = 확정
        new_rec["naver_confirmed_dt"] = datetime.now(timezone(timedelta(hours=9))).isoformat()
        requests.post(f"{SUPABASE_URL}/rest/v1/reservations", headers=HEADERS, json=new_rec).raise_for_status()
        log.info(f"  created #{new_rid} from #{old_rid} (이름={new_rec.get('cust_name') or '미정'}) status=confirmed")
    else:
        # 구 예약도 없으면 이메일에서 파싱 가능한 정보로 새로 생성
        log.warning(f"  구 예약 #{old_rid} DB 없음 → 새로 생성 시도")
        parsed = parse_new_reservation(subj + " " + r.get("body", ""))
        parsed["reservation_id"] = new_rid
        upsert_reservation(parsed, status="confirmed")

    # 구 예약 삭제
    if old_row:
        requests.delete(f"{SUPABASE_URL}/rest/v1/reservations?id=eq.{old_row['id']}", headers=HEADERS).raise_for_status()
        log.info(f"  deleted old: #{old_rid}")

    # 이름/연락처가 없으면 scrape_queue 추가
    final = db_get(new_rid)
    if final and (not final.get("cust_name") or not final.get("cust_phone")):
        # biz_id 찾기
        branches = get_branches()
        biz_id = None
        for bid, br in branches.items():
            if br.get("id") == final.get("bid"):
                biz_id = bid; break
        queue_add(new_rid, biz_id)

def handle_confirm(rid):
    """확정 이메일: 상태만 confirmed로 변경. 이름/연락처 절대 건드리지 않음"""
    row = db_get(rid)
    if not row:
        log.warning(f"  DB #{rid} 없음")
        return
    if row["status"] == "confirmed":
        log.info(f"  #{rid} 이미 confirmed")
        return
    db_patch(row["id"], {"status": "confirmed"})
    log.info(f"  confirmed: #{rid}")

def handle_cancel(rid):
    """취소 이메일: 상태만 cancelled로 변경. 이름/연락처 절대 건드리지 않음"""
    row = db_get(rid)
    if not row:
        log.warning(f"  DB #{rid} 없음")
        return
    if row["status"] == "cancelled":
        log.info(f"  #{rid} 이미 cancelled")
        return
    db_patch(row["id"], {
        "status": "cancelled",
    })
    log.info(f"  cancelled: #{rid}")

def handle_new(parsed, subj):
    """신규 접수: DB 저장 후 즉시 scrape_queue 추가"""
    # 지점명 제목에서 추출 (더 신뢰성 높음)
    sm = re.search(r'하우스왁싱\s+(\S+)', subj)
    if sm: parsed["biz_name"] = sm.group(1).strip()

    rid = parsed.get("reservation_id")
    upsert_reservation(parsed, status="pending")

    # 이름/연락처 불완전하면 scrape_queue 추가
    if rid:
        final = db_get(rid)
        if final and (not final.get("cust_name") or not final.get("cust_phone")):
            branches = get_branches()
            biz_id = None
            biz_name = parsed.get("biz_name") or ""
            branch = find_branch(biz_name)
            if branch:
                for bid, br in branches.items():
                    if br.get("id") == branch.get("id"):
                        biz_id = bid; break
            queue_add(rid, biz_id)
        else:
            log.info(f"  #{rid} 이름/연락처 완전 → scrape 불필요")

# ─── 메일 처리 ───
def proc_msg(mail, uid):
    s, d = mail.uid("fetch", uid, "(RFC822)")
    if s != "OK": return
    msg = email.message_from_bytes(d[0][1])
    fa = msg.get("From", "")
    if "navercorp" not in fa and "naver" not in fa.lower(): return
    subj = dmh(msg["Subject"])
    body = get_body(msg)

    if "변경" in subj:
        log.info(f"[change] {subj}")
        r = parse_change(body)
        r["body"] = body
        if r["old_rid"] and r["new_rid"]:
            try: handle_change(r, subj)
            except Exception as e: log.error(f"  err: {e}", exc_info=True)
        else:
            log.warning(f"  예약번호 파싱 실패: old={r['old_rid']} new={r['new_rid']}")

    elif "확정" in subj:
        log.info(f"[confirm] {subj}")
        r = parse_confirm(body)
        if r["rid"]:
            try: handle_confirm(r["rid"])
            except Exception as e: log.error(f"  err: {e}", exc_info=True)

    elif "취소" in subj:
        log.info(f"[cancel] {subj}")
        r = parse_confirm(body)
        if r["rid"]:
            try: handle_cancel(r["rid"])
            except Exception as e: log.error(f"  err: {e}", exc_info=True)

    elif "접수" in subj or "신규" in subj:
        log.info(f"[new] {subj}")
        r = parse_new_reservation(body)
        if r.get("reservation_id"):
            log.info(f"  #{r['reservation_id']} {r.get('date','')} {r.get('time','')}")
            try: handle_new(r, subj)
            except Exception as e: log.error(f"  err: {e}", exc_info=True)
        else:
            log.warning(f"  예약번호 파싱 실패")

def proc_new(mail, processed):
    sd = (datetime.now() - timedelta(days=3)).strftime("%d-%b-%Y")
    s, d = mail.uid("search", None, f'(FROM "navercorp" SINCE {sd})')
    if s != "OK": return
    ids = d[0].split(); nc = 0
    for uid in ids:
        us = uid.decode()
        if us in processed: continue
        proc_msg(mail, uid); processed.add(us); nc += 1
    if nc: save_processed(processed); log.info(f"processed {nc} emails")

def idle_loop():
    processed = load_processed()
    while True:
        try:
            log.info("Gmail IMAP connecting...")
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            mail.select(GMAIL_FOLDER)
            proc_new(mail, processed)
            log.info("IMAP IDLE waiting...")
            while True:
                tag = mail._new_tag().decode()
                mail.send((tag + " IDLE\r\n").encode())
                resp = mail.readline()
                if b"+" not in resp: log.warning(f"IDLE fail: {resp}"); break
                mail.sock.settimeout(1500)
                try:
                    while True:
                        line = mail.readline()
                        if b"EXISTS" in line: log.info("New email!"); break
                        if b"BYE" in line: raise ConnectionError("bye")
                except (TimeoutError, OSError): pass
                mail.send(b"DONE\r\n"); mail.readline()
                mail.sock.settimeout(None)
                proc_new(mail, processed)
        except KeyboardInterrupt: break
        except Exception as e:
            log.error(f"error: {e}"); log.info("reconnect 30s...")
            time.sleep(30)

if __name__ == "__main__":
    log.info("Naver Gmail IMAP IDLE sync started")
    idle_loop()
