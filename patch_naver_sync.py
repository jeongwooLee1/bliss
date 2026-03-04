import json, os

path = '/home/ubuntu/naver-sync/naver_sync.py'
with open(path, 'r') as f:
    code = f.read()

queue_func = """
SCRAPE_QUEUE_FILE = "/home/ubuntu/naver-sync/scrape_queue.json"

def load_queue():
    try:
        with open(SCRAPE_QUEUE_FILE) as f: return json.load(f)
    except: return []

def save_queue(q):
    with open(SCRAPE_QUEUE_FILE, "w") as f: json.dump(q, f, ensure_ascii=False)

def scrape_and_update(by_biz):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("playwright 없음"); return 0
    updated_count = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = None
        if os.path.exists(SESSION_FILE):
            try:
                with open(SESSION_FILE, "r") as f:
                    storage = json.load(f)
                if storage.get("cookies"):
                    ctx = browser.new_context(storage_state=SESSION_FILE)
            except: pass
        if not ctx: ctx = browser.new_context()
        page = ctx.new_page()
        ensure_login(page)
        try: ctx.storage_state(path=SESSION_FILE)
        except: pass
        for biz_id, targets in by_biz.items():
            target_info = {}
            for k, row in targets.items():
                if k: target_info[k] = row.get("cust_name", "") or ""
            log.info(f"사업장 {biz_id}: {len(target_info)}건 조회")
            results = parse_booking_table(page, biz_id, target_info)
            for rid, info in results.items():
                row = targets[rid]
                existing_name = row.get("cust_name", "") or ""
                existing_phone = row.get("cust_phone", "") or ""
                if existing_name and "*" not in existing_name and existing_phone:
                    log.info(f"  ⏭ #{rid} 이미 완전 → 스킵"); continue
                extracted_name = info["name"]
                extracted_phone = info["phone"].replace("-", "")
                if existing_name and "*" in existing_name:
                    if not validate_name(extracted_name, existing_name):
                        log.warning(f"  ⛔ #{rid} 이름 불일치: {extracted_name} vs {existing_name}"); continue
                update_data = {}
                if not existing_name or "*" in existing_name: update_data["cust_name"] = extracted_name
                if not existing_phone: update_data["cust_phone"] = extracted_phone
                if update_data:
                    try:
                        sb_update("reservations", row["id"], update_data)
                        log.info(f"  💾 #{rid} → {extracted_name} / {extracted_phone}")
                        updated_count += 1
                    except Exception as e:
                        log.error(f"  DB 업데이트 실패 #{rid}: {e}")
            not_found = [r for r in target_info if r not in results]
            if not_found: log.info(f"  ⏭ 미발견: {len(not_found)}건 - {not_found[:5]}")
        try: browser.close()
        except: pass
    return updated_count

def process_queue():
    queue = load_queue()
    if not queue: return 0
    log.info(f"📬 scrape_queue: {len(queue)}건 즉시 처리")
    biz_map = get_naver_biz_ids()
    by_biz = {}
    processed_rids = set()
    for item in queue:
        rid = item.get("rid")
        if not rid: continue
        r = requests.get(f"{SUPABASE_URL}/rest/v1/reservations?select=*&reservation_id=eq.{rid}", headers=HEADERS)
        if not r.ok or not r.json():
            log.warning(f"  #{rid} DB 없음"); processed_rids.add(rid); continue
        row = r.json()[0]
        if row.get("cust_name") and "*" not in row.get("cust_name","") and row.get("cust_phone"):
            log.info(f"  #{rid} 이미 완전 → 스킵"); processed_rids.add(rid); continue
        branch_id = row.get("bid")
        biz_id = item.get("biz_id") or biz_map.get(branch_id)
        if not biz_id:
            log.warning(f"  #{rid} biz_id 없음"); continue
        if biz_id not in by_biz: by_biz[biz_id] = {}
        by_biz[biz_id][str(rid)] = row
        processed_rids.add(str(rid))
    updated = scrape_and_update(by_biz) if by_biz else 0
    remaining = [q for q in queue if q.get("rid") not in processed_rids]
    save_queue(remaining)
    log.info(f"  큐 처리 완료: {updated}건, 잔여 {len(remaining)}건")
    return updated

"""

# run_sync 앞에 삽입
if 'def run_sync():' in code and 'SCRAPE_QUEUE_FILE' not in code:
    code = code.replace('def run_sync():', queue_func + 'def run_sync():', 1)
    print("큐 함수 삽입 완료")
else:
    print("이미 적용됐거나 위치 없음")

# run_sync에서 브라우저 로직 제거하고 scrape_and_update + process_queue 활용
# main()에서 process_queue() 먼저 호출
old_main_sync = '    # 시작 시 한 번 동기화\n    try:\n        run_sync()\n    except Exception as e:\n        log.error(f"초기 동기화 오류: {e}")'
new_main_sync = '    # 시작 시: 큐 처리 후 전체 동기화\n    try:\n        process_queue()\n        run_sync()\n    except Exception as e:\n        log.error(f"초기 동기화 오류: {e}")'

if old_main_sync in code:
    code = code.replace(old_main_sync, new_main_sync)
    print("main() 큐 우선 처리 적용 완료")
else:
    print("main() 패치 위치 없음")

# 폴링 루프에서도 큐 우선 처리
old_poll = '            run_sync()'
new_poll = '            process_queue()\n            run_sync()'
if old_poll in code:
    code = code.replace(old_poll, new_poll, 1)
    print("폴링 루프 큐 우선 처리 완료")

with open(path, 'w') as f:
    f.write(code)
print("저장 완료:", path)
