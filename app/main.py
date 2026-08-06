from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import os
from dotenv import load_dotenv

from app.rabbitmq_pub import publish_message
from app.redis_cache_checker import get_missing_cache_keys, REQUIRED_CACHE_KEYS
from datetime import datetime, timedelta

load_dotenv()

def parse_cron_config(env_var: str, default: str) -> dict:
    config_str = os.getenv(env_var, default)
    config = {}
    for pair in config_str.split(','):
        if '=' in pair:
            key, value = pair.strip().split('=', 1)
            config[key.strip()] = value.strip()
    return config

is_yesterday_processed = False

# ---------- Job functions ----------
async def process_temp_attendance_data():
    global is_yesterday_processed

    now_date = datetime.now().strftime("%Y-%m-%d")
    await publish_message(
        "processTempData_trigger_queue",
        {
            "start_date": now_date,
            "end_date": now_date,
        },
    )
    if is_yesterday_processed == False or now_date != is_yesterday_processed:

        yesterday = datetime.now() - timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y-%m-%d")
        await publish_message(
            "processTempData_trigger_queue",
            {
                "start_date": yesterday_date,
                "end_date": yesterday_date,
            },
        )
        is_yesterday_processed = now_date
        
        
# --------------- Employee Deactivation ---------------
async def employee_deactivation():
    now_date = datetime.now().strftime("%Y-%m-%d")
    await publish_message(
        "employeeDeactivation_trigger_queue",
        {
            "date": now_date
        },
    )
    
# --------------- Insert Weekend Holidays ---------------
async def insert_weekend_holidays():
    year = datetime.now().strftime("%Y")
    await publish_message(
        "insertWeekendHolidays_trigger_queue",
        {
            "year": year,
        },
    )

# --------------- Confirm Provisional Employees ---------------
async def confirm_provisional_employees():
    await publish_message(
        "confirmProvisionalEmployees_trigger_queue",
        {
            "date": datetime.now().strftime("%Y-%m-%d"),
        },
    )
    
# --------------- Check Inactive Employees ---------------
async def sync_roster_assignments():
    await publish_message(
        "syncRosterAssignments_trigger_queue",
        {
            "date": datetime.now().strftime("%Y-%m-%d"),
        },
    )
    
# --------------- Fiscal Year Closing ---------------
async def fiscal_year_closing():
    await publish_message(
        "fiscalYearClosing_trigger_queue",
        {
            "date": datetime.now().strftime("%Y-%m-%d"),
        },
    )



# async def generate_report():
#     await publish_message("reports_queue", {"task": "generate_report"})


# async def cleanup_logs():
#     await publish_message("cleanup_queue", {"task": "cleanup_logs"})

# async def test_publish():
#     await publish_message("test_queue", {"test": "hello", "timestamp": str(datetime.now())})
# async def test_publish():
#     await publish_message("test_queue", {"test": "hello", "timestamp": str(datetime.now())})


# ---------- Cache warmup (before scheduler) ----------
async def ensure_cache_indexes():
    print(f"[cache-warmup] Checking {len(REQUIRED_CACHE_KEYS)} required cache keys in Redis...")
    missing = await get_missing_cache_keys()
    if missing:
        print(f"[cache-warmup] Missing {len(missing)} cache keys: {missing}")
        print("[cache-warmup] Triggering full cache bg job via cacheRegenerate_queue")
        await publish_message(
            "cacheRegenerate_queue",
            {"type": []},
        )
    else:
        print("[cache-warmup] All cache keys present; skipping cache bg job.")
    return missing


# ---------- Scheduler lifecycle ----------
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add cron jobs – same intervals you need
    scheduler.add_job(
        process_temp_attendance_data, 
        'cron', 
        **parse_cron_config('CRON_PROCESS_TEMP_ATTENDANCE', 'hour=*/1'),
        misfire_grace_time=30, 
        id='processTempData_job', 
        max_instances=1
    )
    scheduler.add_job(
        employee_deactivation, 
        'cron', 
        **parse_cron_config('CRON_EMPLOYEE_DEACTIVATION', 'hour=0,minute=0'),
        misfire_grace_time=30, 
        id='employeeDeactivation_job', 
        max_instances=1
    )
    scheduler.add_job(
        insert_weekend_holidays, 
        'cron', 
        **parse_cron_config('CRON_INSERT_WEEKEND_HOLIDAYS', 'month=1,day=1,hour=0,minute=0,second=0'),
        misfire_grace_time=30, 
        id='insertWeekendHolidays_job', 
        max_instances=1
    )
    scheduler.add_job(
        confirm_provisional_employees,
        'cron', 
        **parse_cron_config('CRON_CONFIRM_PROVISIONAL_EMPLOYEES', 'minute=*/1'),
        misfire_grace_time=30, 
        coalesce=True, 
        id='confirmProvisionalEmployees_job', 
        max_instances=1
    )
    scheduler.add_job(
        sync_roster_assignments,
        'cron', 
        **parse_cron_config('CRON_SYNC_ROSTER_ASSIGNMENTS', 'minute=*/5'),
        misfire_grace_time=30, 
        coalesce=True, 
        id='syncRosterAssignments_job', 
        max_instances=1
    )
    scheduler.add_job(
        fiscal_year_closing,
        'cron', 
        **parse_cron_config('CRON_FISCAL_YEAR_CLOSING', 'month=1,day=1,hour=0,minute=0,second=0'),
        misfire_grace_time=30, 
        id='fiscalYearClosing_job', 
        max_instances=1
    )

     
    
    
    # scheduler.add_job(check_inactive_employees, 'cron', hour=2, minute=0)
    # scheduler.add_job(generate_report, 'cron', minute='*/30')
    # scheduler.add_job(cleanup_logs, 'cron', day_of_week='sun', hour=3, minute=0)
    # scheduler.add_job(test_publish, 'interval', seconds=30, id='test_rabbitmq_job')

    await ensure_cache_indexes()

    scheduler.start()
    print("✅ Scheduler started (including test job).")
    yield
    scheduler.shutdown()
    print("✅ Scheduler stopped (including test job)..")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}
    
@app.post("/trigger/test")
async def trigger_test():
    await test_publish()
    return {"status": "test published"}
