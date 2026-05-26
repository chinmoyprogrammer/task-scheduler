from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

from app.rabbitmq_pub import publish_message
from datetime import datetime, timedelta

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
        "employeeDeactivation_queue",
        {
            "date": now_date
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


# ---------- Scheduler lifecycle ----------
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add cron jobs – same intervals you need
    scheduler.add_job(process_temp_attendance_data, 'cron', minute='*/30')
    scheduler.add_job(employee_deactivation, 'cron', second='*/10')
    # scheduler.add_job(check_inactive_employees, 'cron', hour=2, minute=0)
    # scheduler.add_job(generate_report, 'cron', minute='*/30')
    # scheduler.add_job(cleanup_logs, 'cron', day_of_week='sun', hour=3, minute=0)
    # scheduler.add_job(test_publish, 'interval', seconds=30, id='test_rabbitmq_job')

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
