from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.rabbitmq_pub import publish_message
from datetime import datetime

# ---------- Job functions ----------
async def check_inactive_employees():
    await publish_message("inactive_queue", {"task": "check_inactive_employees"})


async def generate_report():
    await publish_message("reports_queue", {"task": "generate_report"})


async def cleanup_logs():
    await publish_message("cleanup_queue", {"task": "cleanup_logs"})

async def test_publish():
    await publish_message("test_queue", {"test": "hello", "timestamp": str(datetime.now())})


# ---------- Scheduler lifecycle ----------
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add cron jobs – same intervals you need
    scheduler.add_job(check_inactive_employees, 'cron', hour=2, minute=0)
    scheduler.add_job(generate_report, 'cron', minute='*/30')
    scheduler.add_job(cleanup_logs, 'cron', day_of_week='sun', hour=3, minute=0)
    scheduler.add_job(test_publish, 'interval', seconds=30, id='test_rabbitmq_job')

    scheduler.start()
    print("✅ Scheduler started (including test job).")
    yield
    scheduler.shutdown()
    print("✅ Scheduler stopped (including test job)..")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}