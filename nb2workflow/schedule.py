import time
import atexit

from apscheduler.schedulers.background import BackgroundScheduler


def schedule_callable(f, interval):
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=f, trigger="interval", seconds=interval)
    scheduler.start()

    atexit.register(lambda: scheduler.shutdown())
