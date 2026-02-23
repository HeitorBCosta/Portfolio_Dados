from datetime import datetime, time
from app.config.settings import Settings

def get_today_range():
    now = datetime.now(Settings.TIMEZONE)
    start = datetime.combine(now.date(), time.min)
    end = datetime.combine(now.date(), time.max)
    return start, end
