from broker import broker
from taskiq.schedule_sources import LabelScheduleSource
from datetime import datetime

scheduler = broker.with_schedule_source(
    LabelScheduleSource(broker),
    [LabelScheduleSource(broker)]
)

@broker.task(schedule=[{"cron": "* * * * *"}])
async def every_minute_job() -> None:
    print(f"[定时任务] {datetime.now()}")