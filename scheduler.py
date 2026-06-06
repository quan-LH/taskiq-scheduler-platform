from broker import broker
from taskiq.schedule_sources import LabelScheduleSource
from datetime import datetime


# 调度器配置 - 支持 Cron 和 Interval 两种定时方式
scheduler = broker.with_schedule_source(
    LabelScheduleSource(broker),
    [LabelScheduleSource(broker)]
)


# ============ Cron 定时任务 ============
@broker.task(schedule=[{"cron": "* * * * *"}])
async def every_minute_job() -> None:
    """每分钟执行一次"""
    print(f"[Cron定时] 每分钟任务执行: {datetime.now()}")


@broker.task(schedule=[{"cron": "0 8 * * *"}])
async def daily_morning_job() -> None:
    """每天早上8点执行"""
    print(f"[Cron定时] 每日早8点任务: {datetime.now()}")


# ============ Interval 定时任务 ============
@broker.task(schedule=[{"interval": "10s"}])
async def every_10_seconds_job() -> None:
    """每10秒执行一次"""
    print(f"[Interval定时] 每10秒任务: {datetime.now()}")


# ============ 定时邮件任务（新增）============
# 注意：需要先配置 tasks/email_tasks.py 中的邮件服务器信息
from tasks.email_tasks import send_daily_report  # noqa: E402

@broker.task(schedule=[{"cron": "0 9 * * *"}])
async def scheduled_daily_email() -> None:
    """每天早上9点发送日报邮件"""
    print(f"[Cron定时] 准备发送每日邮件: {datetime.now()}")
    result = await send_daily_report()
    print(f"[Cron定时] 邮件发送结果: {result}")
