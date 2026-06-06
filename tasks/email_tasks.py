"""
邮件任务模块 - 实现定时邮件发送功能
"""
from broker_config import broker
import asyncio
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email_validator import validate_email, EmailNotValidError


# ============ 邮件配置（可改为从环境变量或配置文件读取）============
SMTP_SERVER = "smtp.qq.com"      # SMTP 服务器地址
SMTP_PORT = 587                   # SMTP 端口（TLS）
SENDER_EMAIL = "3498492554@qq.com"  # 发件人邮箱
SENDER_AUTH_CODE = "psjkyocjkgzjdbfd" # 邮箱授权码（非登录密码）


# ============ 异步发送邮件任务 ============
@broker.task
async def send_email(
    to_email: str,
    subject: str,
    body: str,
    body_type: str = "plain"
) -> dict:
    """
    异步发送邮件任务

    Args:
        to_email: 收件人邮箱
        subject: 邮件主题
        body: 邮件正文
        body_type: 正文类型 (plain/html)

    Returns:
        发送结果字典
    """
    # ----- 验证邮箱格式 -----
    try:
        validate_email(to_email)
    except EmailNotValidError as e:
        print(f"[Email] 邮箱格式错误: {to_email}, 错误: {e}")
        return {"status": "error", "message": f"邮箱格式错误: {e}"}

    # ----- 构建邮件内容 -----
    message = MIMEMultipart()
    message["From"] = SENDER_EMAIL
    message["To"] = to_email
    message["Subject"] = subject
    message.attach(MIMEText(body, body_type, "utf-8"))

    try:
        # ----- 连接 SMTP 服务器并发送（异步模拟，实际发送仍是同步阻塞）-----
        print(f"[Email] 正在连接 SMTP 服务器: {SMTP_SERVER}:{SMTP_PORT}")
        print(f"[Email] 正在发送邮件到: {to_email}")

        # 这里使用 asyncio.to_thread 避免阻塞事件循环
        def _send():
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
                server.starttls()  # 启用 TLS 加密
                server.login(SENDER_EMAIL, SENDER_AUTH_CODE)
                server.send_message(message)

        await asyncio.to_thread(_send)

        print(f"[Email] 邮件发送成功: {to_email}, 主题: {subject}")
        return {
            "status": "success",
            "to": to_email,
            "subject": subject
        }

    except smtplib.SMTPException as e:
        print(f"[Email] SMTP 错误: {e}")
        return {"status": "error", "message": f"SMTP 错误: {e}"}
    except Exception as e:
        print(f"[Email] 发送失败: {e}")
        return {"status": "error", "message": f"发送失败: {e}"}


# ============ 定时邮件任务示例 ============
@broker.task
async def send_daily_report() -> dict:
    """
    每日报告邮件任务 - 可配合 Cron 定时调度

    Returns:
        发送结果
    """
    report_content = """
    您好，

    这是一封自动发送的每日报告邮件。

    系统运行状态：正常
    当前时间：{current_time}

    此邮件由 TaskIQ 定时任务自动发送。

    ---
    基于 TaskIQ 的分布式任务调度平台
    """.format(current_time=asyncio.get_event_loop().time())

    return await send_email(
        to_email="recipient@example.com",  # 替换为实际收件人
        subject="【自动邮件】每日报告 - " + str(asyncio.get_event_loop().time())[:10],
        body=report_content,
        body_type="plain"
    )


# ============ 批量发送邮件任务 ============
@broker.task
async def send_batch_emails(recipients: list, subject: str, body: str) -> dict:
    """
    批量发送邮件任务

    Args:
        recipients: 收件人邮箱列表
        subject: 邮件主题
        body: 邮件正文

    Returns:
        批量发送结果统计
    """
    success_count = 0
    fail_count = 0
    failed_list = []

    for recipient in recipients:
        result = await send_email(
            to_email=recipient,
            subject=subject,
            body=body
        )
        if result.get("status") == "success":
            success_count += 1
        else:
            fail_count += 1
            failed_list.append(recipient)

        # 每个邮件间隔 0.5 秒，避免频率过快
        await asyncio.sleep(0.5)

    print(f"[Email] 批量发送完成: 成功 {success_count}, 失败 {fail_count}")
    return {
        "total": len(recipients),
        "success": success_count,
        "fail": fail_count,
        "failed_list": failed_list
    }
