"""Email notifications via Resend."""

import logging

from newschat.config import RESEND_API_KEY, ALERT_EMAIL_FROM, ALERT_EMAIL_TO

log = logging.getLogger(__name__)


def send_alert_email(subject: str, body_html: str) -> bool:
    """Send an alert email via Resend. Returns True on success, False on failure.

    Silently skips if RESEND_API_KEY or ALERT_EMAIL_TO are not configured.
    """
    if not RESEND_API_KEY or not ALERT_EMAIL_TO:
        log.debug("Email not configured (missing RESEND_API_KEY or ALERT_EMAIL_TO)")
        return False

    try:
        import resend
        resend.api_key = RESEND_API_KEY
        resend.Emails.send({
            "from": ALERT_EMAIL_FROM,
            "to": [ALERT_EMAIL_TO],
            "subject": f"[NowTheNews] {subject}",
            "html": body_html,
        })
        log.info("Alert email sent: %s", subject)
        return True
    except Exception:
        log.exception("Failed to send alert email: %s", subject)
        return False
