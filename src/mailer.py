"""SendGrid email sender."""
import os

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def send(subject: str, html: str) -> int:
    """Send HTML email via SendGrid. Returns HTTP status code."""
    api_key = os.environ["SENDGRID_API_KEY"]
    msg = Mail(
        from_email=os.environ["DIGEST_FROM_EMAIL"],
        to_emails=os.environ["DIGEST_TO_EMAIL"],
        subject=subject,
        html_content=html,
    )
    client = SendGridAPIClient(api_key)
    resp = client.send(msg)
    return resp.status_code
