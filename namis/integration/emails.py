
import logging
from django.core.mail import EmailMessage
from django.contrib.auth.models import User
from django.template.loader import render_to_string

# Configure logging
logger = logging.getLogger(__name__)


def send_email(subject, template_name, context=None, attachments=None):
    users = User.objects.all()
    recipient_list = [user.email for user in users if user.email]

    html_message = render_to_string(template_name, context)

    plain_message = 'Data posted successfully.'

    email = EmailMessage(subject, plain_message,None,recipient_list)

    email.content_subtype = 'html'
    email.body = html_message

    if attachments:
        for attachment in attachments:
            email.attach(attachment['filename'],attachment['content'], attachment['mimetype'])
    try:
        email.send(fail_silently=False)
        logger.info("Email sent successfully to all users.")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise
