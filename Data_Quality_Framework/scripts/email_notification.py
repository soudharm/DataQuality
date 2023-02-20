from django.core.mail import send_mail
from django.conf import settings
from django.core.mail import EmailMessage
import pandas as pd
import os
import win32com.client as win32
from datetime import datetime


# def send_email_notification(file_path, project_path):
#     subject = 'Data Quality validation Result for ' + datetime.now().strftime('%#d %b %Y %H:%M')
#     content = """
#     Hi,
#
#     For more details, you can check the tabs in the Excel file attached.
#
#     Best regards,
#     Team Deloitte
#     """
#     email_from = settings.EMAIL_HOST_USER
#     full_path = os.path.join(project_path, "config") + r"\email_sender_list.csv"
#     df = pd.read_csv(full_path)
#     sender_emails = df['email_id'].tolist()
#     email = EmailMessage(
#         subject, content, email_from, sender_emails)
#     email.attach_file(file_path)
#     email.send()
    # send_mail(
    #     'test dqf tool',
    #     'email notifications working',
    #     settings.EMAIL_HOST_USER,
    #     ['supriya.shinde30@gmail.com'],
    #     fail_silently=False,
    # )

def send_email_notification(file_path, interface_path):
    outlook = win32.Dispatch('outlook.application')
    full_path = os.path.join(interface_path, "config") + r"\email_sender_list.csv"
    df = pd.read_csv(full_path)
    sender_emails = df['email_id'].tolist()
    mail = outlook.CreateItem(0)
    mail.Subject = 'Data Quality validation Result for ' + datetime.now().strftime('%#d %b %Y %H:%M')
    mail.To = ";".join(sender_emails)
    mail.HTMLBody = r"""
    Hi,<br><br>
    For more details, you can check the tabs in the Excel file attached.<br><br>
    Best regards,<br>
    Team Deloitte<br><br>
    """
    mail.Attachments.Add(file_path)
    mail.Send()


# def send_email_notification1():
#     subject = 'Data Quality validation Result for ' + datetime.now().strftime('%#d %b %Y %H:%M')
#     content = """
#     Hi,
#
#     For more details, you can check the tabs in the Excel file attached.
#
#     Best regards,
#     Team Deloitte
#     """
#     email_from = settings.EMAIL_HOST_USER
#     sender_emails = ['supshinde@deloitte.com']
#     email = EmailMessage(
#         subject, content, email_from, sender_emails)
#     email.send()