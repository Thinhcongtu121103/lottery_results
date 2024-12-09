# send_email.py

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class SendEmail:
    def __init__(self, sender_email, sender_password, smtp_server, smtp_port):
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def send_email(self, to, subject, body):
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        try:
            # Kết nối đến máy chủ SMTP và gửi email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()  # Bật mã hóa TLS
                server.login(self.sender_email, self.sender_password)  # Đăng nhập vào tài khoản email
                server.sendmail(self.sender_email, to, msg.as_string())  # Gửi email
            print("Email sent successfully!")
        except Exception as e:
            print(f"Error sending email: {str(e)}")
