import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(sender_email, sender_password, recipient_email, subject, body):
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = subject

    message.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    
    server.starttls()

    server.login(sender_email, sender_password)

    text = message.as_string()
    server.sendmail(sender_email, recipient_email, text)

    server.quit()

    print(f'Email sent to {recipient_email}')

send_email('loanrequestprocess@gmail.com', 'loanrequestm2t', 'ouakrimzakaria18@gmail.com', 'Test Subject', 'This is the body of the email.')
