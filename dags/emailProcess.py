import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def send_email():
    # List of recipient email IDs
    li = ["zakarialukyguy@gmail.com"]

    # Sender's email credentials
    sender_email = "zakarialukyguy@gmail.com"
    sender_password = "emvu smjz wwkm bxqr"  # Use your app password here

    # Subject and body of the email
    subject = "Your Subject"
    body = "This is the body of the email."

    # Path to the file you want to send
    file_path = "/tmp/transformed_data_2024-10-29.csv"

    for dest in li:
        # Create the MIMEMultipart object to structure the email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = dest
        msg['Subject'] = subject

        # Attach the body of the email
        msg.attach(MIMEText(body, 'plain'))

        # Open the file in binary mode and attach it
        try:
            with open(file_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f"attachment; filename={file_path.split('/')[-1]}")
                msg.attach(part)
        except Exception as e:
            print(f"Error opening file: {e}")
            continue

        # Connect to the Gmail SMTP server and send the email
        try:
            with smtplib.SMTP('smtp.gmail.com', 587) as s:
                s.starttls()
                s.login(sender_email, sender_password)
                text = msg.as_string()
                s.sendmail(sender_email, dest, text)
                print(f"Email sent to {dest}")
        except Exception as e:
            print(f"Failed to send email to {dest}: {e}")

# Call the function to send the email

