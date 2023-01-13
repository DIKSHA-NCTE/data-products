import shutil
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
from datetime import date 
from datetime import timedelta

todays_date = date.today()
yesterdays_date = date.today() - timedelta(days=1)
end_date = yesterdays_date.strftime("%Y-%m-%d")


dir_name = '/home/analytics/lavanya/telemetry_failure/prod_output1/'+end_date+ '/sample'
subject = 'Prod Failed Events'

shutil.make_archive('/home/analytics/lavanya/telemetry_failure/prod_output1/'+end_date+ '/sample','zip',dir_name)

email_user = ''
email_password = ' '
#email_send = 'testingnewwork12345@gmail.com'
email_send = " "

subject = 'Prod Failed Events'

msg = MIMEMultipart()
msg['From'] = email_user
msg['To'] = email_send
msg['Subject'] = subject

body = "Hi All \nPlease find the attachment which includes [summary.csv & sample.zip] for Prod failed events \n \n Regards \n Lavanya KR"
msg.attach(MIMEText(body,'plain'))

files=['summary.csv','sample.zip']
for f in files:
    #dir_path= "/home/analytics/lavanya/telemetry_failure/prod_output/2021-06-05/";
    dir_path= '/home/analytics/lavanya/telemetry_failure/prod_output1/'+end_date
    file_path = os.path.join(dir_path, f)
    attachment = MIMEApplication(open(file_path, "rb").read(), _subtype="txt")
    attachment.add_header('Content-Disposition', 'attachment', filename=f)
    msg.attach(attachment)
text = msg.as_string()
server = smtplib.SMTP('smtp.gmail.com',587)
server.starttls()
server.login(email_user,email_password)


#server.sendmail(email_user,email_send,text)
server.sendmail(email_user,email_send.split(','),text)
server.quit()

shutil.rmtree(directory_path) # directory path
