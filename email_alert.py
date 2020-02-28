#!/apps/dslab/anaconda/python3/bin/python
import os
import smtplib
import time
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE

from util import *


def generate_file_name():
    filename = 'failed_offsets' + '_' + str(datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + '.log'
    return filename


def email_alert():
    config_properties = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/config.ini', 'QUERY')
    conn = create_oracle_connection()
    df = execute_oracle_df_qry(conn, config_properties['SELECT_FAIL_QUERY'])
    close_connection(conn)
    return [df.T.to_dict(), df]


def send_email(tolist, msgbody, subject):
    """
    Send an email to specific list of users

    : param tolist: list of emails to send the specific email too
    : param subject: subject of the email being sent
    : param msgbody : the message body of the email to be sent
    """

    msg = MIMEMultipart()
    message = msgbody
    msgfrom = 'no-reply@cisco.com'
    msg['Subject'] = subject
    msg['From'] = msgfrom
    msg['To'] = COMMASPACE.join(tolist)

    file_name = generate_file_name()
    with open(file_name, 'w+') as attachment:
        attachment.write(message)

    with open(file_name, 'rb')as file:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= " + file_name)

    msg.attach(part)

    s = smtplib.SMTP('outbound.cisco.com')
    s.sendmail(msgfrom, tolist, msg.as_string())
    s.quit()
    os.remove(file_name)
    return "Message sent!"

def failed_notification(error_description,offset,partition):
    content = "Failed to adhere to agreed upon format due to " + error_description + " for Offset# " +offset+ " and Partition# " +partition+ " at " + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #Testing
    #send_email(["nvoronch@cisco.com", "stanikon@cisco.com", "pbasaval@cisco.com", "spolampa@cisco.com", "vedaggub@cisco.com", "surpanda@cisco.com"], content, "Attention Failed Offsets! CCW source and ETL developers.")
    #Prod
    send_email(["quoting-dev@cisco.com", "edwtdduty@cisco.com", "edwduty@cisco.com", "ccw-edw-onsite-dev@cisco.com"], content, "Attention Failed Offsets! CCW source and ETL developers.")


def kafka_notification():
    content = "Failed to Connect to Kafka Broker at " + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #Testing
    #send_email(["nvoronch@cisco.com", "stanikon@cisco.com", "pbasaval@cisco.com", "spolampa@cisco.com", "vedaggub@cisco.com", "surpanda@cisco.com"], content, "Attention : Kafka Broker is unavailable. CCW/EDW Ops Team to check and bring it back up. Please inform us when this will be available.")
    #Prod
    send_email(["quoting-dev@cisco.com", "edwtdduty@cisco.com", "edwduty@cisco.com", "ccw-edw-onsite-dev@cisco.com"], content, "Attention : Kafka Broker is unavailable. CCW/EDW Ops Team to check and bring it back up. Please inform us when this will be available.")


def inbox_notification():
    content = "Failed to Move files to inbox folder at " + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) \
        + "\n Team shall investigate why the issue is occurring." \
        + "\n Once permission or inbox issue is resolved, Manually copy using the following steps:" \
        + "\n 1) Login and sudo to edwadm@ai-prd-07" \
        + "\n 2) scp ~/CCW_OUTPUT/<Date>/CCW_STATIC/* edwtdadm@unidev-info-1:/apps/informatica/admin/EDW_DV1_Dev_101/DATA/EDWTD_SALES_ORDERS/inbox/"
    #Testing
    #send_email(["nvoronch@cisco.com", "stanikon@cisco.com", "pbasaval@cisco.com", "spolampa@cisco.com", "vedaggub@cisco.com", "surpanda@cisco.com"], content, "Attention : Not able to move today's batch to inbox folder. CCW/EDW Ops Team to check and resolve issue. Please inform us when inbox issue is resolved.")
    #Prod
    send_email(["quoting-dev@cisco.com", "edwtdduty@cisco.com", "edwduty@cisco.com", "ccw-edw-onsite-dev@cisco.com"], content, "Attention : Not able to move today's batch to inbox folder. CCW/EDW Ops Team to check and resolve issue. Please inform us when inbox issue is resolved.")


def email_notification():
    if not email_alert()[1].empty:
        data = email_alert()[0]
        failed_offsets = []

        for i in data:
            failed_offsets += [
                str(data[i]['kafka_offset_number']) + "\t\t\t||\t\t" + str(
                    data[i]['kafka_partition_number']) + "\t\t||\t" + str(
                    data[i]['kafka_timestamp'].to_pydatetime()) + "\t||\t" + str(data[i]['deal_id']) + "\t||\t" + data[i][
                    'error_description'] + "\t||\t" + str(data[i]['time_failed'].to_pydatetime())]
        content = "KAFKA_OFFSET_NUMBER\t||\tKAFKA_PARTITION_NUMBER\t||\tKAFKA_TIMESTAMP\t\t||\tDEAL_ID\t\t||" \
                  "\tERROR_DESCRIPTION\t\t||\tTIME_FAILED\n"
        for fail_offset in failed_offsets:
            content += "\n" + fail_offset + "\n"

        #Testing
        #send_email(["nvoronch@cisco.com", "stanikon@cisco.com", "pbasaval@cisco.com", "spolampa@cisco.com", "vedaggub@cisco.com", "surpanda@cisco.com"], content, "Attention Failed Offsets! CCW source and ETL developers")
        #Prod
        send_email(["quoting-dev@cisco.com", "edwtdduty@cisco.com", "edwduty@cisco.com", "ccw-edw-onsite-dev@cisco.com"], content, "Attention Failed Offsets! CCW source and ETL developers")

        for offset in email_alert()[1]['kafka_offset_number']:
            conn1 = create_oracle_connection()
            update_query = "update ccw_edw_failed_kafka_offsets set email_flag=0 where kafka_offset_number='{}' ".format(
                offset)
            execute_oracle_qry(conn1, update_query)
            close_connection(conn1)

    else:
        print('NO FAILED OFFSETS TO SEND EMAIL')
