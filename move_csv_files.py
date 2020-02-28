#!/apps/dslab/anaconda/python3/bin/python
from datetime import date

from paramiko import SSHClient
from scp import SCPClient
import logging
import shutil
from email_alert import inbox_notification

# SSH/SCP Directory Recursively
def ssh_scp_files(ssh_host, ssh_user, source_volume, destination_volume):
    logging.info("In ssh_scp_files()method, to copy the files to the server")
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(ssh_host, username=ssh_user)

    with SCPClient(ssh.get_transport()) as scp:
        scp.put(source_volume, recursive=True, remote_path=destination_volume)
        shutil.rmtree(source_volume)

def move_csv_files():
    try:
        #ssh_scp_files('unidev-info-1', 'edwtdadm',
        #              'CCW_OUTPUT/{}/CCW_STATIC/'.format(str(date.today())),
        #              '/apps/informatica/admin/EDW_DV1_Dev_101/DATA/EDWTD_SALES_ORDERS/inbox/')
        ssh_scp_files('unistg-info-1', 'edwtdadm',
                      'CCW_OUTPUT/{}/CCW_STATIC/'.format(str(date.today())),
                      '/apps/informatica/admin/EDW_TS1_TEST_101/DATA/EDWTD_SALES_ORDERS/inbox/')
        #ssh_scp_files('infouniv-iprd-rcdn9-01', 'edwtdadm',
        #              'CCW_OUTPUT/{}/CCW_STATIC/'.format(str(date.today())),
        #              '/apps/informatica/admin/EDW_Prod_101/DATA/EDWTD_SALES_ORDERS/inbox/')
    except Exception as e:
        print("move_csv_files exception")
        print(str(e))
        inbox_notification()
        # failed_offset = 'offset' + str(int(self.offset)) + '_partition' + str(
        #     int(self.partition)) + '_timestamp' + str(self.timestamp) + str(e) + '.json'
        # return str(e)
