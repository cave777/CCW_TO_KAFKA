import os
from datetime import date
import glob
import jaydebeapi


def switch(case):
    return {
        "st_int_cq_deal_kafka.csv": st_int_cq_deal_kafka,
        "st_int_raw_dm_customer_kafka.csv": st_int_raw_dm_customer_kafka,
        "st_int_deal_master_kafka.csv": st_int_deal_master_kafka,
        "st_int_raw_cq_deal_party_vw_kafka.csv": st_int_raw_cq_deal_party_vw_kafka,
        "st_int_raw_cq_ns_deal_extn_vw_kafka.csv": st_int_raw_cq_ns_deal_extn_vw_kafka,
        "st_int_deal_reopen_reason_kafka.csv": st_int_deal_reopen_reason_kafka,
        "st_int_dmr_dm_team_assignment_kafka.csv": st_int_dmr_dm_team_assignment_kafka,
        "st_cq_deal_contact_kafka.csv": st_cq_deal_contact_kafka,
        "st_cq_competitor_kafka.csv": st_cq_competitor_kafka,
        "st_int_raw_cq_dealnsterm_vw_kafka.csv": st_int_raw_cq_dealnsterm_vw_kafka,
        "st_cq_state_transition_int_vw_kafka.csv": st_cq_state_transition_int_vw_kafka,
        "st_cq_qf_response_kafka.csv": st_cq_qf_response_kafka,
        "st_int_raw_pdr_deal_technology_kafka.csv": st_int_raw_pdr_deal_technology_kafka,
        "st_int_cq_single_quote_vw_kafka.csv": st_int_cq_single_quote_vw_kafka,
        "st_int_raw_cq_quote_partner_ex_kafka.csv": st_int_raw_cq_quote_partner_ex_kafka,
        "st_int_dmr_deal_bill_to_kafka.csv": st_int_dmr_deal_bill_to_kafka,
        "st_int_raw_pdr_deal_contact_kafka.csv": st_int_raw_pdr_deal_contact_kafka,
        "st_int_raw_deal_disti_fulfillm_kafka.csv": st_int_raw_deal_disti_fulfillm_kafka,
        "st_int_raw_cq_behavior_reward.csv": st_int_raw_cq_behavior_reward,
        "st_cq_approval_route_kafka.csv": st_cq_approval_route_kafka,
        "st_int_raw_cq_quote_line_vw_kafka.csv": st_int_raw_cq_quote_line_vw_kafka,
        "st_int_raw_cq_quote_lne_conces_kafka.csv": st_int_raw_cq_quote_lne_conces_kafka,

    }.get(case)


def st_int_cq_deal_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_CQ_DEAL_KAFKA_FF "
    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_dm_customer_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_DM_CUSTOMER_KFK_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_deal_master_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_DEAL_MASTER_KAFKA_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_cq_deal_party_vw_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_CQ_DL_PRTY_VW_KFK_F "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_cq_ns_deal_extn_vw_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_CQ_NS_DL_EXTN_VW_F "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_deal_reopen_reason_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_DEAL_REOPN_REASN_KFK_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_dmr_dm_team_assignment_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_DMR_DM_TEM_ASIMNT_KFK_F "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_cq_deal_contact_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_CQ_DEAL_CONTACT_KAFKA_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_cq_competitor_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_CQ_COMPETITOR_KAFKA_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_cq_dealnsterm_vw_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_CQ_DLNSTR_VW_KFK_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_cq_state_transition_int_vw_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_CQ_STATE_TRN_INT_VW_KFK_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_cq_qf_response_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_CQ_QF_RESPONSE_KAFKA_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_pdr_deal_technology_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_PDR_DEAL_TECH_KFK_F "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_cq_single_quote_vw_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_CQ_SNGL_QUTE_VW_KFKA_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_cq_quote_partner_ex_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RW_CQ_QUTE_PTR_EX_KFK_F "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_dmr_deal_bill_to_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_DMR_DL_BILL_TO_KAFKA_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_pdr_deal_contact_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_PDR_DEAL_CNT_KFK_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_deal_disti_fulfillm_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_DL_DISTI_FULF_KFK_F "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_cq_behavior_reward(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_CQ_BHVR_RWRD_KFKA_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_cq_approval_route_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_CQ_APPROVAL_ROUTE_KAFKA_FF"

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_cq_quote_line_vw_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RAW_CQ_QOT_LN_VW_KFK_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


def st_int_raw_cq_quote_lne_conces_kafka(flat_file_count, flat_file_name, conn):
    cur = conn.cursor()
    query = "SELECT count(*) from STGDB.ST_INT_RW_CQ_QT_LN_CNCS_KFK_FF "

    cur.execute(query)
    pre_stage_count = cur.fetchall()[0][0]
    diff = flat_file_count - pre_stage_count

    if diff != 0:
        print('Pre stage is not mathed with Flat File {} and diff is {}'.format(flat_file_name, diff))


if __name__ == '__main__':

    connection = jaydebeapi.connect(jclassname="com.teradata.jdbc.TeraDriver",
                                    url="jdbc:teradata://TDPROD.cisco.com/LOGMECH=TD2",
                                    driver_args=['EDW_QA_AUTOMATION', 'Welcome#1234'],
                                    jars=['/apps/dslab/jdbcjars/tdjars/tdgssconfig.jar',
                                          '/apps/dslab/jdbcjars/tdjars/terajdbc4.jar'])
    files_list = glob.glob("/users/edwadm/CCW_OUTPUT/{}/CCW_DATA_{}/*.csv".format(str(date.today()), str(date.today())))

    for f in files_list:
        file_count = os.popen('sort {} | uniq -c | wc -l'.format(f)).read().split()
        format_file = f.split('/')
        ff_count, filename = int(file_count[0]) - 1, format_file[-1]
        switch(filename)(ff_count, filename, connection)
