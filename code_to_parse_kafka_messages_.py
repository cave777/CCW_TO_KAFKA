#!/apps/dslab/anaconda/python3/bin/python
import json
import os
import pdb
import time
from datetime import datetime, date
from functools import wraps
from email_alert import failed_notification

from util import *

with open('static_message.json') as file:
    data = json.load(file)


def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print("Total time running %s: %s seconds" %
              (function.__name__, str(t1 - t0))
              )
        return result

    return function_timer


file_path = 'CCW_OUTPUT/{}/CCW_DATA'.format(str(date.today())) + '_' + str(date.today())
static_file_path = 'CCW_OUTPUT/{}/CCW_STATIC'.format(str(date.today()))


def create_folder(file):
    if file == 'dynamic':
        if not os.path.exists(file_path):
            os.makedirs(file_path, exist_ok=True)
            return os.path.abspath(file_path)

        return os.path.abspath(file_path)

    elif file == 'static':
        if not os.path.exists(static_file_path):
            os.makedirs(static_file_path, exist_ok=True)
            return os.path.abspath(static_file_path)
        else:
            return os.path.abspath(static_file_path)


def data_frame_generator(json_data, required_field_list, in_rec=False):
    """
    Args:
        json_data: Complete JSON with all data.
        required_field_list: List of all required fields for the dataframe.

    Returns:
        A dataframe with required fields. Doesn't work if the values are in a list
        (pass one element after another if in list)
        """
    tmp_dict = {}
    final_dict = {}
    for key in json_data.keys():
        if isinstance(json_data[key], dict):
            tmp_dict.update(
                data_frame_generator(json_data[key], required_field_list, in_rec=True).to_dict('list'))
        else:
            if key in required_field_list:
                tmp_dict[key] = [json_data[key]]
    if not in_rec:
        for col in required_field_list:
            if col in tmp_dict.keys():
                final_dict[col] = tmp_dict[col]
            else:
                final_dict[col] = [None]
    return pd.DataFrame.from_dict(final_dict)


class Json_to_csv:
    # def __init__(self, json_data, offset, partition, timestamp):
    def __init__(self, json_data):
        # self.offset = offset
        # self.partition = partition
        # self.timestamp = timestamp
        self.data_area = json_data['dataArea']
        self.deal = json_data['dataArea']['deal']
        # self.quotes = self.deal['quotes']

    def write_to_csv(self, data_frame, file_name):
        return [data_frame.to_csv(create_folder('dynamic') + '/' + file_name, sep='ê',
                                  mode='a', encoding='utf-8', header=False, index=False),
                data_frame.to_csv(create_folder('static') + '/' + file_name, sep='ê',
                                  mode='a', encoding='utf-8', header=False, index=False)]

    def insert_kafka_offset_columns(self, data_frame):  # todo change something
        data_frame.insert(len(data_frame.T), 'UPDATED_BY_KAFKA', 'KAFKA', True)
        # data_frame.insert(len(data_frame.T), 'UPDATED_ON_KAFKA',
        #                   datetime.fromtimestamp(self.timestamp / 1e3).strftime("%Y-%m-%d %H:%M:%S"), True)
        # data_frame.insert(len(data_frame.T), 'OFFSET_NUMBER', self.offset, True)
        # data_frame.insert(len(data_frame.T), 'PARTITION_NUMBER', self.partition, True)
        # data_frame.insert(len(data_frame.T), 'MESSAGE_SEQUENCE_NUMBER', self.data_area['seq'], True)

    def log_to_db(self, flag, error_description=None, error_section=None):
        config_properties = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/config.ini', 'QUERY')
        time_stamp = datetime.fromtimestamp(self.timestamp / 1e3).strftime("%Y-%m-%d %H:%M:%S")
        conn = create_oracle_connection()
        if flag == 'success':
            insert_query = config_properties['INSERT_QUERY_SUCCESS'].format(self.offset, self.partition, time_stamp,
                                                                            self.deal['dealObjectId'],
                                                                            datetime.now().strftime(
                                                                                "%Y-%m-%d %H:%M:%S"),
                                                                            self.data_area['seq'])
            execute_oracle_qry(conn, insert_query)

        elif flag == 'fail':
            insert_query = config_properties['INSERT_QUERY_FAIL'].format(self.offset, self.partition, time_stamp,
                                                                         self.deal['dealObjectId'], error_description,
                                                                         error_section,
                                                                         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                                         1, self.data_area['seq'])
            execute_oracle_qry(conn, insert_query)
        close_connection(conn)

    def insert_general_kafka(self, flag, csv_filename):
        if flag in self.deal and self.deal[flag] != {}:
            constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini', flag.upper())
            data_parser = data_frame_generator(self.deal[flag], constant.keys())
            data_parser.loc[0, 'parentId'] = self.deal['dealObjectId']
            # if flag == 'endCustomer':
            #    data_parser.loc[0, 'ACCOUNT_TYPE'] = data_parser.iloc[0]['endCustomerType']
            self.insert_kafka_offset_columns(data_parser)
            data_parser.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
            self.write_to_csv(data_parser, csv_filename)

    def insert_generic_kafka(self, flag, csv_filename):
        if flag in self.deal and self.deal[flag] != []:
            constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini', flag.upper())
            for _, value in enumerate(self.deal[flag]):
                data_parser = data_frame_generator(value, constant.keys())
                data_parser.loc[0, 'parentId'] = self.deal['dealObjectId']
                self.insert_kafka_offset_columns(data_parser)
                data_parser.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                self.write_to_csv(data_parser, csv_filename)

    def insert_quote_generic_kafka(self, flag, csv_filename):
        for _, quotes in enumerate(self.deal['quotes']):
            if flag in quotes and quotes[flag] != []:
                constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini', flag.upper())
                for _, value in enumerate(quotes[flag]):
                    data_parser = data_frame_generator(value, constant.keys())
                    data_parser.loc[0, 'parentId'] = quotes['quoteId']
                    self.insert_kafka_offset_columns(data_parser)
                    data_parser.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                    self.write_to_csv(data_parser, csv_filename)

    def insert_quote_general_kafka(self, flag, csv_filename):
        for _, quotes in enumerate(self.deal['quotes']):
            if flag in quotes and quotes[flag] != {}:
                constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini', flag.upper())
                data_parser = data_frame_generator(quotes[flag], constant.keys())
                data_parser.loc[0, 'parentId'] = quotes['quoteId']
                self.insert_kafka_offset_columns(data_parser)
                data_parser.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                self.write_to_csv(data_parser, csv_filename)

    def insert_st_int_cq_deal_kafka(self):

        constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini', 'DEAL')
        data_parser = data_frame_generator(self.deal, constant.keys())
        data_parser.loc[0, 'optyNumber'] = data_parser.iloc[0]['dealId']
        if 'createrDetail' in self.deal:
            if 'role' in self.deal['createrDetail']:
                data_parser.loc[0, 'role'] = self.deal['createrDetail']['role']
            if 'paeUserId' in self.deal['createrDetail']:
                data_parser.loc[0, 'paeUserId'] = self.deal['createrDetail']['paeUserId']
        self.insert_kafka_offset_columns(data_parser)
        data_parser.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
        self.write_to_csv(data_parser, 'st_int_cq_deal_kafka.csv')

    def insert_st_int_cq_single_quote_vw_kafka(self, quotes_list):
        if 'quotes' in self.deal:
            constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini', 'QUOTES')
            for _, quotes in enumerate(quotes_list):
                data_parser = data_frame_generator(quotes, constant.keys())
                if 'priceListInfo' in quotes:
                    if 'erpPriceListId' in quotes['priceListInfo']:
                        data_parser.loc[0, 'erpPriceListId'] = quotes['priceListInfo']['erpPriceListId']
                    if 'currencyCode' in quotes['priceListInfo']:
                        data_parser.loc[0, 'currencyCode'] = quotes['priceListInfo']['currencyCode']
                data_parser.loc[0, 'parentId'] = self.deal['dealObjectId']
                self.insert_kafka_offset_columns(data_parser)
                data_parser.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                self.write_to_csv(data_parser, 'st_int_cq_single_quote_vw_kafka.csv')

    def insert_st_cq_approval_route_kafka(self):
        for _, quotes in enumerate(self.deal['quotes']):
            if 'standardApprovalRouteInfo' in quotes and quotes['standardApprovalRouteInfo'] != {}:
                if 'BR' in quotes['standardApprovalRouteInfo'].keys():
                    flag = 'BR'
                elif 'GPN' in quotes['standardApprovalRouteInfo'].keys():
                    flag = 'GPN'
                elif 'IR' in quotes['standardApprovalRouteInfo'].keys():
                    flag = 'IR'
                else:
                    flag = ''
                constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini',
                                                          'STANDARDAPPROVALROUTEINFO')
                for _, value in enumerate(quotes['standardApprovalRouteInfo'][flag]['approvalRoute']):
                    data_parser = data_frame_generator(value, constant.keys())
                    data_parser.loc[0, 'parentId'] = quotes['quoteId']
                    self.insert_kafka_offset_columns(data_parser)
                    data_parser.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                    self.write_to_csv(data_parser, 'st_cq_approval_route_kafka.csv')

    def insert_st_int_raw_pdr_deal_contact_kafka(self, quotes_list):
        for _, quotes in enumerate(quotes_list):
            if 'quoteContacts' in quotes:
                constant = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini',
                                                          'QUOTECONTACTS')
                if 'PARTNER' in quotes['quoteContacts']:
                    partner = quotes['quoteContacts']['PARTNER']['contact']
                    for part in partner:
                        data_parser1 = data_frame_generator(part, constant.keys())
                        if not data_parser1.empty:
                            data_parser1.loc[0, 'parentId'] = quotes['quoteId']
                            self.insert_kafka_offset_columns(data_parser1)
                            data_parser1.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                            self.write_to_csv(data_parser1, 'st_int_raw_pdr_deal_contact_kafka.csv')
                if 'ENDCUSTOMER' in quotes['quoteContacts']:
                    end_customer = quotes['quoteContacts']['ENDCUSTOMER']['contact']
                    for end in end_customer:
                        data_parser2 = data_frame_generator(end, constant.keys())
                        if not data_parser2.empty:
                            data_parser2.loc[0, 'parentId'] = quotes['quoteId']
                            self.insert_kafka_offset_columns(data_parser2)
                            data_parser2.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                            self.write_to_csv(data_parser2, 'st_int_raw_pdr_deal_contact_kafka.csv')

    def recursive_child_lines(self, child_line, constant_keys, quotes, in_rec=True):
        if in_rec:
            if 'childLines' in child_line:
                for _, child in enumerate(child_line['childLines']):
                    data_parser2 = data_frame_generator(child, constant_keys)
                    if not data_parser2.empty:
                        data_parser2.loc[0, 'parentId'] = quotes
                        self.insert_kafka_offset_columns(data_parser2)
                        data_parser2.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                        self.write_to_csv(data_parser2, 'st_int_raw_cq_quote_line_vw_kafka.csv')
                        self.recursive_child_lines(child, constant_keys, quotes)
            else:
                self.recursive_child_lines(child_line, constant_keys, quotes, in_rec=False)

    def recursive_child_concessions(self, child_line, constant_keys, quotes, in_rec=True):
        if in_rec:
            if 'childLines' in child_line:
                for _, child in enumerate(child_line['childLines']):
                    if 'concession' in child:
                        for _, child_concession in enumerate(child['concession']):
                            data_parser2 = data_frame_generator(child_concession, constant_keys)
                            if not data_parser2.empty:
                                self.insert_kafka_offset_columns(data_parser2)
                                data_parser2.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                                self.write_to_csv(data_parser2, 'st_int_raw_cq_quote_lne_conces_kafka.csv')
                    self.recursive_child_lines(child, constant_keys, quotes)
        else:
            self.recursive_child_lines(child_line, constant_keys, quotes, in_rec=False)

    def insert_st_int_raw_cq_quote_line_vw_kafka(self, quotes_list):
        for _, quotes in enumerate(quotes_list):
            if 'majorLineList' in quotes:
                constant = collect_property_file_contents('constant.ini', 'MAJORLINES')
                for _, major in enumerate(quotes['majorLineList']):
                    data_parser1 = data_frame_generator(major, constant.keys())
                    data_parser1.loc[0, 'parentId'] = quotes['quoteId']
                    if not data_parser1.empty:
                        self.insert_kafka_offset_columns(data_parser1)
                        data_parser1.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                        self.write_to_csv(data_parser1, 'st_int_raw_cq_quote_line_vw_kafka.csv')
                    if 'childLines' in major:
                        # pdb.set_trace()
                        for _, child in enumerate(major['childLines']):
                            data_parser2 = data_frame_generator(child, constant.keys())
                            if not data_parser2.empty:
                                data_parser2.loc[0, 'parentId'] = quotes['quoteId']
                                self.insert_kafka_offset_columns(data_parser2)
                                data_parser2.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                                self.write_to_csv(data_parser2, 'st_int_raw_cq_quote_line_vw_kafka.csv')
                                self.recursive_child_lines(child, constant.keys(), quotes['quoteId'])

    def insert_st_int_raw_cq_quote_lne_conces_kafka(self, quotes_list):
        for _, quotes in enumerate(quotes_list):
            if 'majorLineList' in quotes:
                constant = collect_property_file_contents('constant.ini', 'CONCESSION')
                for _, major in enumerate(quotes['majorLineList']):
                    if 'concession' in major:
                        for _, concession in enumerate(major['concession']):
                            data_parser1 = data_frame_generator(concession, constant.keys())
                            if not data_parser1.empty:
                                self.insert_kafka_offset_columns(data_parser1)
                                data_parser1.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                                self.write_to_csv(data_parser1, 'st_int_raw_cq_quote_lne_conces_kafka.csv')
                    if 'childLines' in major:
                        pdb.set_trace()
                        for _, child_line in enumerate(major['childLines']):
                            if 'concession' in child_line:
                                for _, child_concession in enumerate(child_line['concession']):
                                    data_parser2 = data_frame_generator(child_concession, constant.keys())
                                    if not data_parser2.empty:
                                        self.insert_kafka_offset_columns(data_parser2)
                                        data_parser2.replace('\n\n|\n|\r\n', ' ', regex=True, inplace=True)
                                        self.write_to_csv(data_parser2, 'st_int_raw_cq_quote_lne_conces_kafka.csv')

                            self.recursive_child_concessions(child_line, constant.keys(), quotes['quoteId'])

    def parse_all(self):
        try:
            if 'seq' in self.data_area:
                # print (self.data_area['seq'])
                # print (self.deal['dealId'])
                # print ('dealId' in self.deal)
                # print ("Message has seq")
                if 'dealId' in self.deal:
                    if self.deal['quotes']:
                        # print ("Message has dealId")
                        # print (self.data_area)
                        # self.insert_st_int_cq_deal_kafka()

                        # self.insert_general_kafka('endCustomer', 'st_int_raw_dm_customer_kafka.csv')
                        # self.insert_general_kafka('dealNodesDetails', 'st_int_deal_master_kafka.csv')
                        # self.insert_general_kafka('partyDetails', 'st_int_raw_cq_deal_party_vw_kafka.csv')
                        # self.insert_general_kafka('nsDealExtn', 'st_int_raw_cq_ns_deal_extn_vw_kafka.csv')
                        # self.insert_general_kafka('dealReopenReason', 'st_int_deal_reopen_reason_kafka.csv')
                        #
                        # self.insert_generic_kafka('teamDetails', 'st_int_dmr_dm_team_assignment_kafka.csv')
                        # self.insert_generic_kafka('dealContacts', 'st_cq_deal_contact_kafka.csv')
                        # self.insert_generic_kafka('competitorDetails', 'st_cq_competitor_kafka.csv')
                        # self.insert_generic_kafka('nonStdTerm', 'st_int_raw_cq_dealnsterm_vw_kafka.csv')
                        # self.insert_generic_kafka('stateTransitions', 'st_cq_state_transition_int_vw_kafka.csv')
                        # self.insert_generic_kafka('qfResponse', 'st_cq_qf_response_kafka.csv')
                        # self.insert_generic_kafka('technology', 'st_int_raw_pdr_deal_technology_kafka.csv')
                        #
                        # self.insert_st_int_cq_single_quote_vw_kafka(self.deal['quotes'])
                        # self.insert_quote_generic_kafka('distiFulfillmentDetails',
                        #                                 'st_int_raw_deal_disti_fulfillm_kafka.csv')
                        # self.insert_quote_generic_kafka('behaviourRewards', 'st_int_raw_cq_behavior_reward.csv')
                        #
                        # self.insert_quote_general_kafka('billingAddress', 'st_int_dmr_deal_bill_to_kafka.csv')
                        # self.insert_quote_general_kafka('partnerInfo', 'st_int_raw_cq_quote_partner_ex_kafka.csv')
                        #
                        # self.insert_st_cq_approval_route_kafka()
                        # self.insert_st_int_raw_pdr_deal_contact_kafka(self.deal['quotes'])
                        self.insert_st_int_raw_cq_quote_line_vw_kafka(self.deal['quotes'])
                        # self.insert_st_int_raw_cq_quote_lne_conces_kafka(self.deal['quotes'])

                        # self.log_to_db('success')

            #         else:
            #             raise KeyError('QUOTES SECTION IS MISSING')
            #             self.log_to_db('fail', 'QUOTES SECTION IS MISSING', 'QUOTES SECTION IS MISSING')
            #
            #     else:
            #         raise KeyError('DEALID KEY IS MISSING')
            #         self.log_to_db('fail', 'DEALID KEY IS MISSING', 'DEALID KEY IS MISSING')
            #
            # else:
            #     raise KeyError('SEQ KEY IS MISSING')
            #     self.log_to_db('fail', 'SEQ KEY IS MISSING', 'SEQ KEY IS MISSING')

        except Exception as e:
            print("parse_all exception")
            print(str(e))
            # failed_notification(eval(str(e)), self.offset, self.partition)
            # final_offset = 'offset' + str(int(self.offset)) + '_partition_' + str(int(self.partition)) + '.json'
            # with open(final_offset, 'w') as outfile:
            #     json.dump(self.value, outfile)
            # self.log_to_db('fail', eval(str(e)), eval(str(e)))
            # pass
            # return str(e)

        finally:
            pass


j_to_c = Json_to_csv(data)
j_to_c.parse_all()

# for col in data.columns:
#   if data[col].dtypes == 'object':
#     data[col] = data[col].str[:1]
# print(data)