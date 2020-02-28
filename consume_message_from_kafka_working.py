#!/apps/dslab/anaconda/python3/bin/python
import logging
import time
import json
import pdb
import sys
import datetime

from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka import OffsetAndMetadata
import multiprocessing as mp
from code_to_parse_kafka_messages import Json_to_csv
from create_empty_csv_files import generate_csv_files
from email_alert import email_notification
from email_alert import kafka_notification
# from email_alert import *
# from email_alert import email_alert failed_notification
from check_kafka_status import app_kafka_monitor
from move_csv_files import move_csv_files


# with open('ex.json') as constant_json:
#     constant_data = json.load(constant_json)


def consumer_func():
    global j_to_c
    # RETIRED STAGE
    # consumer = KafkaConsumer(bootstrap_servers='quote-kfk-cstg1-01:9092', auto_offset_reset='earliest',
    #                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    # NEW STAGE
    # consumer = KafkaConsumer(bootstrap_servers='quote-kfk-cstg2-02:9092', auto_offset_reset='earliest',
    #                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    # INCREMENTAL LOAD STAGE
    # try:
    #    consumer = KafkaConsumer(bootstrap_servers='quote-kfk-stg2-02:9092', auto_offset_reset='earliest',
    #                            group_id = 'EDWGROUP',
    #                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    # except Exception as e:
    #    print("Failed to Connect to Kafka Broker")
    #    print(str(e))
    #    # pass
    #    kafka_notification()
    #    sys.exit(1)

    # PROD
    # consumer = KafkaConsumer(bootstrap_servers='quote-kfk-prd-01:9092',
    #                         auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    print("Connected to Kafka Consumer at %s: " % datetime.datetime.now())
    try:
        consumer = KafkaConsumer(bootstrap_servers='quote-kfk-prd-01:9092', auto_offset_reset='earliest',
                                 group_id='EDWGROUP', enable_auto_commit=False,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    except Exception as e:
        print("Failed to Connect to Kafka Broker")
        print(str(e))
        # pass
        kafka_notification()
        sys.exit(1)

    consumer.subscribe(['EDW'])
    consumer.poll(timeout_ms=1000, max_records=1)
    read_time = time.time()
    print("--- %s seconds read time ---" % (time.time() - read_time))
    # pdb.set_trace()
    count = 0
    pool = mp.Pool()
    START_TIME = time.time()
    print("Started parsing at %s: " % datetime.datetime.now())
    for message in consumer:
        # pdb.set_trace()
        try:
            temp_message = message

            if (count == 150000) or ((time.time() - START_TIME) >= 1800):
                # time.sleep(120)
                print(message.offset)
                print(message.partition)
                j_to_c = Json_to_csv(message.value, message.offset, message.partition, message.timestamp)
                print("after JSON_To-CSV")
                # j_to_c.parse_all()
                pool.apply_async(j_to_c.parse_all, )
                print("after parsing")
                print('New Kafka offset in part0: %s' % consumer.committed(TopicPartition('EDW', 0)))
                print('New Kafka offset in part1: %s' % consumer.committed(TopicPartition('EDW', 1)))
                print("Break here")
                break

            print(message.offset)
            print(message.partition)

            count = count + 1
            print(count)

            j_to_c = Json_to_csv(message.value, message.offset, message.partition, message.timestamp)
            print("after JSON_To-CSV")
            # j_to_c.parse_all()
            pool.apply_async(j_to_c.parse_all, )
            print("after parsing")

            try:
                consumer.commit()
            except Exception as e:
                print("Commit exception : Unable to rebalance due to no message found (missing message value)")
                print(str(e))
                # Json_to_csv.log_to_db('fail', "Commit exception : Unable to rebalance due to no message found (missing message value)", "Commit exception : Unable to rebalance due to no message found (missing message value)")
                pass

            print('New Kafka offset in part0: %s' % consumer.committed(TopicPartition('EDW', 0)))
            print('New Kafka offset in part1: %s' % consumer.committed(TopicPartition('EDW', 1)))

            # if count > 86774:
            #    break

            # final_offset = 'offset' + str(int(message.offset)) + '_partition_' + str(int(message.partition)) + '.json'
            # with open(final_offset, 'w') as outfile:
            #    json.dump(message.value, outfile)
            # if message.offset == 39768:
            #    break


        except Exception as e:
            print("Consumer exception")
            print(str(e))
            # print(message.offset)
            # print(message.partition)
            # print(temp_message.value)
            final_offset = 'offset' + str(int(temp_message.offset)) + '_partition_' + str(
                int(temp_message.partition)) + '.json'
            with open(final_offset, 'w') as outfile:
                json.dump(temp_message.value, outfile)
            # Json_to_csv.log_to_db('fail', "Consumer Exception : Please Check", "Consumer Exception : Please Check")
            pass

        finally:
            pass
    print("Finished Kafka polling at %s: " % datetime.datetime.now())
    pool.close()

    print("after pool.close")
    pool.join()
    print("Finished parsing at %s: " % datetime.datetime.now())
    # consumer.commit_async()
    consumer.close(False)
    print("Closed Kafka Consumer at %s: " % datetime.datetime.now())
    # consumer.close(True)


if __name__ == "__main__":
    start_time = time.time()
    print("Started script at %s: " % datetime.datetime.now())
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    generate_csv_files()

    # address = 'quote-kfk-stg2-02'
    address = 'quote-kfk-prd-01'
    port = 9092
    if app_kafka_monitor(address, port)['STATUS'] == 'SUCCESS':
        consumer_func()
        print("Started scp at %s: " % datetime.datetime.now())
        move_csv_files()
        print("Finished scp at %s: " % datetime.datetime.now())
        # move_csv_file_stage()
        # move_csv_file_prod()
        email_notification()

    else:
        pass

    print("Finished script at %s: " % datetime.datetime.now())
    print("--- %s seconds ---" % (time.time() - start_time))
