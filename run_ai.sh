#!/bin/bash
ssh -o StrictHostKeyChecking=no -l edwadm ai-prd-01 "/apps/dslab/anaconda/python3/bin/python /users/edwadm/CCW_QUOTING_KAFKA/consume_message_from_kafka_working.py > /users/edwadm/CCW_QUOTING_KAFKA/logs/prod_committed_$(date +%Y-%m-%d_%H:%M).log"
