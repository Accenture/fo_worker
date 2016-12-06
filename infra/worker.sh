#!/usr/bin/env bash
DEPLOY_PATH=/kohls/apps/fo;
LOG_PATH=/kohls/apps/fo/logs;
for ((i=4; i>=1; i--))
    do
        now=$(date +"%d_%m_%Y_%H:%M:%S:%3N")
        cd ${DEPLOY_PATH}/fo_worker && nohup /var/lib/anaconda3/bin/python ${DEPLOY_PATH}/fo_worker/src/worker.py >> ${LOG_PATH}/Worker.$now.log 2>&1 &
    done
