#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../..
workload_config=${root_dir}/conf/workloads/structuredStreaming/groupBy.conf
. "${root_dir}/bin/functions/load_bench_config.sh"


enter_bench HadoopPrepareDatafile1 ${workload_config} ${current_dir}
show_bannar start

CHAR_AMOUNT=10
REPETITION=10
ROW_SET_FILE=rowSet.tmp
ROW_SET_DIR=/tmp/
DFS_PATH=/tmp/

echo -e "${On_Blue}Rows:${STRUCT_STREAMING_KEY_NUMBER}${Color_Off}"

ROW_GEN_OPTION=" -kn ${STRUCT_STREAMING_KEY_NUMBER} \
                 -r ${REPETITION} \
                 -c ${CHAR_AMOUNT} \
                 -p ${ROW_SET_DIR}${ROW_SET_FILE}"

JVM_OPTS="-cp ${DATATOOLS} -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf "


CMD="${JAVA_BIN}  ${JVM_OPTS} HiBench.RowGenerator ${ROW_GEN_OPTION}"

execute_withlog $CMD

rmr_hdfs ${DFS_PATH}${ROW_SET_FILE}

upload_to_hdfs ${ROW_SET_DIR}${ROW_SET_FILE}  ${DFS_PATH}

rm ${ROW_SET_DIR}${ROW_SET_FILE}

rmr_hdfs $STREAMING_DATA_DIR || true
#echo -e "${On_Blue}Pages:${PAGES}, USERVISITS:${USERVISITS}${Color_Off}"

OPTION="-t struct \
        -b ${STREAMING_DATA_DIR} \
        -n ${STREAMING_DATA1_NAME} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -rm ${STRUCT_STREAMING_ROW_MULTIPLIER} \
        -f ${DFS_PATH}${ROW_SET_FILE}"

        START_TIME=`timestamp`
run_hadoop_job ${DATATOOLS} HiBench.DataGen ${OPTION}
END_TIME=`timestamp`
SIZE="0"

show_bannar finish
leave_bench





