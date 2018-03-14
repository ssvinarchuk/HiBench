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
set -u

current_dir=`dirname "$0"`
root_dir=`cd "${current_dir}/.."; pwd`

. ${root_dir}/bin/functions/color.sh

MAPR_REPORT=${root_dir}/report/mapr-report.csv
MAPR_STREAMS_REPORT=${root_dir}/report/mapr-streams-report.csv

# Remove old MapR report
if [ -f ${MAPR_REPORT} ] ; then
    rm ${MAPR_REPORT}
fi

# Remove old MapR Streams report
if [ -f ${MAPR_STREAMS_REPORT} ] ; then
    rm ${MAPR_STREAMS_REPORT}
fi

for benchmark in `cat $root_dir/conf/mapr-spark-benchmarks.lst`; do
    if [[ $benchmark == \#* ]]; then
        continue
    fi

    echo -e "${UYellow}${BYellow}Prepare ${Yellow}${UYellow}${benchmark} ${BYellow}...${Color_Off}"
    benchmark="${benchmark/.//}"

    WORKLOAD=$root_dir/bin/workloads/${benchmark}

    # Non-streaming benchmark
    if [[ $benchmark != *"streaming"* ]] && [[ $benchmark != *"structuredStreaming"* ]]; then

        # Prepare
        echo -e "${BCyan}Exec script: ${Cyan}${WORKLOAD}/prepare/prepare.sh${Color_Off}"
        "${WORKLOAD}/prepare/prepare.sh"

        result=$?
        if [ $result -ne 0 ]
        then
        echo "ERROR: ${benchmark} prepare failed!"
            exit $result
        fi

        # Run benchmark
        echo -e "${UYellow}${BYellow}Run ${Yellow}${UYellow}${benchmark}/spark${Color_Off}"
        echo -e "${BCyan}Exec script: ${Cyan}$WORKLOAD/spark/run.sh${Color_Off}"
        $WORKLOAD/spark/run.sh

        result=$?
        if [ $result -ne 0 ]
        then
            echo -e "${On_IRed}ERROR: ${benchmark}/spark failed to run successfully.${Color_Off}"
                exit $result
        fi

    fi

    # Streaming benchmark
    if [[ $benchmark == *"streaming"* ]] || [[ $benchmark == *"structuredStreaming"* ]]; then

        # Seed dataset generation
        echo -e "${BCyan}Exec streaming seed dataset generator script: ${Cyan}${WORKLOAD}/prepare/genSeedDataset.sh${Color_Off}"
        "${WORKLOAD}/prepare/genSeedDataset.sh"

        result=$?
        if [ $result -ne 0 ]
        then
        echo "ERROR: ${benchmark} seed dataset generation failed!"
            exit $result
        fi

        # Start Streaming benchmark in background
        echo -e "${BCyan}Exec streaming script in background: ${Cyan}$WORKLOAD/spark/run.sh${Color_Off}"
        $WORKLOAD/spark/run.sh &
        STREAMING_PID=$!
        echo -e "${BCyan}Benchmark PID: ${Cyan}$STREAMING_PID${Color_Off}"

        # Wait Streaming benchmark to start
        # TODO change it
        sleep 90

        # Publishing records to MapR Stream
        echo -e "${BCyan}Exec records publishing script in background: ${Cyan}${WORKLOAD}/prepare/dataGen.sh${Color_Off}"
        $WORKLOAD/prepare/dataGen.sh &

        # Wait benchmark to complete
        GREP_STREAMING_PROC_NUM=$(ps -aux | awk '{print $2}' | grep ${STREAMING_PID} | wc -l)
        while [ ${GREP_STREAMING_PROC_NUM} -gt 0 ]
        do
            sleep 1
            GREP_STREAMING_PROC_NUM=$(ps -aux | awk '{print $2}' | grep ${STREAMING_PID} | wc -l)
        done

        # Find Data Generator PID
        GENERATOR_PID=$(ps -aux | grep -v 'python' | grep DataGenerator | grep java | awk '{print $2}')
        echo -e "${BCyan}Stop Data Generator with PID: ${Cyan}${GENERATOR_PID}${Color_Off}"

        # Stop Data Generator
        kill -9 ${GENERATOR_PID}

        echo -e "${UYellow}${BYellow}Streaming benchmark ${Yellow}${UYellow}${benchmark} ${BYellow} is done.${Color_Off}"

        # Generating metrics
        $WORKLOAD/common/metrics_reader.sh

    fi

    # found MapR specific script
	if [ -f ${WORKLOAD}/spark/runMaprDB.sh ]; then

        echo -e "${UYellow}${BYellow}Run MapR specific ${Yellow}${UYellow}${benchmark}/spark${Color_Off}"
        echo -e "${BCyan}Exec script: ${Cyan}$WORKLOAD/spark/runMaprDB.sh${Color_Off}"
        $WORKLOAD/spark/runMaprDB.sh

        result=$?
        if [ $result -ne 0 ]
        then
            echo -e "${On_IRed}ERROR: MapR specific ${benchmark}/spark failed to run successfully.${Color_Off}"
            exit $result
        fi

    fi

    # found verification script
	if [ -f ${WORKLOAD}/spark/verify.sh ]; then

        echo -e "${UYellow}${BYellow}Run verification specific ${Yellow}${UYellow}${benchmark}/spark${Color_Off}"
        echo -e "${BCyan}Exec script: ${Cyan}$WORKLOAD/spark/verify.sh${Color_Off}"
        $WORKLOAD/spark/verify.sh

        result=$?
        if [ $result -ne 0 ]
        then
            echo -e "${On_IRed}ERROR: Verification ${benchmark}/spark failed to run successfully.${Color_Off}"
            exit $result
        fi

    fi

done

echo "Run all done!"
echo "Report can be found at '${MAPR_REPORT}'"
echo "Streams report can be found at '${MAPR_STREAMS_REPORT}'"