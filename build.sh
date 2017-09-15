#!/usr/bin/env bash

HOSTNAME = hostname -i

case "$1" in
    all)
      mvn -Dspark=2.1 -Dscala=2.11 clean package
      shift 1;;
esac

cp conf/hadoop.conf.template conf/hadoop.conf
cp conf/spark.conf.template conf/spark.conf


 cat >> conf/spark.conf << EOM
hibench.masters.hostnames ${HOSTNAME}
hibench.slaves.hostnames ${HOSTNAME}
EOM
