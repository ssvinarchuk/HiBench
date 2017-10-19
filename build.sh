#!/usr/bin/env bash

case "$1" in
    spark)
      mvn -P sql,sparkbench,websearch,micro,streaming,spark2.1,ml,graph,scala2.11,!defaultSpark,!defaultScalaVersion,!allModules clean install
      shift 1;;
esac