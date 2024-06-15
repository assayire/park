#!/usr/bin/env bash

avro_jar_file='avro-tools.jar'
avro_jar_url='https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.3/avro-tools-1.11.3.jar'

if [ ! -f "$avro_jar_file" ]; then
  echo >&2 "Downloading avro-tools JAR from maven ..."
  curl -o "$avro_jar_file" "$avro_jar_url"
fi

java -jar "$avro_jar_file" \
  compile schema \
  src/main/resources/organizations.avsc \
  build/generated
