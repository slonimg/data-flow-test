#!/usr/bin/env bash

if [[ $1 = "local" ]]; then
    INPUT=$2
    OUTPUT=$3
    echo 'Running with direct runner'
    echo 'Input: pom.xml'
    echo "Output: $OUTPUT"
    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.CountWords \
     -Dexec.args="--inputFile=$INPUT --output=$OUTPUT" -Pdirect-runner
else
    PROJECT=$2 # <your-gcp-project>
    TMP=$3 # gs://<your-gcs-bucket>/tmp
    INPUT=$4 # gs://apache-beam-samples/shakespeare/*
    OUTPUT=$5 # gs://<your-gcs-bucket>/counts
    echo 'Running with dataflow runner.'
    echo "Google project id: $PROJECT"
    echo "Staging folder: $TMP"
    echo "Input: gs://apache-beam-samples/shakespeare/*"
    echo "Output: $OUTPUT"

    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.CountWords \
     -Dexec.args="--runner=DataflowRunner --project=$PROJECT \
                  --gcpTempLocation=$TMP \
                  --inputFile=$INPUT --output=$OUTPUT" \
     -Pdataflow-runner
fi