#!/usr/bin/env bash

if [[ $3 = "" ]]; then
    INPUT=$1
    OUTPUT=$2
    echo 'Running with direct runner'
    echo 'Input: pom.xml'
    echo "Output: $OUTPUT"
    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.CountWords \
     -Dexec.args="--inputFile=$INPUT --output=$OUTPUT" -Pdirect-runner
else
    PROJECT=$1 # <your-gcp-project>
    STAGING=$2 # gs://<your-gcs-bucket>/staging
    INPUT=$3 # gs://apache-beam-samples/shakespeare/*
    OUTPUT=$4 # gs://<your-gcs-bucket>/counts
    echo 'Running with dataflow runner.'
    echo "Google project id: $PROJECT"
    echo "Staging folder: $STAGING"
    echo "Input: gs://apache-beam-samples/shakespeare/*"
    echo "Output: $OUTPUT"

    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.CountWords \
     -Dexec.args="--runner=DataflowRunner --project=$PROJECT \
                  --stagingLocation=$STAGING \
                  --inputFile=$INPUT --output=$OUTPUT" \
     -Pdataflow-runner
fi