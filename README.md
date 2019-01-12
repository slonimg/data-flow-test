# data-flow-test

# Installation
```
git clone https://github.com/slonimg/data-flow-test.git
cd word-count-beam
chmod +x run.sh
```


## Run locally
```
./run.sh pom.xml counts
```

## Run via dataflow
```
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"

```
  
```
# ./run.sh <projectId> <stagingLocation> <input> <output>
# i.e:
./run.sh my-google-project gs://my-bucket/tmp gs://apache-beam-samples/shakespeare/* gs://my-bucket/counts
```
