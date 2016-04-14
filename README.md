#AnomalyDetection-Demo
Demo repo for anomaly detection using NIDS (network intrustion detection) data


## Instructions for running demo using spark-submit:

1. Extract data (test.csv) from /src/main/resources/data_test.zip (too large to upload to github unzipped)
2. Build a jar file using "mvn package"
3. Modify the launch script launch.sh to use correct spark master
4. Run launch.sh



Some things of note:

- Trained network, data and other required files are in /src/main/resources/ directory and will be included in the fat jar built using "mvn package"
- UI (dropwizard) ports can be configured in /src/main/resources/nids-dropwizard.yml. Default port: 8080