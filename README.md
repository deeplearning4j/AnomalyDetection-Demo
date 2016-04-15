#AnomalyDetection-Demo
Demo repo for anomaly detection using NIDS (network intrustion detection) data. Model trained to monitor and identify unauthorized, illicit and anomalous network behavior. 

Setup for static and streaming solutions. The streaming model collects packets that traverse a given network, applies a trained feedforward network and flags suspicious traffic.

## Instructions for running demo using spark-submit:

1. Extract data (test.csv) from /src/main/resources/data_test.zip (too large to upload to github unzipped)
2. Build a jar file using "mvn package"
3. Modify the launch script launch.sh to use correct spark master
4. Run launch.sh


Some things of note:
- Trained network, data and other required files are in /src/main/resources/ directory and will be included in the fat jar built using "mvn package"
- UI (dropwizard) ports can be configured in /src/main/resources/nids-dropwizard.yml. Default port: 8080 

## Models: 

- MLP | Feedforward (currently used for streaming)
- RNN
- AutoEncoder
- MLP simulated AutoEncoder

Streaming uses a feedforward network:

## Datasets 

- UNSW NB-15 = main dataset used in the project especially for streaming
  - Cyber Range Lab of the Australian Cyber Security 
  - Hybrid of normal activities and synthetic contemporary attack behaviors using the IXIA tool
  - 3 networks
  - 45 IP addresses
  - 16 hours data collection
  - 49 features
  - 2.5M records
  - Includes 9 core attack families with training dataset breakdown as follows:
    - Normal 56K 
    - Analysis 2K
    - Backdoor 1.7K
    - DoS 12K
    - Exploits 33K
    - Fuzzers 18K
    - Generic 40K
    - Reconnaissance 10K
    - Shellcode 1K
    - Worms 130
  - ISCX
  - NSL-KDD
    
