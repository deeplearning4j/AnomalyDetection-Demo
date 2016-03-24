package org.deeplearning4j.examples;

/**
 *
 */
public class TrainMLPAuto {


    public static void main(String[] args) throws Exception {

        NIDSMain.main(
                "--dataSet", "NSLKDD",
                "--modelType", "MLPAuto",
                "--batchSize", "128",
                "--numBatches", "2000",
                "--numTestBatches", "500",
                "--numEpochs", "2",
                "--iterations", "1",
                "--nIn", "112",
                "--nOut", "40",
//                "--earlyStop",
                "--useHistogramListener"
//                "--saveModel"
        );
    }
}
