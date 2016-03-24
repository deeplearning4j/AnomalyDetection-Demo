package org.deeplearning4j.examples;

/**
 *
 */
public class TrainAutoEncoder {


    public static void main(String[] args) throws Exception {

        NIDSMain.main(
                "--dataSet", "NSLKDD",
                "--modelType", "Denoise",
                "--batchSize", "100",
                "--numBatches", "100",
                "--numTestBatches", "100",
                "--iterations", "1",
                "--numEpochs", "2",
                "--nIn", "112",
                "--nOut", "40",
                "--useHistogramListener"
        );
    }
}
