package org.deeplearning4j.examples;


/**
 * Run to split, preprocess and train model.
 * Uncomment streaming if you want streaming to kick off after training
 *
 */
public class FullPipelineRun {

    public static void main(String... args) throws Exception {
        NIDSMain.main(
//                "--preProcess",
                "--modelType", "MLP",
                "--batchSize", "128",
                "--numEpochs", "1",
//                "--iterations", "1",
                "--dataSet", "UNSW_NB15",
                "--numBatches", "5000",
//                "--nIn", "66",
//                "--nOut", "10",
//                "--dataSet", "NSLKDD",
//                "--nIn", "112",
//                "--nOut", "40",
//                "--numBatches", "200",
                "--numTestBatches", "1000",
//                "--useHistogramListener",
                "--saveModel"
        );
//        NIDSStreaming.main("UNSW_NB15");

    }
}
