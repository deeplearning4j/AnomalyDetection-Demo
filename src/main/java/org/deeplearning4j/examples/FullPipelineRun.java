package org.deeplearning4j.examples;


/**
 * Run to split, preprocess and train model.
 *
 * Uses dataSet UNSW_NB15, model MLP
 */
public class FullPipelineRun {

    public static void main(String... args) throws Exception {
        NIDSMain.main(
                "--preProcess",
                "--dataSet", "UNSW_NB15",
                "--modelType", "MLP",
                "--batchSize", "128",
                "--numBatches", "20",
                "--numTestBatches", "2",
                "--numEpochs", "2",
                "--iterations", "1",
                "--nIn", "66",
                "--nOut", "10",
//                "--dataSet", "NSLKDD",
//                "--nIn", "112",
//                "--nOut", "40",
                "--saveModel"
        );
//        NIDSStreaming.main("UNSW_NB15");

    }
}
