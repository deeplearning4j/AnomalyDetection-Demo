package org.deeplearning4j.examples;


import org.deeplearning4j.examples.datasets.nb15.NB15Util;

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
//                "--iterations", "1",
//                "--dataSet", "UNSW_NB15",
//                "--numBatches", "10000",
                "--numEpochs", "60",
//                "--nIn", "66",
//                "--nOut", "10",
                "--dataSet", "NSLKDD",
                "--nIn", "112",
                "--nOut", "40",
                "--numBatches", "200",
                "--numTestBatches", "100",
//                "--useHistogramListener",
                "--earlyStop"
//                "--saveModel"
        );
//        NIDSStreaming.main("UNSW_NB15");

        //Testing for new UI:
//        new NB15StreamingUI(NB15Util.getCsvSchema());
//        NIDSStreamingNew.main("UNSW_NB15");

    }
}
