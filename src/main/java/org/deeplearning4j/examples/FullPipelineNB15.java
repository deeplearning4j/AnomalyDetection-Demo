package org.deeplearning4j.examples;



public class FullPipelineNB15 {

    public static void main(String... args) throws Exception {
        NIDSMain.main(
                "--preProcess",
                "--modelType", "MLP",
                "--batchSize", "128",
                "--iterations", "1",
                "--dataSet", "UNSW_NB15",
                "--numBatches", "10000",
                "--nIn", "66",
                "--nOut", "10",
                "--saveModel"
        );
    }
}
