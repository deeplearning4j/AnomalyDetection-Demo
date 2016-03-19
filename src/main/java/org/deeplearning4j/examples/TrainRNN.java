package org.deeplearning4j.examples;

/**
 * Created by Alex on 19/03/2016.
 */
public class TrainRNN {

    public static void main(String[] args) throws Exception {

        NIDSMain.main(
                "--dataSet", "UNSW_NB15",
                "--modelType", "RNN",
                "--batchSize", "64",
                "--numEpochs", "1",
                "--nIn", "66",
                "--nOut", "10",
                "-tBPTT", "100",
                "--useHistogramListener"
        );

    }

}
