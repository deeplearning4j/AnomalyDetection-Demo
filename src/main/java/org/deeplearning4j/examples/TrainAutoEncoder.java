package org.deeplearning4j.examples;

/**
 *
 */
public class TrainAutoEncoder {


    public static void main(String... args) throws Exception {
// Ideas for initial values
//                        new int[]{nIn, 500, 100},
//                        new int[]{500, 100, nOut},
//                        iterations,
//                        "relu",
//                        WeightInit.XAVIER,
//                        1e-1,
//                        1e-3

        NIDSMain.main(
                "--dataSet", "NSLKDD",
                "--modelType", "Denoise",
                "--batchSize", "100",
                "--numBatches", "100",
                "--numEpochs", "2",
                "--iterations", "1",
                "--nIn", "112",
                "--nOut", "40",
                "--useHistogramListener"
        );
    }
}
