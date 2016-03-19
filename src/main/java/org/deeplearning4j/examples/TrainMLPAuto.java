package org.deeplearning4j.examples;

/**
 *
 */
public class TrainMLPAuto {


    public static void main(String... args) throws Exception {
// Ideas for initial values
//        new int[]{nIn,50,200,300,200,50},
//                new int[]{50,200,300,200,50,nOut},
//                iterations,
//                "leakyrelu",
//                WeightInit.RELU,
//                1e-4,
//                1

        NIDSMain.main(
                "--dataSet", "NSLKDD",
                "--modelType", "MLPAuto",
                "--batchSize", "128",
                "--numBatches", "20",
                "--numEpochs", "2",
                "--iterations", "1",
                "--nIn", "112",
                "--nOut", "40",
                "--useHistogramListener"
        );
    }
}
