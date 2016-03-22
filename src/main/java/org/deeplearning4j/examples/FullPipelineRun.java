package org.deeplearning4j.examples;

import org.deeplearning4j.examples.dataProcessing.PreprocessingPreSplit;
import org.deeplearning4j.examples.dataProcessing.SplitTrainTestRaw;

/**
 * Run to split, preprocess and train model.
 *
 * Uses dataSet UNSW_NB15, model MLP
 */
public class FullPipelineRun {

    public static void main(String... args) throws Exception {
        SplitTrainTestRaw.main("UNSW_NB15");
        PreprocessingPreSplit.main("UNSW_NB15");
        NIDSMain.main(
                "--dataSet", "UNSW_NB15",
                "--modelType", "MLP",
                "--batchSize", "128",
                "--numBatches", "20000",
                "--numTestBatches", "2500",
                "--numEpochs", "2",
                "--iterations", "1",
                "--nIn", "66",
                "--nOut", "10",
                "--saveModel"
        );
//        NIDSStreaming.main("UNSW_NB15");

    }
}
