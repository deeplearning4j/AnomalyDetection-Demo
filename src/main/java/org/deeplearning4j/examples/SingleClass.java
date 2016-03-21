package org.deeplearning4j.examples;

import org.deeplearning4j.examples.TrainMLP;
import org.deeplearning4j.examples.dataProcessing.PreprocessingPreSplit;
import org.deeplearning4j.examples.dataProcessing.SplitTrainTestRaw;

/**
 * Run to split, preprocess and train model.
 */
public class SingleClass {

    public static void main(String... args) throws Exception {
        SplitTrainTestRaw.main("UNSW_NB15");
        PreprocessingPreSplit.main("UNSW_NB15");
        TrainMLP.main("UNSW_NB15"); // TODO chagne to NIDSMAIN

    }
}
