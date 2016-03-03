package org.deeplearning4j.examples;

import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;

/**
 */

public class StandardNIDS extends NIDSMain{

    protected MultipleEpochsIterator loadData(int batchSize, int totalNumExamples, String mode) {
        System.out.println("Load data....");
        // TODO setup data load
//        return new MultipleEpochsIterator(numEpochs,...);
        return null;
    }

    protected MultiLayerNetwork trainModel(MultiLayerNetwork model, MultipleEpochsIterator data){
        System.out.println("Train model....");
        startTime = System.currentTimeMillis();
        model.fit(data);
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return model;
    }

    protected void evaluatePerformance(MultiLayerNetwork model, MultipleEpochsIterator iter){
        System.out.println("Evaluate model....");

        startTime = System.currentTimeMillis();
        Evaluation eval = model.evaluate(iter, labels);
        endTime = System.currentTimeMillis();
        System.out.println(eval.stats());
        System.out.println("****************************************************");
        testTime = (int) (endTime - startTime) / 60000;

    }

}
