package org.deeplearning4j.examples;

import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.deeplearning4j.datasets.canova.RecordReaderDataSetIterator;
import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.dataset.api.DataSet;

import java.io.File;

/**
 */

public class StandardNIDS extends NIDSMain{

    protected MultipleEpochsIterator loadData(int batchSize, String dataPath, int labelIdx, int numEpochs, int numBatches) throws Exception{
        CSVRecordReader rr = new CSVRecordReader(0,",");
        rr.initialize(new FileSplit(new File(dataPath)));
        DataSetIterator iter = new RecordReaderDataSetIterator(rr, batchSize, labelIdx , nOut);
//        DataSetIterator iter = new RecordReaderDataSetIterator(rr, batchSize, labelIdx , nOut, numBatches); 3.9
        return new MultipleEpochsIterator(numEpochs, iter);

    }

    protected MultiLayerNetwork trainModel(MultiLayerNetwork net, MultipleEpochsIterator iter, MultipleEpochsIterator testIter){
        DataSet next;
        System.out.println("Train model....");
        startTime = System.currentTimeMillis();
        int countTrain = 0;

        while(iter.hasNext()) {
            countTrain++;
            next = iter.next();
            if(next == null) break;
            net.fit(next);
            if (countTrain % TEST_EVERY_N_MINIBATCHES == 0) {
                //Test:
                log.info("--- Evaluation after {} examples ---",countTrain*batchSize);
                evaluatePerformance(net, testIter);
                testIter.reset();
            }
        }
        if(!iter.hasNext()) iter.reset();
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return net;
    }

    protected void evaluatePerformance(MultiLayerNetwork net, MultipleEpochsIterator iter){
        startTime = System.currentTimeMillis();
//        Evaluation eval = net.evaluate(iter, labels); 3.9
        Evaluation eval = new Evaluation(labels);
        int countEval = 0;
        while(iter.hasNext() && countEval++ < testBatchSize){
            org.nd4j.linalg.dataset.DataSet ds = iter.next();
            eval.eval(ds.getLabels(),net.output(ds.getFeatureMatrix()));
        }

        endTime = System.currentTimeMillis();
        System.out.println(eval.stats());
//        System.out.print("False Alarm Rate: " + eval.falseAlarmRate()); 3.9
        testTime = (int) (endTime - startTime) / 60000;

    }


}
