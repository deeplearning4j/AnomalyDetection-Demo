package org.deeplearning4j.examples;

import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.deeplearning4j.datasets.canova.RecordReaderDataSetIterator;
import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.datasets.iterator.MultipleEpochsIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;

import java.io.File;

/**
 */

public class StandardNIDS extends NIDSMain{

    protected MultipleEpochsIterator loadData(int batchSize, String dataPath, int labelIdx, int numEpochs) throws Exception{
        System.out.println("Load data....");
        CSVRecordReader rr = new CSVRecordReader(0,",");
        rr.initialize(new FileSplit(new File(dataPath)));
        DataSetIterator iter = new RecordReaderDataSetIterator(rr, batchSize, labelIdx , nOut);
        return new MultipleEpochsIterator(numEpochs, iter);

    }

    protected MultiLayerNetwork trainModel(MultiLayerNetwork net, MultipleEpochsIterator iter){
        System.out.println("Train model....");
        startTime = System.currentTimeMillis();
        int countTrain = 0;
        net.fit(iter);
        if(countTrain % TEST_EVERY_N_MINIBATCHES == 0){
            iter.reset();
            //Test:
            evaluatePerformance(net, iter);
        }
        if(!iter.hasNext()) iter.reset();
        endTime = System.currentTimeMillis();
        trainTime = (int) (endTime - startTime) / 60000;
        return net;
    }

    protected void evaluatePerformance(MultiLayerNetwork net, MultipleEpochsIterator iter){
        System.out.println("Evaluate model....");
        startTime = System.currentTimeMillis();
        Evaluation eval = net.evaluate(iter, labels);
        endTime = System.currentTimeMillis();
        System.out.println(eval.stats());
        System.out.println("****************************************************");
        testTime = (int) (endTime - startTime) / 60000;

    }

}
