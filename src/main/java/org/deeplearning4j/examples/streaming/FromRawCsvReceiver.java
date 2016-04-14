package org.deeplearning4j.examples.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.canova.api.io.WritableConverter;
import org.canova.api.io.converters.WritableConverterException;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.writable.Writable;
import org.deeplearning4j.preprocessing.api.TransformProcess;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.util.FeatureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**FromRawCsvReceiver: goes from raw test data -> preprocessed -> normalized -> INDArray
 * Idea is to keep the raw data around so we can eventually plot it in the UI
 */
public class FromRawCsvReceiver extends Receiver<Tuple3<Long,INDArray,List<Writable>>> {

    private static final Logger log = LoggerFactory.getLogger(FromRawCsvReceiver.class);

//    private final String localCSVPath;
    private final TransformProcess preprocess;
    private final TransformProcess normalizer;
    private final int labelIndex;
    private final int numPossibleLabels;
    private RecordReader rr;
//    private DataSetIterator iterator;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private final int examplesPerSecond;
    private final AtomicLong exampleCounter = new AtomicLong(0);

    private WritableConverter converter = null;
    private boolean regression = false;

    private File dataFile;

    public FromRawCsvReceiver(File dataFile, TransformProcess preprocess, TransformProcess normalizer,
                               int labelIdx, int nOut, int examplesPerSecond) throws Exception {
        super(StorageLevel.MEMORY_ONLY());
        this.preprocess = preprocess;
        this.normalizer = normalizer;
        this.examplesPerSecond = examplesPerSecond;
        this.labelIndex = labelIdx;
        this.numPossibleLabels = nOut;

        this.dataFile = dataFile;
    }

    public FromRawCsvReceiver(String localCSVPath, TransformProcess preprocess, TransformProcess normalizer,
                              int labelIdx, int nOut, int examplesPerSecond) throws Exception {
        super(StorageLevel.MEMORY_ONLY());
        this.preprocess = preprocess;
        this.normalizer = normalizer;
        this.examplesPerSecond = examplesPerSecond;
//        this.localCSVPath = localCSVPath;
        this.labelIndex = labelIdx;
        this.numPossibleLabels = nOut;

        this.dataFile = new File(localCSVPath);

//        RecordReader rr = new CSVRecordReader(0,",");
//        rr.initialize(new FileSplit(new File(localCSVPath)));
//        iterator = new RecordReaderDataSetIterator(rr,1,labelIdx,nOut);


    }

    @Override
    public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY();
    }

    @Override
    public void onStart() {
        log.info("Starting Spark Streaming data receiver...");

        rr = new CSVRecordReader(0,",");
        try{
//            rr.initialize(new FileSplit(new File(localCSVPath)));
            rr.initialize(new FileSplit(dataFile));
        }catch(Exception e){
            throw new RuntimeException(e);
        }
//        iterator = new RecordReaderDataSetIterator(rr,1,labelIdx,nOut);

        //Start a thread:
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while(!stop.get() && rr.hasNext()){

                    long start = System.currentTimeMillis();
                    for( int i=0; i<examplesPerSecond && rr.hasNext(); i++ ){
                        long exampleNum = exampleCounter.incrementAndGet();

                        //Load the raw data:
                        Collection<Writable> raw = rr.next();
                        List<Writable> rawList = (raw instanceof List ? ((List<Writable>)raw) : new ArrayList<>(raw));

                        //Do preprocessing, then normalization:
                        List<Writable> preproc = preprocess.execute(rawList);
                        if(preproc == null) continue;   //may be null if filtered
                        List<Writable> norm = normalizer.execute(preproc);

                        DataSet ds = getDataSet(norm);

                        Tuple3<Long,INDArray,List<Writable>> out = new Tuple3<>(exampleNum,ds.getFeatures(),rawList);
                        store(out);
                    }
                    long end = System.currentTimeMillis();

                    long sleepTime = 1000 - (end-start);
                    if(sleepTime > 0){
                        try{
                            Thread.sleep(sleepTime);
                        }catch(InterruptedException e){
                            log.error("Interrupted exception thrown in receiver thread",e);
                            throw new RuntimeException(e);
                        }
                    }

                }

                log.info("Receiver shutting down: stop = {}, iterator.hasNext() = {}",stop.get(), rr.hasNext());
            }
        });
        t.setDaemon(true);
        t.start();

    }

    @Override
    public void onStop() {
        stop.set(true);
        log.info("Spark Streaming data receiver: onStop() called");
    }


    //Method taken from RecordReaderDataSetIterator
    private DataSet getDataSet(List<Writable> record) {

//        //allow people to specify label index as -1 and infer the last possible label
//        if (numPossibleLabels >= 1 && labelIndex < 0) {
//            labelIndex = record.size() - 1;
//        }

        INDArray label = null;
        INDArray featureVector = Nd4j.create(labelIndex >= 0 ? record.size()-1 : record.size());
        int featureCount = 0;
        for (int j = 0; j < record.size(); j++) {
            Writable current = record.get(j);
            if (current.toString().isEmpty())
                continue;
            if (labelIndex >= 0 && j == labelIndex) {
                if (converter != null)
                    try {
                        current = converter.convert(current);
                    } catch (WritableConverterException e) {
                        e.printStackTrace();
                    }
                if (numPossibleLabels < 1)
                    throw new IllegalStateException("Number of possible labels invalid, must be >= 1");
                if (regression) {
                    label = Nd4j.scalar(current.toDouble());
                } else {
                    int curr = current.toInt();
                    if (curr >= numPossibleLabels)
                        curr--;
                    label = FeatureUtil.toOutcomeVector(curr, numPossibleLabels);
                }
            } else {
                featureVector.putScalar(featureCount++, current.toDouble());
            }
        }

        return new DataSet(featureVector, labelIndex >= 0 ? label : featureVector);
    }
}
