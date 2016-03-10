package org.deeplearning4j.examples.nb15.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.CSVRecordReader;
import org.canova.api.split.FileSplit;
import org.deeplearning4j.datasets.canova.RecordReaderDataSetIterator;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**FromCsvReceiver:
 * Created by Alex on 10/03/2016.
 */
public class FromCsvReceiver extends Receiver<Tuple2<Long,INDArray>> {

    private static final Logger log = LoggerFactory.getLogger(FromCsvReceiver.class);

    private final String localCSVPath;
    private final int labelIdx;
    private final int nOut;
    private DataSetIterator iterator;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private final int examplesPerSecond;
    private final AtomicLong exampleCounter = new AtomicLong(0);

    public FromCsvReceiver(String localCSVPath, int labelIdx, int nOut, int examplesPerSecond) throws Exception {
        super(StorageLevel.MEMORY_ONLY());
        this.examplesPerSecond = examplesPerSecond;
        this.localCSVPath = localCSVPath;
        this.labelIdx = labelIdx;
        this.nOut = nOut;

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

        RecordReader rr = new CSVRecordReader(0,",");
        try{
            rr.initialize(new FileSplit(new File(localCSVPath)));
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        iterator = new RecordReaderDataSetIterator(rr,1,labelIdx,nOut);

        //Start a thread:
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(!stop.get() && iterator.hasNext()){

                    long start = System.currentTimeMillis();
                    for( int i=0; i<examplesPerSecond && iterator.hasNext(); i++ ){
                        DataSet ds = iterator.next();
                        long exampleNum = exampleCounter.incrementAndGet();

                        Tuple2<Long,INDArray> out = new Tuple2<Long, INDArray>(exampleNum,ds.getFeatures());
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

                log.info("Receiver shutting down: stop = {}, iterator.hasNext() = {}",stop.get(), iterator.hasNext());
            }
        }).start();
    }

    @Override
    public void onStop() {
        stop.set(true);
        log.info("Spark Streaming data receiver: onStop() called");
    }
}
