package org.deeplearning4j.examples;

import org.apache.commons.io.FilenameUtils;

import java.io.File;

/**
 *
 */
public class DataPath {

    public static boolean WIN = System.getProperty("os.name").toLowerCase().contains("win");
    public static final String WIN_DIR = "C:/Data/";
    public static final String MAC_DIR = FilenameUtils.concat(System.getProperty("user.home"), "data/NIDS/");

    protected static String rawFilePath = WIN? "labeled_flows_xml/" : "raw/";
    protected static String inputFilePath = WIN? "CSV/" : "input/";
    protected static String preprocessedFilePath = WIN? "Out/": "preprocessed/";
    protected static String outputFilePath = WIN? "Out/": "out/";
    protected static String chartFilePath = "Out/Charts/";
    protected static String rawTrainTestSplit = "rawTrainTest/";
    protected final String dataSet;

    public final String DATA_BASE_DIR;
    public final String RAW_DIR;
    public final String RAW_TRAIN_TEST_SPLIT_DIR;
    public final String IN_DIR;
    public final String PRE_DIR;
    public final String OUT_DIR;
    public final String CHART_DIR_ORIG;
    public final String CHART_DIR_NORM;
    public final String NORMALIZER_FILE;
    public final String RAW_TRAIN_PATH;
    public final String RAW_TEST_PATH;

    public static final String REPO_BASE_DIR = FilenameUtils.concat(System.getProperty("user.dir"), "src/main/resources/");
    public static final String TRAIN_DATA_PATH = FilenameUtils.concat(REPO_BASE_DIR, "train/");
    public static final String TEST_DATA_PATH = FilenameUtils.concat(REPO_BASE_DIR, "test/");

    public static final boolean AWS = false;
    protected static String S3_BUCKET = "anomaly-data";
    // TODO add data name to path
    protected static String S3_KEY_PREFIX_IN = "nids/input/";
    protected static String S3_KEY_PREFIX_OUT = "nids/preprocessed/";


    public DataPath(String dataSet){
        this.dataSet = dataSet;
        this.DATA_BASE_DIR = WIN ? FilenameUtils.concat(WIN_DIR, dataSet) : FilenameUtils.concat(MAC_DIR, dataSet);
        this.RAW_DIR = FilenameUtils.concat(DATA_BASE_DIR, rawFilePath);
        this.RAW_TRAIN_TEST_SPLIT_DIR = FilenameUtils.concat(DATA_BASE_DIR, rawTrainTestSplit);
        this.IN_DIR = FilenameUtils.concat(DATA_BASE_DIR, inputFilePath);
        this.PRE_DIR = FilenameUtils.concat(DATA_BASE_DIR, preprocessedFilePath);
        this.OUT_DIR = FilenameUtils.concat(DATA_BASE_DIR, outputFilePath);
        this.CHART_DIR_ORIG = FilenameUtils.concat(DATA_BASE_DIR, chartFilePath + "Orig/");
        this.CHART_DIR_NORM = FilenameUtils.concat(DATA_BASE_DIR,  chartFilePath + "Norm/");
        this.NORMALIZER_FILE = FilenameUtils.concat(OUT_DIR, "normalizerTransform.bin");

        this.RAW_TRAIN_PATH = FilenameUtils.concat(RAW_TRAIN_TEST_SPLIT_DIR,"train.csv");
        this.RAW_TEST_PATH = FilenameUtils.concat(RAW_TRAIN_TEST_SPLIT_DIR,"test.csv");

        if(!new File(IN_DIR).exists()) new File(IN_DIR).mkdirs();
        if(!new File(PRE_DIR).exists()) new File(PRE_DIR).mkdirs();
        if(!new File(OUT_DIR).exists()) new File(OUT_DIR).mkdirs();
        if(!new File(CHART_DIR_ORIG).exists()) new File(CHART_DIR_ORIG).mkdirs();
        if(!new File(CHART_DIR_NORM).exists()) new File(CHART_DIR_NORM).mkdirs();
    }


}
