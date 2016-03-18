package org.deeplearning4j.examples.utils;

import org.apache.commons.io.FilenameUtils;

import java.io.File;

/**
 *
 */
public class DataPathUtil {

    public static boolean WIN = System.getProperty("os.name").toLowerCase().contains("win");
    public static final String WIN_DIR = "C:/Data/";
    public static final String MAC_DIR = FilenameUtils.concat(System.getProperty("user.home"), "data/NIDS/");

    protected static String rawFilePath = WIN? "labeled_flows_xml/" : "raw/";
    protected static String rawTrainTestSplit = "rawTrainTest/";

    protected static String inputFilePath = "input/";
    protected static String preprocessedFilePath = "preprocessed/";
    protected static String outputFilePath = "out/";
    protected static String chartFilePath = FilenameUtils.concat(outputFilePath,"/charts/");
    protected static String dataSet;

    public final String DATA_BASE_DIR;
    public final String IN_DIR;
    public final String OUT_DIR;
    public final String CHART_DIR_ORIG;
    public final String CHART_DIR_NORM;

    public final String RAW_DIR;
    public final String RAW_TRAIN_DATA_DIR;
    public final String RAW_TEST_DATA_DIR;
    public final String RAW_TRAIN_FILE;
    public final String RAW_TEST_FILE;
    public final String PRE_DIR;
    public final String PRE_TRAIN_DATA_DIR;
    public final String PRE_TEST_DATA_DIR;
    public final String TRAIN_DATA_FILE;
    public final String TEST_DATA_FILE;
    public final String NORMALIZER_FILE;
    public final String NORM_SCHEMA;
    public final String NORM_TRAIN_DATA_FILE;
    public final String NORM_TEST_DATA_FILE;
    public final String NETWORK_CONFIG_FILE;
    public final String NETWORK_PARAMS_FILE;


    public static final String REPO_BASE_DIR = FilenameUtils.concat(System.getProperty("user.dir"), "src/main/resources/");


    public static final boolean AWS = false;
    protected static String S3_BUCKET = "anomaly-data";
    // TODO add data name to path
    protected static String S3_KEY_PREFIX_IN = "nids/input/";
    protected static String S3_KEY_PREFIX_OUT = "nids/preprocessed/";


    public DataPathUtil(String dataSet){
        this.dataSet = dataSet;
        this.DATA_BASE_DIR = WIN ? FilenameUtils.concat(WIN_DIR, dataSet) : FilenameUtils.concat(MAC_DIR, dataSet);

        this.RAW_DIR = FilenameUtils.concat(DATA_BASE_DIR, rawFilePath);
        this.IN_DIR = FilenameUtils.concat(DATA_BASE_DIR, inputFilePath);
        this.PRE_DIR = FilenameUtils.concat(DATA_BASE_DIR, preprocessedFilePath);
        this.OUT_DIR = FilenameUtils.concat(DATA_BASE_DIR, outputFilePath);

        this.CHART_DIR_ORIG = FilenameUtils.concat(DATA_BASE_DIR, chartFilePath + "Orig/");
        this.CHART_DIR_NORM = FilenameUtils.concat(DATA_BASE_DIR,  chartFilePath + "Norm/");

        // for raw data splits
        this.RAW_TRAIN_DATA_DIR = FilenameUtils.concat(RAW_DIR, "train/");
        this.RAW_TEST_DATA_DIR = FilenameUtils.concat(RAW_DIR, "test/");
        this.RAW_TRAIN_FILE = FilenameUtils.concat(RAW_TRAIN_DATA_DIR,"train.csv");
        this.RAW_TEST_FILE = FilenameUtils.concat(RAW_TEST_DATA_DIR,"test.csv");

        // for preprocessed data splits
        this.PRE_TRAIN_DATA_DIR = FilenameUtils.concat(PRE_DIR, "train/");
        this.PRE_TEST_DATA_DIR = FilenameUtils.concat(PRE_DIR, "test/");
        this.TRAIN_DATA_FILE = FilenameUtils.concat(PRE_TRAIN_DATA_DIR,"train.csv");
        this.TEST_DATA_FILE = FilenameUtils.concat(PRE_TRAIN_DATA_DIR,"test.csv");

        // for normalized files
        this.NORMALIZER_FILE = FilenameUtils.concat(PRE_DIR, "normalizerTransform.bin");
        this.NORM_SCHEMA = FilenameUtils.concat(PRE_DIR, "normalizedDataSchema.txt");
        this.NORM_TRAIN_DATA_FILE = FilenameUtils.concat(PRE_DIR, "0"+dataSet+"normalized0.csv");
        this.NORM_TEST_DATA_FILE = FilenameUtils.concat(PRE_DIR, "1"+dataSet+"normalized0.csv");

        // for saving model and params
        this.NETWORK_CONFIG_FILE = FilenameUtils.concat(OUT_DIR,"config.json");
        this.NETWORK_PARAMS_FILE = FilenameUtils.concat(OUT_DIR,"params.bin");

        // setup folders if the do not exist
        if(!new File(IN_DIR).exists()) new File(IN_DIR).mkdirs();
        if(!new File(PRE_DIR).exists()) new File(PRE_DIR).mkdirs();
        if(!new File(RAW_TRAIN_DATA_DIR).exists()) new File(RAW_TRAIN_DATA_DIR).mkdirs();
        if(!new File(RAW_TEST_DATA_DIR).exists()) new File(RAW_TEST_DATA_DIR).mkdirs();
        if(!new File(PRE_TRAIN_DATA_DIR).exists()) new File(PRE_TRAIN_DATA_DIR).mkdirs();
        if(!new File(PRE_TEST_DATA_DIR).exists()) new File(PRE_TEST_DATA_DIR).mkdirs();
        if(!new File(OUT_DIR).exists()) new File(OUT_DIR).mkdirs();
        if(!new File(CHART_DIR_ORIG).exists()) new File(CHART_DIR_ORIG).mkdirs();
        if(!new File(CHART_DIR_NORM).exists()) new File(CHART_DIR_NORM).mkdirs();
    }


}
