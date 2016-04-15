CURRDIR=$(pwd)
CLASS="org.deeplearning4j.examples.StreamingDemoEntryPoint2"
MASTER="local[*]"
ADDITIONALARGS="--num-executors 1 --executor-cores 12 --executor-memory 1024M"
JARLOC="$CURRDIR/AnomalyDetection-Demo-1.0.jar"
echo "spark-submit --class $CLASS --master $MASTER $JARLOC"
spark-submit --class $CLASS --master $MASTER $ADDITIONALARGS $JARLOC