CURRDIR=$(pwd)
CLASS="org.deeplearning4j.examples.LaunchProcessing"
MASTER="local[*]"
ADDITIONALARGS="--num-executors 1 --executor-cores 12 --executor-memory 1024M"
JARLOC="$CURRDIR/AnomalyDetection-Demo-1.0.jar"
echo "spark-submit --class $CLASS --master $MASTER $JARLOC"
java -cp AnomalyDetection-Demo-1.0.jar org.deeplearning4j.examples.LaunchDropWizard & /usr/lib/spark/bin/spark-submit --class $CLASS --master $MASTER $ADDITIONALARGS $JARLOC && fg