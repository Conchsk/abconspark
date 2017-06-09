package cn.wyj.abconspark.clustering.distribute;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class ClusteringDTMain {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("spark://J106-WYJ-UBT:7077")
				.set("spark.executor.memory", "2g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), "hdfs://localhost:9000/kddcup_mm.ls")
				.toJavaRDD().cache();

		int classNum = 3;
		int swarmSize = 50;
		int maxCycle = 10;
		int trialLimit = 100;
		double mistakeRate = 0.05;

		ClusteringDTHive hive = new ClusteringDTHive(data, classNum, swarmSize,
				maxCycle, trialLimit, mistakeRate);
		hive.solve(sc);
		hive.show();
		//hive.predict(data);
		sc.close();
	}
}
