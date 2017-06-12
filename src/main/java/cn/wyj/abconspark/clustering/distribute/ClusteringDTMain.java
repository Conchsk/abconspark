package cn.wyj.abconspark.clustering.distribute;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class ClusteringDTMain {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("spark://ubuntu1:7077")
				.set("spark.executor.cores", args[0])
				.set("spark.executor.memory", "2g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), "hdfs://ubuntu1:9000/kddcup.ls")
				.toJavaRDD().map(v -> {
					return new LabeledPoint(v.label(),
							new SparseVector(2, new int[] { 1, 2 },
							new double[] { v.features().apply(0), v.features().apply(1) }));
				}).cache();

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
