package cn.wyj.abconspark.clustering.distribute;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class ClusteringDTMain {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("spark://spark-1:7077")
				.set("spark.cores.max", args[0])
				.set("spark.executor.memory", args[1]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), "hdfs://spark-1:9000/user/wyj/kddcup.ls")
				.toJavaRDD().map(new Function<LabeledPoint, LabeledPoint>() {
					private static final long serialVersionUID = 1L;
					@Override
					public LabeledPoint call(LabeledPoint v) throws Exception {
						return new LabeledPoint(v.label(),
								new SparseVector(2, new int[] { 1, 2 },
								new double[] { v.features().apply(0), v.features().apply(1) }));
					}
				});

		int classNum = 3;
		int swarmSize = 500;
		int maxCycle = 21;
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
