package cn.wyj.abconspark.clustering.distribute;

import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ClusteringDTMain {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("test").master("spark://127.0.0.1:7077").getOrCreate();
		Dataset<LabeledPoint> data = spark.read().format("libsvm").load("hdfs://127.0.0.1:9000/kddcup/kddcup.csv")
				.as(Encoders.bean(LabeledPoint.class));
		
		int classNum = 3;
		int swarmSize = 50;
		int maxCycle = 10;
		int trialLimit = 100;
		double mistakeRate = 0.05;
		
		ClusteringDTHive hive = new ClusteringDTHive(data, classNum, swarmSize, maxCycle, trialLimit, mistakeRate);
		hive.solve();
		hive.show();
		
		spark.close();
	}
}
