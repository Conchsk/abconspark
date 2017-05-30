package cn.wyj.abconspark;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class KDDMain {
	public static void main(String[] args) throws FileNotFoundException {
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("spark://J106-WYJ-UBT:7077").setJars(new String[] {
						"/home/wyj/Softwares/spark-2.1.1-bin-hadoop2.7/workspace/test.jar" });
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<LabeledPoint> rdd = MLUtils
				.loadLibSVMFile(sc.sc(), "/home/wyj/Documents/kddcup/kddcup.data")
				.toJavaRDD().cache();
		rdd.map(v -> {
			StringBuilder sb = new StringBuilder();
			sb.append(v.label() + ",");
			for (int i = 0; i < 41; ++i)
				sb.append(v.features().apply(i) + ",");
			String ret = sb.toString().replaceAll(".0,", ",");
			ret = ret.substring(0, ret.length() - 1);
			return ret;
		}).saveAsTextFile("/home/wyj/Documents/kddcup/kddcupcsv");
		sc.close();
		
//		JavaRDD<LabeledPoint> rdd = MLUtils
//				.loadLibSVMFile(sc.sc(), "/home/wyj/Documents/wineSVM.data_mm")
//				.toJavaRDD().cache();
//		JavaRDD<LabeledPoint> rdd = sc.textFile("/home/wyj/Documents/winePCA.csv")
//				.map(v -> {
//					String[] params = v.split(",");
//					double label = Double.parseDouble(params[0]);
//					double[] features = new double[5];
//					for (int i = 0; i < 5; ++i)
//						features[i] = Double.parseDouble(params[i + 1]);
//					return new LabeledPoint(label, (Vectors.dense(features)));
//				});
//		double[][] data = new double[178][13];
//		Scanner scanner = new Scanner(new File("/home/wyj/Documents/winePCA.csv"));
//		for (int i = 0; scanner.hasNextLine(); ++i) {
//			String[] temp = scanner.nextLine().split(",");
//			for (int j = 0; j < 5; ++j)
//				data[i][j] = Double.parseDouble(temp[j + 1]);
//		}
//		scanner.close();
		
//		JavaRDD<LabeledPoint> rdd = MLUtils
//				.loadLibSVMFile(sc.sc(), "/home/wyj/Documents/irisSVM.data_mm")
//				.toJavaRDD().cache();
		
		
//		KDDHive hive = new KDDHive();
//
//		hive.local = true;
//		hive.localData = data;
//		
////		hive.classNum = 8;
////		hive.featureNum = 38;
////		hive.sampleNum = 4096;
//		
//		hive.classNum = 3;
//		hive.featureNum = 5;
//		hive.sampleNum = 178;
//
////		hive.classNum = 3;
////		hive.featureNum = 4;
////		hive.sampleNum = 150;
//
//		hive.maxCycle = 500;
//		hive.trialLimit = 100;
//		hive.employeNum = 100;
//		hive.onlookerNum = 100;
//		hive.scoutNum = 0;
//		hive.bestFitness = 0.0;
//
//		hive.solve();
//		for (int i = 0; i < hive.sampleNum; ++i) {
//			System.out.print(hive.transform(data[i]) + " ");
//			if (i % 50 == 0)
//				System.out.println();
//		}
//		JavaRDD<Tuple2<Integer, Integer>> result = hive.transform(rdd);
//		hive.accuracy(result);
//		sc.close();
	}
}
