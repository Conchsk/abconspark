package cn.wyj.abconspark.clustering.standalone;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class ClusteringSAMain {
	public static void main(String[] args) throws FileNotFoundException {
		double[][] data = readCSV("F:/VMware/kddcup/kddcup4_mm.csv", 485269, 23);
		int classNum = 3;
		int swarmSize = 50;
		int maxCycle = 10;
		int trialLimit = 100;
		double mistakeRate = 0.05;

		ClusteringSAHive hive = new ClusteringSAHive(data, classNum, swarmSize, maxCycle, trialLimit, mistakeRate);
		hive.solve();
		hive.show();
//		for (int i = 0; i < data.length; ++i) {
//			System.out.print(hive.predict(data[i]) + " ");
//			if ((i + 1) % 50 == 0)
//				System.out.println();
//		}
	}

	public static double[][] readCSV(String uri, int sampleNum, int featureNum) throws FileNotFoundException {
		double[][] ret = new double[sampleNum][featureNum];
		Scanner sc = new Scanner(new File(uri));
		sc.nextLine();
		for (int i = 0; sc.hasNextLine(); ++i) {
			String line = sc.nextLine();
			if (line.isEmpty())
				break;
			else {
				String[] features = line.split(",");
				for (int j = 0; j < featureNum; ++j)
					ret[i][j] = Double.parseDouble(features[j]);
			}
		}
		sc.close();
		return ret;
	}
}
