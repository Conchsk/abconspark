package cn.wyj.abconspark.clustering.standalone;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class ClusteringSAMain {
	public static void main(String[] args) throws FileNotFoundException {
		double[][] data = readCSV("F:/iris_mm.csv", 150, 4);
		ClusteringSAHive hive = new ClusteringSAHive(data, 3, 1000, 100, 100, 100, 0);
		hive.solve();
		hive.show();
		for (int i = 0; i < data.length; ++i) {
			System.out.println(hive.predict(data[i]));
//			if ((i + 1) % 50 == 0)
//				System.out.println();
		}
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
