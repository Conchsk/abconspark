package cn.wyj.abconspark.clustering.standalone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ClusteringSAMain {
	public static void main(String[] args) throws IOException {
		double[][] data = readCSV("/home/wyj/Documents/kddcup_mm_pca_mm.csv", 485269, 5);
		int classNum = 3;
		int swarmSize = 50;
		int maxCycle = 100;
		int trialLimit = 100;
		double mistakeRate = 0.05;

		ClusteringSAHive hive = new ClusteringSAHive(data, classNum, swarmSize, maxCycle, trialLimit, mistakeRate);
		hive.solve();
		predict("/home/wyj/Documents/kddcup_mm_pca_mm.csv", classNum, 23, hive, data);
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
	
	@SuppressWarnings({ "unchecked", "serial" })
	public static void predict(String uri, int classNum, int labelIndex, ClusteringSAHive hive, double[][] features) throws IOException {
		Map<String, Integer>[] maps = new HashMap[classNum];
		for (int i = 0; i < classNum; ++i) {
			maps[i] = new HashMap<String, Integer>(){
				{
					put("normal.", 0);
					put("neptune.", 0);
					put("smurf.", 0);
				}
			};
		};
		FileWriter fw = new FileWriter(new File("/home/wyj/Documents/kddcup_mm_pca53.csv"));
		fw.write("label\n");
		Scanner sc = new Scanner(new File(uri));
		sc.nextLine();
		for (int i = 0; sc.hasNextLine(); ++i) {
			String line = sc.nextLine();
			if (line.isEmpty())
				break;
			else {
				String label = line.split(",")[labelIndex];
				int predictLabel = hive.predict(features[i]);
				int oldVal = maps[predictLabel].get(label);
				maps[predictLabel].put(label, oldVal + 1);
				fw.write(predictLabel + "\n");
			}
		}
		sc.close();
		fw.close();
		
		for (int i = 0; i < classNum; ++i)
			for (String key : maps[i].keySet())
				System.out.println(String.format("label: %d, %s: %d", i, key, maps[i].get(key)));
	}
}
