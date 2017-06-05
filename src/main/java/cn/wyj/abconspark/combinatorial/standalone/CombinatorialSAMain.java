package cn.wyj.abconspark.combinatorial.standalone;

public class CombinatorialSAMain {
	public static void main(String[] args) {
		double[] data = new double[] { 3, 2, 6, 4, 5, 7, 9, 13, 4, 12, 10, 18, 22, 11, 8, 26, 14, 6, 17, 27, 11, 17, 26,
				16, 7, 23, 15, 18, 15, 13 };
		int machineNum = 10;
		int swarmSize = 50;
		int maxCycle = 1048577;
		int trialLimit = 100;
		double mistakeRate = 0.05;

		CombinatorialSAHive hive = new CombinatorialSAHive(data, machineNum, swarmSize, maxCycle, trialLimit,
				mistakeRate);
		hive.solve();
		//hive.show();
	}
}
