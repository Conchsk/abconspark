package cn.wyj.abconspark.function.standalone;

public class FunctionSAMain {
	public static void main(String[] args) {
		double[][] data = new double[][] {
			{ 1, 2, 3, 4, 5 }, { 2, 3, 4, 5, 1 }, { 3, 4, 5, 1, 2 }, { 4, 5, 1, 2, 3 },
			{ 5, 1, 2, 3, 4 }, { -1, -2, -3, -4, -5 }, { -2, -3, -4, -5, -1 }, { -3, -4, -5, -1, -2 },
			{ -4, -5, -1, -2, -3 }, { -5, -1, -2, -3, -4 }
		};
		int swarmSize = 50;
		int maxCycle = 2000;
		int trailLimit = 100;
		double mistakeRate = 0.05;
		
		FunctionSAHive hive = new FunctionSAHive(data, swarmSize, maxCycle, trailLimit, mistakeRate);
		hive.solve();
		hive.show();
	}
}
