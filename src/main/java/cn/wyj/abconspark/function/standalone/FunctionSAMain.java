package cn.wyj.abconspark.function.standalone;

public class FunctionSAMain {
	public static void main(String[] args) {
		double[][] data = new double[][] {
			{ 1, 2, 3, 4, 5 }, { 2, 3, 4, 5, 1 }, { 3, 4, 5, 1, 2 }, { 4, 5, 1, 2, 3 },
			{ 5, 1, 2, 3, 4 }, { -1, -2, -3, -4, -5 }, { -2, -3, -4, -5, -1 }, { -3, -4, -5, -1, -2 },
			{ -4, -5, -1, -2, -3 }, { -5, -1, -2, -3, -4 }
		};

		FunctionSAHive hive = new FunctionSAHive(data, 10000, 100, 100, 100, 100);
		hive.solve();
		hive.show();
	}
}
