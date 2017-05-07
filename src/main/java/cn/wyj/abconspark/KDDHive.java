package cn.wyj.abconspark;

@SuppressWarnings("serial")
public class KDDHive implements java.io.Serializable {
	public double[][] data;
	public int classNum = 3;
	public int featureNum = 4;
	public int sampleNum = 150;
	
	public int maxCycle;
	public int trialLimit;
	public int employeNum;
	public int onlookerNum;
	public int scoutNum;

	public KDDBee[] bees;
	public double[][] bestMemory;
	public double bestFitness;
	
	public double[][] genRandom() {
		int[] labeleds = new int[sampleNum];
		for (int i = 0; i < sampleNum; ++i)
			labeleds[i] = KDDRand.nextInt(classNum);
		return calcCenter(labeleds);
	}

	public double[][] genNeighbor(double[][] memory, double[][] neihborMemory) {
		double[][] nextMem = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j)
				nextMem[i][j] = memory[i][j] + (2 * KDDRand.nextDouble()  - 1) * neihborMemory[i][j];
		return nextMem;
	}

	public double calcFitness(double[][] memory) {
		int[] labeleds = markLabel(memory);
		double[][] centers = calcCenter(labeleds);
		
		double fitness = 0.0;
		for (int i = 0; i < sampleNum; ++i) {
			double tempFit = 0.0;
			for (int j = 0; j < featureNum; ++j)
				tempFit += Math.pow(data[i][j] - centers[labeleds[i]][j], 2.0);
			fitness += Math.sqrt(tempFit);
		}
		return 1 / fitness;
	}
	
	private int[] markLabel(double[][] centers) {
		int[] labeleds = new int[sampleNum];
		for (int i = 0; i < sampleNum; ++i) {
			double distance = Double.MAX_VALUE;
			for (int j = 0; j < classNum; ++j) {
				double temp = 0;
				for (int k = 0; k < featureNum; ++k)
					temp += Math.pow(data[i][k] - centers[j][k], 2.0);
				if (distance > temp) {
					distance = temp;
					labeleds[i] = j;
				}
			}
		}
		return labeleds;
	}
	
	private double[][] calcCenter(int[] labeleds) {
		double[][] centers = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j)
				centers[i][j] = 0.0;
		
		int[] tempSum = new int[classNum];
		for (int i = 0; i < classNum; ++i)
			tempSum[i] = 0;
		
		for (int i = 0; i < sampleNum; ++i) {
			for (int j = 0; j < featureNum; ++j)
				centers[labeleds[i]][j] += data[i][j];
			++tempSum[labeleds[i]];
		}		
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j)
				centers[i][j] /= tempSum[i];
		return centers;
	}
}
