package cn.wyj.abconspark.clustering.standalone;

import cn.wyj.abconspark.Bee;
import cn.wyj.abconspark.Rand;

@SuppressWarnings("serial")
public class ClusteringSAHive implements java.io.Serializable {
	private double[][] data;
	private int sampleNum;
	private int featureNum;
	private int classNum;

	private int swarmSize;
	private int maxCycle;
	private int trialLimit;
	private double mistakeRate;

	private Bee<double[][]>[] bees;
	public double[][] bestMemory;
	private double bestFitness;

	private double[][] genRandom() {
		double[][] randMem = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j)
				randMem[i][j] = Rand.nextDouble();
		return randMem;
	}

	private double[][] genNeighbor(double[][] memory, double[][] neihborMemory) {
		double[][] nextMem = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i) {
			for (int j = 0; j < featureNum; ++j) {
				nextMem[i][j] = memory[i][j] + (2 * Rand.nextDouble() - 1) * (memory[i][j] - neihborMemory[i][j]);
				if (nextMem[i][j] < 0.0)
					nextMem[i][j] = 0.0;
				if (nextMem[i][j] > 1.0)
					nextMem[i][j] = 1.0;
			}
		}
		return nextMem;
	}

	public double calcFitness(double[][] memory) {
		double fitness = 0.0;
		for (int i = 0; i < sampleNum; ++i) {
			double distance = Double.MAX_VALUE;
			for (int j = 0; j < classNum; ++j) {
				double temp = 0.0;
				for (int k = 0; k < featureNum; ++k)
					temp += Math.pow(data[i][k] - memory[j][k], 2.0);
				temp = Math.pow(temp, 0.5);
				if (distance > temp)
					distance = temp;
			}
			fitness += distance;
		}
		return 1.0 / fitness;
	}

	public ClusteringSAHive(double[][] data, int classNum, int swarmSize, int maxCycle, int trialLimit,
			double mistakeRate) {
		this.data = data;
		this.sampleNum = data.length;
		this.featureNum = data[0].length;
		this.classNum = classNum;

		this.swarmSize = swarmSize;
		this.maxCycle = maxCycle;
		this.trialLimit = trialLimit;
		this.mistakeRate = mistakeRate;

		this.bestFitness = 0.0;
	}

	@SuppressWarnings("unchecked")
	public void solve() {
		long start, stop;
		
		// init
		start = System.currentTimeMillis();
		System.out.println("init start");
		
		bees = new Bee[swarmSize];
		for (int i = 0; i < swarmSize; ++i) {
			double[][] randMem = genRandom();
			double randFit = calcFitness(randMem);
			bees[i] = new Bee<double[][]>(randMem, randFit, 0);
			if (bestFitness < randFit) {
				bestMemory = randMem;
				bestFitness = randFit;
			}
		}
		
		stop = System.currentTimeMillis();
		System.out.println("init finished after " + (stop - start) / 1000.0 + " s");

		// main loop
		for (int i = 0; i < maxCycle; ++i) {
			start = System.currentTimeMillis();
			System.out.println("one cycle start");
			
			double sumOfFitness = 0.0;
			for (int j = 0; j < swarmSize; ++j)
				sumOfFitness += bees[j].fitness;
			
			for (int j = 0; j < swarmSize; ++j) {
				// employe bees & onlooker bees
				if (bees[j].trial < trialLimit) {
					for (int k = 0; k < swarmSize * bees[j].fitness / sumOfFitness; ++k) {
						int neighborIndex = Rand.nextInt(swarmSize);
						while (neighborIndex == j)
							neighborIndex = Rand.nextInt(swarmSize);

						double[][] neighborMem = genNeighbor(bees[j].memory, bees[neighborIndex].memory);
						double neighborFit = calcFitness(neighborMem);
						if (bees[j].fitness < neighborFit) {
							if (Rand.nextDouble() > mistakeRate) {
								bees[j].memory = neighborMem;
								bees[j].fitness = neighborFit;
								bees[j].trial = 0;
							} else
								++bees[j].trial;
						} else {
							if (Rand.nextDouble() < mistakeRate) {
								bees[j].memory = neighborMem;
								bees[j].fitness = neighborFit;
								bees[j].trial = 0;
							} else
								++bees[j].trial;
						}
					}
				}
				// scout bees
				else {
					bees[j].memory = genRandom();
					bees[j].fitness = calcFitness(bees[j].memory);
					bees[j].trial = 0;
				}
				
				if (bestFitness < bees[j].fitness) {
					bestMemory = bees[j].memory;
					bestFitness = bees[j].fitness;
				}
			}
			
			stop = System.currentTimeMillis();
			System.out.println("one cycle finished after " + (stop - start) / 1000.0 + " s");
			
			//if ((i + 1) * 10 % maxCycle == 0)
			//	System.out.print('#');
				
			show();
		}
		System.out.println();
	}

	public void show() {
		System.out.println(String.format("fitness: %.2f", 1.0 / bestFitness));
//		System.out.print("center: ");
//		for (int i = 0; i < classNum; ++i) {
//			for (int j = 0; j < featureNum; ++j)
//				System.out.print(String.format("%.2f ", bestMemory[i][j]));
//			System.out.println();
//		}
	}

	public int predict(double[] features) {
		int predictLabel = 0;
		double distance = Double.MAX_VALUE;
		for (int i = 0; i < classNum; ++i) {
			double temp = 0.0;
			for (int j = 0; j < featureNum; ++j)
				temp += Math.pow(features[j] - bestMemory[i][j], 2.0);
			temp = Math.pow(temp, 0.5);
			if (distance > temp) {
				distance = temp;
				predictLabel = i;
			}
		}
		return predictLabel;
	}
}
