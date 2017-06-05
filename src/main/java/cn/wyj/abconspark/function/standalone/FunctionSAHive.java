package cn.wyj.abconspark.function.standalone;

import cn.wyj.abconspark.Bee;
import cn.wyj.abconspark.Rand;

@SuppressWarnings("serial")
public class FunctionSAHive implements java.io.Serializable {
	private double[][] data;
	private int dataCount;
	private int dimension;

	private int swarmSize;
	private int maxCycle;
	private int trialLimit;
	private double mistakeRate;

	private Bee<double[]>[] bees;
	private double[] bestMemory;
	private double bestFitness;

	public double[] genRandom() {
		double[] newMemory = new double[dimension];
		for (int i = 0; i < newMemory.length; ++i)
			newMemory[i] = 200 * Rand.nextDouble() - 100;
		return newMemory;
	}

	public double[] genNeighbor(double[] memory, double[] neighborMemory) {
		double[] newMemory = new double[dimension];
		for (int i = 0; i < newMemory.length; ++i) {
			newMemory[i] += (2 * Rand.nextDouble() - 1) * (memory[i] - neighborMemory[i]);
			if (newMemory[i] < -100)
				newMemory[i] = -100;
			if (newMemory[i] > 100)
				newMemory[i] = 100;
		}
		return newMemory;
	}

	public double calcFitness(double[] memory) {
		// double fitness = 0.0;
		// for (int i = 0; i < dataCount; ++i)
		// fitness += Math.pow(errorCalc(memory, data[i]), 2);
		// return 1.0 / (fitness + 1.0);
		double fitness = memory[0] * memory[0] + memory[1] * memory[1] + memory[2] * memory[2] + memory[3] * memory[3]
				+ memory[4] * memory[4];
		return 1.0 / (fitness + 1.0);
	}

//	private double errorCalc(double[] param, double[] x) {
//		return (80.0 - param[0]) * Math.pow(x[0], 3.0) + (40.0 - param[1]) * Math.pow(x[1], 2.0)
//				+ (1.0 - param[2]) * Math.pow(x[2], 1.0) + (-40.0 - param[3]) * Math.pow(x[3], 2.0)
//				+ (-80.0 - param[4]) * Math.pow(x[4], 3.0);
//	}

	public FunctionSAHive(double[][] data, int swarmSize, int maxCycle, int trialLimit, double mistakeRate) {
		this.data = data;
		this.dataCount = data.length;
		this.dimension = data[0].length;

		this.swarmSize = swarmSize;
		this.maxCycle = maxCycle;
		this.trialLimit = trialLimit;
		this.mistakeRate = mistakeRate;

		this.bestFitness = 0.0;
	}

	@SuppressWarnings("unchecked")
	public void solve() {
		// init
		bees = new Bee[swarmSize];
		for (int i = 0; i < swarmSize; ++i) {
			double[] randMem = genRandom();
			double randFit = calcFitness(randMem);
			bees[i] = new Bee<double[]>(randMem, randFit, 0);
			if (bestFitness < randFit) {
				bestMemory = randMem;
				bestFitness = randFit;
			}
		}

		// main loop
		for (int i = 0; i < maxCycle; ++i) {
			double sumOfFitness = 0.0;

			for (int j = 0; j < swarmSize; ++j) {
				// employe bees
				if (bees[j].trial < trialLimit) {
					double[] neighborMem = genNeighbor(bees[j].memory, bees[Rand.nextInt(swarmSize)].memory);
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
				} else {
					bees[j].memory = genRandom();
					bees[j].fitness = calcFitness(bees[j].memory);
					bees[j].trial = 0;
				}

				sumOfFitness += bees[j].fitness;
			}

			// onlooker bees
			for (int j = 0; j < swarmSize; ++j) {
				for (int k = 0; k < swarmSize * bees[j].fitness / sumOfFitness; ++k) {
					int neighborIndex = Rand.nextInt(swarmSize);
					while (neighborIndex == j)
						neighborIndex = Rand.nextInt(swarmSize);

					double[] neighborMem = genNeighbor(bees[j].memory, bees[neighborIndex].memory);
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

				if (bestFitness < bees[j].fitness) {
					bestMemory = bees[j].memory;
					bestFitness = bees[j].fitness;
					System.out.println("onlooker");
				}
			}

			if ((i + 1) * 10 % maxCycle == 0)
				System.out.print('#');
		}
		System.out.println();
	}

	public void show() {
		System.out.println("fitness: " + (1.0 / bestFitness - 1.0));
		System.out.print("paramPredict: ");
		for (int i = 0; i < dimension; ++i)
			System.out.print(String.format("%.2f ", bestMemory[i]));
		System.out.println();
	}
}
