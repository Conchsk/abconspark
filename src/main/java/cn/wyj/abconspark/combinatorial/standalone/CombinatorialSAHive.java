package cn.wyj.abconspark.combinatorial.standalone;

import cn.wyj.abconspark.Bee;
import cn.wyj.abconspark.Rand;

@SuppressWarnings("serial")
public class CombinatorialSAHive implements java.io.Serializable {
	private double[] data;
	private int jobNum;
	private int machineNum;

	private int swarmSize;
	private int maxCycle;
	private int trialLimit;
	private double mistakeRate;

	private Bee<int[]>[] bees;
	private int[] bestMemory;
	private double bestFitness;
	
	private int[] genRandom() {
		int dimension = jobNum + machineNum - 1;
		int[] randMem = new int[dimension];
		for (int i = 0; i < dimension; ++i)
			randMem[i] = i;
		for (int i = 0; i < dimension; ++i) {
			int j = Rand.nextInt(dimension);
			if (i != j) {
				int temp = randMem[i];
				randMem[i] = randMem[j];
				randMem[j] = temp;
			}
		}
		return randMem;
	}

	private int[] genNeighbor(int[] memory) {
		int dimension = jobNum + machineNum - 1;
		int[] neighborMem = new int[dimension];
		for (int i = 0; i < dimension; ++i)
			neighborMem[i] = memory[i];
		int i = Rand.nextInt(dimension);
		int j = (i + 1) % dimension;
		if (i != j) {
			int temp = neighborMem[i];
			neighborMem[i] = neighborMem[j];
			neighborMem[j] = temp;
		}
		return neighborMem;
	}

	private double calcFitness(int[] memory) {
		int dimension = jobNum + machineNum - 1;
		double fitness = 0.0;
		double d = 0.0;
		for (int i = 0; i < dimension; ++i) {
			if (memory[i] < jobNum)
				d += data[memory[i]];
			else {
				if (fitness < d)
					fitness = d;
				d = 0.0;
			}
		}
		if (fitness < d)
			fitness = d;
		return 1.0 / fitness;
	}

	public CombinatorialSAHive(double[] data, int machineNum, int swarmSize, int maxCycle, int trialLimit,
			double mistakeRate) {
		this.data = data;
		this.jobNum = data.length;
		this.machineNum = machineNum;

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
			int[] randMem = genRandom();
			double randFit = calcFitness(randMem);
			bees[i] = new Bee<int[]>(randMem, randFit, 0);
			if (bestFitness < randFit) {
				bestMemory = randMem;
				bestFitness = randFit;
			}
		}
		
		int mod = 1;

		// main loop
		for (int i = 0; i < maxCycle; ++i) {
			double sumOfFitness = 0.0;
			
			for (int j = 0; j < swarmSize; ++j) {
				// employe bees
				if (bees[j].trial < trialLimit) {
					int[] neighborMem = genNeighbor(bees[j].memory);
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
				// scout bees
				else {
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

					int[] neighborMem = genNeighbor(bees[j].memory);
					double neighborFit = calcFitness(neighborMem);
					if (bees[j].fitness < neighborFit) {
						if (Rand.nextDouble() > mistakeRate) {
							bees[j].memory = neighborMem;
							bees[j].fitness = neighborFit;
							bees[j].trial = 0;
						} else {
							++bees[j].trial;
						}
					} else {
						if (Rand.nextDouble() < mistakeRate) {
							bees[j].memory = neighborMem;
							bees[j].fitness = neighborFit;
							bees[j].trial = 0;
						} else {
							++bees[j].trial;
						}
					}
				}

				if (bestFitness < bees[j].fitness) {
					bestMemory = bees[j].memory;
					bestFitness = bees[j].fitness;
				}
			}
			
//			if ((i + 1) * 10 % maxCycle == 0)
//				System.out.print('#');
			
			if ((i + 1) == mod) {
				System.out.println(1.0 / bestFitness);
				mod *= 2;
			}
				
			//show();
		}
		System.out.println();
	}

	public void show() {
		System.out.println(String.format("fitness: %.0f", 1.0 / bestFitness));
		System.out.print("schedule: ");
		for (int i = 0; i < jobNum + machineNum - 1; ++i)
			System.out.print(String.format("%d ", bestMemory[i]));
		System.out.println();
	}
}
