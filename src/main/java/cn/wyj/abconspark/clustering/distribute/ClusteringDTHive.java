package cn.wyj.abconspark.clustering.distribute;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

import cn.wyj.abconspark.Bee;
import cn.wyj.abconspark.Rand;

@SuppressWarnings("serial")
public class ClusteringDTHive implements java.io.Serializable {
	private JavaRDD<LabeledPoint> data;
	//private int sampleNum;
	private int featureNum;
	private int classNum;

	private int maxCycle;
	private int trialLimit;
	private int employeNum;
	private int onlookerNum;
	private int scoutNum;

	private Bee<double[][]>[] bees;
	private double[][] bestMemory;
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
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j) {
				nextMem[i][j] = memory[i][j] + (2 * Rand.nextDouble() - 1) * (memory[i][j] - neihborMemory[i][j]);
				if (nextMem[i][j] < 0.0)
					nextMem[i][j] = 0.0;
				if (nextMem[i][j] > 1.0)
					nextMem[i][j] = 1.0;
			}
		return nextMem;
	}

	private double calcFitness(double[][] memory) {
		return 1.0 / data.map(v -> {
			double distance = Double.MAX_VALUE;
			for (int i = 0; i < classNum; ++i) {
				double temp = 0.0;
				for (int j = 0; j < featureNum; ++j)
					temp += Math.pow(v.features().apply(j) - memory[i][j], 2.0);
				temp = Math.pow(temp, 0.5);
				if (distance > temp)
					distance = temp;
			}
			return distance;
		}).reduce((a, b) -> a + b);
	}

	public ClusteringDTHive(JavaRDD<LabeledPoint> data, int classNum, int maxCycle, int trialLimit, int employeNum, int onlookerNum,
			int scoutNum) {
		this.data = data;
		//this.sampleNum = (int) data.count();
		this.featureNum = data.top(1).get(0).features().size();
		this.classNum = classNum;

		this.maxCycle = maxCycle;
		this.trialLimit = trialLimit;
		this.employeNum = employeNum;
		this.onlookerNum = onlookerNum;
		this.scoutNum = scoutNum;
		
		this.bestFitness = 0.0;
	}

	@SuppressWarnings("unchecked")
	public void solve() {
		// init
		bees = new Bee[employeNum];
		for (int i = 0; i < employeNum; ++i) {
			double[][] randMem = genRandom();
			double randFit = calcFitness(randMem);
			bees[i] = new Bee<double[][]>(randMem, randFit, 0);
			if (bestFitness < randFit) {
				bestMemory = randMem;
				bestFitness = randFit;
			}
		}

		// main loop
		for (int i = 0; i < maxCycle; ++i) {
			double sumOfFitness = 0.0;

			// employe bees
			for (int j = 0; j < employeNum; ++j) {
				if (bees[j].trial < trialLimit) {
					double[][] neighborMem = genNeighbor(bees[j].memory, bees[Rand.nextInt(employeNum)].memory);
					double neighborFit = calcFitness(neighborMem);
					if (bees[j].fitness < neighborFit) {
						bees[j].memory = neighborMem;
						bees[j].fitness = neighborFit;
						bees[j].trial = 0;
					} else
						++bees[j].trial;
				} else {
					bees[j].memory = genRandom();
					bees[j].fitness = calcFitness(bees[j].memory);
					bees[j].trial = 0;
				}

				sumOfFitness += bees[j].fitness;
			}

			// onlooker bees
			for (int j = 0; j < employeNum; ++j) {
				for (int k = 0; k < onlookerNum * bees[j].fitness / sumOfFitness; ++k) {
					int neighborIndex = Rand.nextInt(employeNum);
					while (neighborIndex == j)
						neighborIndex = Rand.nextInt(employeNum);

					double[][] neighborMem = genNeighbor(bees[j].memory, bees[neighborIndex].memory);
					double neighborFit = calcFitness(neighborMem);
					if (bees[j].fitness < neighborFit) {
						bees[j].memory = neighborMem;
						bees[j].fitness = neighborFit;
						bees[j].trial = 0;
					} else
						++bees[j].trial;
				}

				if (bestFitness < bees[j].fitness) {
					bestMemory = bees[j].memory;
					bestFitness = bees[j].fitness;
				}
			}

			// scout bees
			for (int j = 0; j < scoutNum; ++j) {
				double[][] randMem = genRandom();
				double randFit = calcFitness(randMem);
				if (bestFitness < randFit) {
					bestMemory = randMem;
					bestFitness = randFit;
				}
			}
			
			//show();
		}
	}

	public void show() {
		System.out.println("fitness: " + 1.0 / bestFitness);
		System.out.print("center: ");
		for (int i = 0; i < classNum; ++i) {
			for (int j = 0; j < featureNum; ++j)
				System.out.print(String.format("%.2f ", bestMemory[i][j]));
			System.out.println();
		}
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
