package cn.wyj.abconspark.clustering.distribute;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import cn.wyj.abconspark.Bee;
import cn.wyj.abconspark.Rand;
import scala.Tuple2;

@SuppressWarnings("serial")
public class ClusteringDTHive implements java.io.Serializable {
	private Dataset<LabeledPoint> data;
	private int featureNum;
	private int classNum;

	private int swarmSize;
	private int maxCycle;
	private int trialLimit;
	private double mistakeRate;

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
		}, Encoders.DOUBLE()).reduce((a, b) -> a + b);
	}

	public ClusteringDTHive(Dataset<LabeledPoint> data, int classNum, int swarmSize, int maxCycle, int trialLimit,
			double mistakeRate) {
		this.data = data;
		this.featureNum = data.first().features().size();
		this.classNum = classNum;

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
			
			for (int j = 0; j < swarmSize; ++j) {
				// employe bees
				if (bees[j].trial < trialLimit) {
					double[][] neighborMem = genNeighbor(bees[j].memory, bees[Rand.nextInt(swarmSize)].memory);
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

				if (bestFitness < bees[j].fitness) {
					bestMemory = bees[j].memory;
					bestFitness = bees[j].fitness;
				}
			}
			System.out.println(i);
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
	
	public Dataset<Tuple2<Integer, Double>> predict(Dataset<LabeledPoint> features) {
		return features.map(new MapFunction<LabeledPoint, Tuple2<Integer, Double>>() {
			@Override
			public Tuple2<Integer, Double> call(LabeledPoint arg0) throws Exception {
				int predictLabel = 0;
				double distance = Double.MAX_VALUE;
				for (int i = 0; i < classNum; ++i) {
					double temp = 0.0;
					for (int j = 0; j < featureNum; ++j)
						temp += Math.pow(arg0.features().apply(j) - bestMemory[i][j], 2.0);
					temp = Math.pow(temp, 0.5);
					if (distance > temp) {
						distance = temp;
						predictLabel = i;
					}
				}
				return new Tuple2<Integer, Double>(predictLabel, arg0.label()) ;
			}
		}, Encoders.tuple(Encoders.INT(), Encoders.DOUBLE()));
	}
}
