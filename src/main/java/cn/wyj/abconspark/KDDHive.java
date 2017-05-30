package cn.wyj.abconspark;

import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

@SuppressWarnings("serial")
public class KDDHive implements java.io.Serializable {
	public boolean local = true;

	public double[][] localData;
	public JavaRDD<LabeledPoint> sparkData;

	public int classNum;
	public int featureNum;
	public int sampleNum;

	public int maxCycle;
	public int trialLimit;
	public int employeNum;
	public int onlookerNum;
	public int scoutNum;

	public KDDBee[] bees;
	public double[][] bestMemory;
	public double bestFitness;

	public double[][] genRandom() {
		double[][] randMem = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j)
				randMem[i][j] = KDDRand.nextDouble();
		return randMem;
	}

	public double[][] genNeighbor(double[][] memory, double[][] neihborMemory) {
		double[][] nextMem = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j) {
				nextMem[i][j] = memory[i][j] + (2 * KDDRand.nextDouble() - 1)
						* (memory[i][j] - neihborMemory[i][j]);
				if (nextMem[i][j] < 0.0)
					nextMem[i][j] = KDDRand.nextDouble();
				if (nextMem[i][j] > 1.0)
					nextMem[i][j] = KDDRand.nextDouble();
			}
		return nextMem;
	}

	public double calcFitness(double[][] memory) {
		double fitness = 0.0;
		if (local) {
			for (int i = 0; i < sampleNum; ++i) {
				double distance = Double.MAX_VALUE;
				for (int j = 0; j < classNum; ++j) {
					double temp = 0.0;
					for (int k = 0; k < featureNum; ++k)
						temp += Math.pow(localData[i][k] - memory[j][k], 2.0);
					temp = Math.pow(temp, 0.5);
					if (distance > temp)
						distance = temp;
				}
				fitness += distance;
			}
		} else {
			fitness = sparkData.map(v -> {
				double distance = Double.MAX_VALUE;
				for (int i = 0; i < classNum; ++i) {
					double temp = 0.0;
					for (int j = 0; j < featureNum; ++j)
						temp += Math.pow(v.features().apply(j) - memory[i][j],
								2.0);
					temp = Math.pow(temp, 0.5);
					if (distance > temp)
						distance = temp;
				}
				return distance;
			}).reduce((a, b) -> (a + b));
		}
		return 1.0 / fitness;
	}

	public void solve() {
		bees = new KDDBee[employeNum];
		for (int i = 0; i < employeNum; ++i) {
			double[][] randMem = genRandom();
			double randFit = calcFitness(randMem);
			bees[i] = new KDDBee(randMem, randFit, 0);
			if (bestFitness < randFit) {
				bestMemory = randMem;
				bestFitness = randFit;
			}
		}

		// double[][] testMem = new double[][] {
		// { 0.53, 0.50, 0.48, 0.51, 0.50, 0.52, 0.52, 0.50, 0.49, 0.51,
		// 0.51, 0.52, 0.53 },
		// { 0.33, 0.23, 0.47, 0.50, 0.27, 0.44, 0.37, 0.44, 0.38, 0.15,
		// 0.47, 0.56, 0.17 },
		// { 0.56, 0.51, 0.58, 0.56, 0.32, 0.24, 0.09, 0.60, 0.23, 0.52,
		// 0.16, 0.15, 0.25 } };
		// for (int i = 0; i < classNum; ++i) {
		// for (int j = 0; j < featureNum; ++j)
		// bees[0].memory[i][j] = testMem[i][j];
		// bees[0].fitness = calcFitness(bees[0].memory);
		// }

		for (int i = 0; i < maxCycle; ++i) {
			double sumOfFitness = 0.0;

			for (int j = 0; j < employeNum; ++j) {
				// employe bees
				if (bees[j].trial < trialLimit) {
					double[][] neighborMem = genNeighbor(bees[j].memory,
							bees[KDDRand.nextInt(employeNum)].memory);
					double neighborFit = calcFitness(neighborMem);
					if (bees[j].fitness < neighborFit) {
						bees[j].memory = neighborMem;
						bees[j].fitness = neighborFit;
						bees[j].trial = 0;
					} else
						++bees[j].trial;
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
			for (int j = 0; j < employeNum; ++j) {
				for (int k = 0; k < onlookerNum * bees[j].fitness
						/ sumOfFitness; ++k) {
					int neighborIndex = KDDRand.nextInt(employeNum);
					while (neighborIndex == j)
						neighborIndex = KDDRand.nextInt(employeNum);

					double[][] neighborMem = genNeighbor(bees[j].memory,
							bees[neighborIndex].memory);
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

			System.out.println("fitness: " + 1.0 / bestFitness);
			System.out.print("center: ");
			for (int j = 0; j < classNum; ++j) {
				for (int k = 0; k < featureNum; ++k)
					System.out.print(String.format("%.2f ", bestMemory[j][k]));
				System.out.println();
			}
		}
	}

	public int transform(double[] features) {
		int predictLabel = 0;
		double distance = Double.MAX_VALUE;
		for (int i = 0; i < classNum; ++i) {
			double temp = 0.0;
			for (int j = 0; j < featureNum; ++j)
				temp += Math.pow(features[j] - bestMemory[i][j], 2.0);
			temp = Math.pow(temp, 0.5);
			if (distance > temp) {
				predictLabel = i;
				distance = temp;
			}
		}
		return predictLabel;
	}

	public JavaRDD<Tuple2<Integer, Integer>> transform(
			JavaRDD<LabeledPoint> data) {
		return data.map(v -> {
			int predictLabel = 0;
			double distance = Double.MAX_VALUE;
			for (int i = 0; i < classNum; ++i) {
				double temp = 0.0;
				for (int j = 0; j < featureNum; ++j)
					temp += Math.pow(v.features().apply(j) - bestMemory[i][j],
							2.0);
				temp = Math.pow(temp, 0.5);
				if (distance > temp) {
					predictLabel = i;
					distance = temp;
				}
			}
			return new Tuple2<Integer, Integer>((int) v.label(), predictLabel);
		});
	}

	public void accuracy(JavaRDD<Tuple2<Integer, Integer>> clusterResult) {
		List<Tuple2<Integer, String>> statistics = clusterResult
				.mapToPair(v -> {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < classNum; ++i) {
						if (i == v._1)
							sb.append("1,");
						else
							sb.append("0,");
					}
					return new Tuple2<Integer, String>(v._2, sb.toString());
				}).reduceByKey((a, b) -> {
					String[] aCountArray = a.split(",");
					String[] bCountArray = b.split(",");
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < classNum; ++i)
						sb.append((Integer.parseInt(aCountArray[i])
								+ Integer.parseInt(bCountArray[i])) + ",");
					return sb.toString();
				}).collect();

		int totalCorrect = 0;
		for (int i = 0; i < statistics.size(); ++i) {
			int predictLabel = statistics.get(i)._1;
			System.out.print("predictLabel: " + predictLabel + " consists: ");

			String[] countArrayStr = statistics.get(i)._2.split(",");
			int[] countArray = new int[classNum];
			int maxNumLabel = 0;
			int clusterSize = 0;
			for (int j = 0; j < classNum; ++j) {
				countArray[j] = Integer.parseInt(countArrayStr[j]);
				System.out.print(j + ":" + countArray[j] + " ");
				if (countArray[maxNumLabel] < countArray[j])
					maxNumLabel = j;
				clusterSize += countArray[j];
			}
			System.out.println(
					"accuracy: " + 1.0 * countArray[maxNumLabel] / clusterSize);

			totalCorrect += countArray[maxNumLabel];
		}
		System.out.println("totalAccuracy: " + 1.0 * totalCorrect / sampleNum);
	}
}
