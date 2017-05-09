package cn.wyj.abconspark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;

@SuppressWarnings("serial")
public class KDDHive implements java.io.Serializable {
	public List<LabeledPoint> data;
	public int classNum = 3;
	public int featureNum = 4;
	public int sampleNum = 150;
	
	public int maxCycle;
	public int trialLimit;
	public int employeNum;
	public int onlookerNum;
	public int scoutNum;

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
				tempFit += Math.pow(data.get(i).features().apply(j) - centers[labeleds[i]][j], 2.0);
			fitness += Math.sqrt(tempFit);
		}
		return 1 / fitness;
	}
	
	public void solve(JavaSparkContext sc) {
		JavaRDD<KDDBee> rdd = sc.parallelize(Arrays.asList(new KDDBee[employeNum]));
		rdd = rdd.map(value -> {
			double[][] randMem = genRandom();
			double randFit = calcFitness(randMem);
			return new KDDBee(randMem, randFit, 0);
		}).cache();
		
		for (int i = 0; i < maxCycle; ++i) {
			// final var
			@SuppressWarnings("unchecked")
			JavaRDD<KDDBee>[] finalVar = new JavaRDD[1];
			
			// employe bees
			finalVar[0] = rdd;
			JavaRDD<KDDBee> tempRdd = rdd.map(value -> {
				if (value.trial < trialLimit) {
					double[][] neighborMem = genNeighbor(value.memory, finalVar[0].takeSample(false, 1).get(0).memory);
					double neighborFit = calcFitness(neighborMem);
					if (value.fitness < neighborFit) {
						value.memory = neighborMem;
						value.fitness = neighborFit;
						value.trial = 0;
					} else
						++value.trial;
				} else {
					value.memory = genRandom();
					value.fitness = calcFitness(value.memory);
					value.trial = 0;
				}
				return value;
			}).cache();
			rdd.unpersist();
			
			// onlooker bees
			double sumOfFitness = rdd.reduce((a, b) -> {
				a.fitness += b.fitness;
				return a;
			}).fitness;
			
			finalVar[0] = tempRdd;
			rdd = tempRdd.map(value -> {
				for (int j = 0; j < onlookerNum * value.fitness / sumOfFitness; ++j) {
					double[][] neighborMem = genNeighbor(value.memory, finalVar[0].takeSample(false, 1).get(0).memory);
					double neighborFit = calcFitness(neighborMem);
					if (value.fitness < neighborFit) {
						value.memory = neighborMem;
						value.fitness = neighborFit;
						value.trial = 0;
					} else
						++value.trial;
				}
				return value;
			}).cache();
			tempRdd.unpersist();
			
			KDDBee bestBee = rdd.reduce((a, b) -> {
				return a.fitness > b.fitness ? a : b;
			});
			if (bestFitness < bestBee.fitness) {
				bestMemory = bestBee.memory;
				bestFitness = bestBee.fitness;
			}
			
			// scout
			tempRdd = sc.parallelize(Arrays.asList(new KDDBee[scoutNum]));
			tempRdd = tempRdd.map(value -> {
				double[][] randMem = genRandom();
				double randFit = calcFitness(randMem);
				return new KDDBee(randMem, randFit, 0);
			}).cache();
			bestBee = tempRdd.reduce((a, b) -> {
				return a.fitness > b.fitness ? a : b;
			});
			if (bestFitness < bestBee.fitness) {
				bestMemory = bestBee.memory;
				bestFitness = bestBee.fitness;
			}
			tempRdd.unpersist();
		}
	}
	
	private int[] markLabel(double[][] centers) {
		int[] labeleds = new int[sampleNum];
		for (int i = 0; i < sampleNum; ++i) {
			double distance = Double.MAX_VALUE;
			for (int j = 0; j < classNum; ++j) {
				double temp = 0;
				for (int k = 0; k < featureNum; ++k)
					temp += Math.pow(data.get(i).features().apply(k) - centers[j][k], 2.0);
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
				centers[labeleds[i]][j] += data.get(i).features().apply(j);
			++tempSum[labeleds[i]];
		}		
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j)
				centers[i][j] /= tempSum[i];
		return centers;
	}
}
