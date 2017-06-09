package cn.wyj.abconspark.clustering.distribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.util.Utils;

import cn.wyj.abconspark.Bee;
import scala.Tuple2;

@SuppressWarnings("serial")
public class ClusteringDTHive implements java.io.Serializable {
	private List<LabeledPoint> data;
	private int sampleNum;
	private int featureNum;
	private int classNum;

	private int swarmSize;
	private int maxCycle;
	private int trialLimit;
	private double mistakeRate;

	private double[][] bestMemory;
	private double bestFitness;

	private double[][] genRandom() {
		double[][] randMem = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j)
				randMem[i][j] = Utils.random().nextDouble();
		return randMem;
	}

	private double[][] genNeighbor(double[][] memory,
			double[][] neihborMemory) {
		double[][] nextMem = new double[classNum][featureNum];
		for (int i = 0; i < classNum; ++i)
			for (int j = 0; j < featureNum; ++j) {
				nextMem[i][j] = memory[i][j]
						+ (2 * Utils.random().nextDouble() - 1)
								* (memory[i][j] - neihborMemory[i][j]);
				if (nextMem[i][j] < 0.0)
					nextMem[i][j] = 0.0;
				if (nextMem[i][j] > 1.0)
					nextMem[i][j] = 1.0;
			}
		return nextMem;
	}

	private double calcFitness(double[][] memory) {
		double fitness = 0.0;
		for (int i = 0; i < sampleNum; ++i) {
			double distance = Double.MAX_VALUE;
			for (int j = 0; j < classNum; ++j) {
				double temp = 0.0;
				for (int k = 0; k < featureNum; ++k)
					temp += Math.pow(data.get(i).features().apply(k) - memory[j][k], 2.0);
				temp = Math.pow(temp, 0.5);
				if (distance > temp)
					distance = temp;
			}
			fitness += distance;
		}
		return 1.0 / fitness;
	}

	public ClusteringDTHive(JavaRDD<LabeledPoint> data, int classNum,
			int swarmSize, int maxCycle, int trialLimit, double mistakeRate) {
		this.data = data.collect();
		this.sampleNum = (int) data.count();
		this.featureNum = data.first().features().size();
		this.classNum = classNum;

		this.swarmSize = swarmSize;
		this.maxCycle = maxCycle;
		this.trialLimit = trialLimit;
		this.mistakeRate = mistakeRate;

		this.bestFitness = 0.0;
	}

	@SuppressWarnings("unchecked")
	public void solve(JavaSparkContext sc) {
		long start, stop;

		// init
		start = System.currentTimeMillis();
		System.out.println("init start");
		
		Bee<double[][]>[] beeArray = new Bee[swarmSize];
		for (int i = 0; i < swarmSize; ++i) {
			double[][] randMem = genRandom();
			double randFit = calcFitness(randMem);
			beeArray[i] = new Bee<double[][]>(randMem, randFit, 0);
		}		
		JavaRDD<Bee<double[][]>> rdd = sc.parallelize(Arrays.asList(beeArray)).cache();
		
		stop = System.currentTimeMillis();
		System.out.println("init finished after " + (stop - start) / 1000.0 + " s");

		// main loop		
		for (int i = 0; i < maxCycle; ++i) {
			start = System.currentTimeMillis();
			System.out.println("cycle " + i + " start");
			
			// find best solution so far
			Bee<double[][]> bestBee = rdd.mapPartitions(v -> {
				List<Bee<double[][]>> ret = new ArrayList<Bee<double[][]>>();
				double[][] tempMem = null;
				double tempFit = 0.0;
				while (v.hasNext()) {
					Bee<double[][]> bee = v.next();
					if (tempFit < bee.fitness) {
						tempMem = bee.memory;
						tempFit = bee.fitness;
					}
				}
				ret.add(new Bee<double[][]>(tempMem, tempFit, 0));
				return ret.iterator();
			}).reduce((a, b) -> a.fitness > b.fitness ? a : b);
			
			if (bestFitness < bestBee.fitness) {
				bestMemory = bestBee.memory;
				bestFitness = bestBee.fitness;
			}
			
			// calc SumOfFitness
			double sumOfFitness = rdd.mapPartitions(v -> {
				List<Bee<double[][]>> ret = new ArrayList<Bee<double[][]>>();
				double tempSumFit = 0.0;
				while (v.hasNext()) {
					Bee<double[][]> bee = v.next();
					tempSumFit += bee.fitness;
				}				
				ret.add(new Bee<double[][]>(null, tempSumFit, 0));
				return ret.iterator();
			}).reduce((a, b) -> {
				a.fitness += b.fitness;
				return a;
			}).fitness;
			
			// main proc
			rdd = rdd.mapPartitions(v -> {
				List<Bee<double[][]>> beeList = new ArrayList<Bee<double[][]>>();
				while (v.hasNext())
					beeList.add(v.next());
				int partSwarmSize = beeList.size();
				
				List<Bee<double[][]>> ret = new ArrayList<Bee<double[][]>>();
				for (int j = 0; j < partSwarmSize; ++j) {
					Bee<double[][]> bee = beeList.get(j);
					if (bee.trial < trialLimit) {
						for (int k = 0; k < swarmSize * bee.fitness / sumOfFitness; ++k) {
							int neighborIndex = Utils.random().nextInt(partSwarmSize);
							while (neighborIndex == j)
								neighborIndex = Utils.random().nextInt(partSwarmSize);
							double[][] neighborMem = genNeighbor(bee.memory, beeList.get(neighborIndex).memory);
							double neighborFit = calcFitness(neighborMem);
							if (bee.fitness < neighborFit) {
								if (Utils.random().nextDouble() > mistakeRate) {
									bee.memory = neighborMem;
									bee.fitness = neighborFit;
									bee.trial = 0;
								} else
									++bee.trial;
							} else {
								if (Utils.random().nextDouble() < mistakeRate) {
									bee.memory = neighborMem;
									bee.fitness = neighborFit;
									bee.trial = 0;
								} else
									++bee.trial;
							}
						}
					} else {
						bee.memory = genRandom();
						bee.fitness = calcFitness(bee.memory);
						bee.trial = 0;
					}
					ret.add(bee);
				}
				return ret.iterator();
			}).cache();
			
			stop = System.currentTimeMillis();
			System.out.println("cycle " + i + " finished after " + (stop - start) / 1000.0 + " s");
			System.out.println(1.0 / bestFitness);
		}
	}

	public void show() {
		System.out.println("fitness: " + 1.0 / bestFitness);
		System.out.print("center: ");
		for (int i = 0; i < classNum; ++i) {
			for (int j = 0; j < featureNum; ++j)
				System.out.print(bestMemory[i][j] + ",");
			System.out.println();
		}
	}

	public void predict(JavaRDD<LabeledPoint> features) {
		features.mapToPair(v -> {
			int predictLabel = 0;
			double distance = Double.MAX_VALUE;
			for (int i = 0; i < classNum; ++i) {
				double temp = 0.0;
				for (int j = 0; j < featureNum; ++j)
					temp += Math.pow(v.features().apply(j) - bestMemory[i][j], 2.0);
				temp = Math.pow(temp, 0.5);
				if (distance > temp) {
					distance = temp;
					predictLabel = i;
				}
			}
			Map<String, Integer> tempMap = new HashMap<String, Integer>(){
				{
					put("0.0", 0);
					put("1.0", 0);
					put("2.0", 0);
				}
			};
			tempMap.put(Double.toString(v.label()), 1);
			return new Tuple2<Integer, Map<String, Integer>>(predictLabel, tempMap);
		}).reduceByKey((a, b) -> {
			for (String key : a.keySet()) {
				Integer valueA = a.get(key);
				Integer valueB = b.get(key);
				a.put(key, valueA + valueB);
			}
			return a;
		}).collectAsMap();
	}
}
