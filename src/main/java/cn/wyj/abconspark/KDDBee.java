package cn.wyj.abconspark;

@SuppressWarnings("serial")
public class KDDBee implements java.io.Serializable {
	public double[][] memory;
	public double fitness;
	public int trial;

	public KDDBee() {

	}

	public KDDBee(double[][] memory, double fitness, int trial) {
		this.memory = memory;
		this.fitness = fitness;
		this.trial = trial;
	}
}
