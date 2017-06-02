package cn.wyj.abconspark;

@SuppressWarnings("serial")
public class Bee<T> implements java.io.Serializable {
	public T memory;
	public double fitness;
	public int trial;

	public Bee() {

	}

	public Bee(T memory, double fitness, int trial) {
		this.memory = memory;
		this.fitness = fitness;
		this.trial = trial;
	}
}
