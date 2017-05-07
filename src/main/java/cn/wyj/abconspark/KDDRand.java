package cn.wyj.abconspark;

import java.util.Random;

@SuppressWarnings("serial")
public class KDDRand implements java.io.Serializable {
	public static Random rand = new Random(1234L);
	
	public static synchronized int nextInt(int range) {
		return rand.nextInt(range);
	}
	
	public static synchronized double nextDouble() {
		return rand.nextDouble();
	}
}
