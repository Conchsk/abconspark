package cn.wyj.abconspark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	SparkConf conf = new SparkConf().setAppName("test")
    			.setMaster("spark://J106-WYJ-UBT:7077")
    			.setJars(new String[] {
    					"/home/wyj/Softwares/spark-2.1.0-bin-hadoop2.7/workspace/abconspark.jar"
    				});
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    	System.out.println(rdd.reduce((a, b) -> a + b));
    	sc.close();
    }
}
