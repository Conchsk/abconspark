package cn.wyj.abconspark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class KDDMain 
{
    public static void main( String[] args )
    {
    	SparkSession ss = SparkSession.builder()
    			.appName("test")
    			.master("spark://J106-WYJ-UBT:7077")
    			.getOrCreate();    	
    	Dataset<Row> df = ss.read().format("libsvm").load();
    	ss.close();
    }
}
