package cn.wyj.abconspark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class KDDMain 
{
    public static void main( String[] args )
    {
    	SparkSession spark = SparkSession.builder()
    			.appName("test")
    			.master("spark://J106-WYJ-UBT:7077")
    			.getOrCreate();
    	JavaRDD<LabeledPoint> rdd = MLUtils.loadLibSVMFile(spark.sparkContext(),
    			"hdfs://localhost:9000/user/wyj/kddcup/kddcup.data_8*512_all").toJavaRDD();
    	Dataset<Row> data = spark.createDataFrame(rdd, LabeledPoint.class);
    	MinMaxScaler scaler = new MinMaxScaler()
    			.setInputCol("features")
    			.setOutputCol("scaledFeatures");
    	MinMaxScalerModel scalerModel = scaler.fit(data);
    	Dataset<Row> scaledData = scalerModel.transform(data);
    	JavaRDD<LabeledPoint> scaledRdd = scaledData.javaRDD().map(
    			value -> new LabeledPoint(value.getDouble(0), (Vector) value.get(2)));
    	MLUtils.saveAsLibSVMFile(scaledRdd.rdd(),
    			"hdfs://localhost:9000/user/wyj/kddcup/kddcup.data_8*512_all_scale");
    	spark.close();
    }
}
