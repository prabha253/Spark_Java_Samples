package Sample.first_java_example;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import java.util.Arrays;
import static com.google.common.base.Preconditions.checkArgument;

public class WordCount {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(WordCount.class);
	
	public static void main(String args[]) {
		
		
		//checkArgument(args.length >1,"");
		WordCount s1=new WordCount();
		String inputFile="F:/c3/Hive_Properties.txt";
		s1.run(inputFile);
				//.run(args[1]);
	}

	private void run(String inputFile) {
		// TODO Auto-generated method stub
				
		String master="local[*]";
		SparkConf conf =new SparkConf().setAppName(WordCount.class.getName()).setMaster(master);
		JavaSparkContext context=new JavaSparkContext(conf);
		
		context.textFile(inputFile).flatMap(text ->Arrays.asList(text.split(" ")).iterator()).mapToPair(word ->new Tuple2<>(word,1))
		.reduceByKey((a,b) -> a+b)
		.foreach(result ->LOGGER.info(String.format("Word[%] Count [%d].", result._2)));
		
		
	}
	
	
	
	

}
