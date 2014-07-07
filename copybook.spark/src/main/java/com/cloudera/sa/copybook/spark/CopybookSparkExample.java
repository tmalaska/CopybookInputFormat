package com.cloudera.sa.copybook.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.cloudera.sa.copybook.mapreduce.CopybookInputFormat;

public class CopybookSparkExample {
	public static void main(String[] args) {
		if (args.length == 0) {

		}
		if (args.length == 0) {
			System.out
					.println("UniqueSeqGenerator {master} {copybookInputPath} {dataFileInputPath} {outputFolder}");
			return;
		}

		String master = args[0];
		String copybookInputPath = args[1];
		String dataFileInputPath = args[2];
		String outputPath = args[3];

		JavaSparkContext jsc = new JavaSparkContext(master,
				"UniqueSeqGenerator", null, "SparkCopybookExample.jar");

		Configuration config = new Configuration();
		CopybookInputFormat.setCopybookHdfsPath(config, copybookInputPath);
		
		JavaPairRDD<LongWritable, Text> rdd = jsc.newAPIHadoopFile(dataFileInputPath, CopybookInputFormat.class, LongWritable.class, Text.class, config);
		JavaRDD<String> pipeDelimiter = rdd.map(new MapFunction());

		pipeDelimiter.saveAsTextFile(outputPath);
	}
	
	public static class MapFunction extends Function<Tuple2<LongWritable, Text>, String> {

		@Override
		public String call(Tuple2<LongWritable, Text> line) throws Exception {
			String[] cells = line._2.toString().split(new Character((char) 0x01).toString());
			
			StringBuilder strBuilder = new StringBuilder();
			
			for (String cell: cells) {
				strBuilder.append(cell + "|");
			}
			return strBuilder.toString();
		}
		
	}
}
