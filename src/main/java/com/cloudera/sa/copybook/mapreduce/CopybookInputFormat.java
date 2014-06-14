package com.cloudera.sa.copybook.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CopybookInputFormat extends FileInputFormat<LongWritable, Text>{

	  @Override
	  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
	      throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    return new CopybookRecordReader();
	  }


}
