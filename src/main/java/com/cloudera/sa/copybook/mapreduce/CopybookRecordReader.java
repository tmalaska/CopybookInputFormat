package com.cloudera.sa.copybook.mapreduce;

import java.io.BufferedInputStream;
import java.io.IOException;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sa.copybook.Const;

public class CopybookRecordReader extends RecordReader<LongWritable, Text> {

  private int recordByteLength;
  private long start;
  private long pos;
  private long end;

  private LongWritable key = null;
  private Text value = null;

  AbstractLineReader ret;
  ExternalRecord externalRecord;
  private static String fieldDelimiter = new Character((char) 0x01).toString();
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    String cblPath = context.getConfiguration().get(
        Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);

    FileSystem fs = FileSystem.get(context.getConfiguration());

    BufferedInputStream inputStream = new BufferedInputStream(fs.open(new Path(
        cblPath)));

    CobolCopybookLoader copybookInt = new CobolCopybookLoader();
    try {
      externalRecord = copybookInt
          .loadCopyBook(inputStream, "RR", CopybookLoader.SPLIT_NONE, 0,
              "cp037", Convert.FMT_MAINFRAME, 0, null);

      int fileStructure = Constants.IO_FIXED_LENGTH;

      for (ExternalField field : externalRecord.getRecordFields()) {
        recordByteLength += field.getLen();
      }

      // jump to the point in the split that the first whole record of split
      // starts at
      FileSplit fileSplit = (FileSplit) split;

      start = fileSplit.getStart();
      end = start + fileSplit.getLength();
      final Path file = fileSplit.getPath();

      BufferedInputStream fileIn = new BufferedInputStream(fs.open(fileSplit
          .getPath()));

      if (start != 0) {
        pos = start - (start % recordByteLength) + recordByteLength;

        fileIn.skip(pos);
      }

      ret = LineIOProvider.getInstance().getLineReader(
          fileStructure,
          LineIOProvider.getInstance().getLineProvider(fileStructure));

      ret.open(fileIn, externalRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (pos >= end) {
      return false;
    }

    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new Text();
    }

    AbstractLine line = ret.read();

    if (line == null) {
      return false;
    }

    pos += recordByteLength;

    key.set(pos);

    StringBuilder strBuilder = new StringBuilder();

    boolean isFirst = true;
    int i = 0;
    for (ExternalField field : externalRecord.getRecordFields()) {
      if (isFirst) {
        isFirst = false;
      } else {
        strBuilder.append(fieldDelimiter);
      }
      strBuilder.append(line.getFieldValue(0, i++));
    }

    value.set(strBuilder.toString());
    key.set(pos);

    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {

    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  @Override
  public void close() throws IOException {
    ret.close();
  }

}
