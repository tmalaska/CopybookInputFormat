package com.cloudera.sa.copybook.mapred;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.ToLayoutDetail;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.IO.LineReaderWrapper;
import net.sf.JRecord.Numeric.Convert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.cloudera.sa.copybook.Const;

public class CopybookRecordReader implements RecordReader<LongWritable, Text> {

  private int recordByteLength;
  private long start;
  private long pos;
  private long end;

  AbstractLineReader ret;
  ExternalRecord externalRecord;
  
  private static String fieldDelimiter = new Character((char) 0x01).toString();

  public CopybookRecordReader(FileSplit genericSplit, JobConf job)
      throws IOException {
    try {
      String cblPath = job.get(Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);

      if (cblPath == null) {
        if (job != null) {
          MapWork mrwork = Utilities.getMapWork(job);

          if (mrwork == null) {
            System.out.println("When running a client side hive job you have to set \"copybook.inputformat.cbl.hdfs.path\" before executing the query." );
            System.out.println("When running a MR job we can get this from the hive TBLProperties" );
          }
          Map<String, PartitionDesc> map = mrwork.getPathToPartitionInfo();
          
          for (Map.Entry<String, PartitionDesc> pathsAndParts : map.entrySet()) {
            System.out.println("Hey");
            Properties props = pathsAndParts.getValue().getProperties();
            cblPath = props
                .getProperty(Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);
            break;
          }
        }
      }

      FileSystem fs = FileSystem.get(job);
      BufferedInputStream inputStream = new BufferedInputStream(
          fs.open(new Path(cblPath)));
      CobolCopybookLoader copybookInt = new CobolCopybookLoader();
      externalRecord = copybookInt
          .loadCopyBook(inputStream, "RR", CopybookLoader.SPLIT_NONE, 0,
              "cp037", Convert.FMT_MAINFRAME, 0, null);

      int fileStructure = Constants.IO_FIXED_LENGTH;

      for (ExternalField field : externalRecord.getRecordFields()) {
        recordByteLength += field.getLen();
      }

      // jump to the point in the split that the first whole record of split
      // starts at
      FileSplit split = (FileSplit) genericSplit;

      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();

      BufferedInputStream fileIn = new BufferedInputStream(fs.open(split
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
      e.printStackTrace();
    } 

  }

  public boolean next(LongWritable key, Text value) throws IOException {
    
    System.out.println("next");
    try {
    if (pos >= end) {
      return false;
    }

    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new Text();
    }

    // int result = fileIn.read(mainframeRecord);

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
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public Text createValue() {
    return new Text();
  }

  public long getPos() throws IOException {
    return pos;
  }

  public void close() throws IOException {
    ret.close();
  }

  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

}
