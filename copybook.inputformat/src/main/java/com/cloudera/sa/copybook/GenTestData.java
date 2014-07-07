package com.cloudera.sa.copybook;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map.Entry;
import java.util.Random;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.IFieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.Line;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.AbstractLineWriter;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;
import net.sf.JRecord.Types.Type;

public class GenTestData {
  public static void main(String[] args) throws Exception{
    String cbl = "example/example.cbl";
    String outputFile = "example/gen/example.dat";
    int numberOfRecords = 100;
    int numberOfRowsToSample = 100;
    
    if (args.length == 0) {
      System.out
          .println("GenTestData <cbl file> <output file> <numOfOutputRows> <numOfSampledRows> ");
      System.out.println("");
      System.out.println("Using defaults");
    } else {
      cbl = args[0];
      outputFile = args[1];
      numberOfRecords = Integer.parseInt(args[2]);
      numberOfRowsToSample = Integer.parseInt(args[3]);
    }
    int fileStructure = Constants.IO_FIXED_LENGTH;
    
    CopybookLoader copybookInt = new CobolCopybookLoader();
    ExternalRecord externalRecord = copybookInt.loadCopyBook(cbl,
        CopybookLoader.SPLIT_NONE, 0, "cp037", Convert.FMT_MAINFRAME, 0, null);

    CobolIoProvider ioProvider = CobolIoProvider.getInstance();
    
    System.out.println("Started output file");
    
    AbstractLineWriter writer  = ioProvider.getLineWriter(fileStructure, outputFile);
   
    Random r = new Random();
    
    for (int i = 0; i < numberOfRecords; i++) {
      AbstractLine line = new Line(externalRecord.asLayoutDetail());
      
      for (Entry<String, IFieldDetail> entry : externalRecord.asLayoutDetail().getRecordFieldNameMap().entrySet()) {
        
        int type = entry.getValue().getType();
        
        if (type == Type.ftChar) {
          StringBuilder str = new StringBuilder();
          
          for (int j = 0; j < entry.getValue().getLen(); j++) {
            str.append((char)(97 + r.nextInt(25)));
          }
          
          line.setField(entry.getValue(), str.toString());
        } else if (type == Type.ftPackedDecimal) {
          int len = entry.getValue().getLen();
          
          StringBuilder str = new StringBuilder();
          
          for (int j = 0; j < entry.getValue().getLen(); j++) {
            str.append((char)(48 + r.nextInt(9)));
          }
          
          line.setField(entry.getValue(), Long.parseLong(str.toString()));
        } else if (type == Type.ftBinaryBigEndian) {
          int len =  entry.getValue().getLen();
          
          StringBuilder str = new StringBuilder();
          
          for (int j = 0; j < entry.getValue().getLen(); j++) {
            str.append((char)(48 + r.nextInt(9)));
          }
          
          line.setField(entry.getValue(), Long.parseLong(str.toString()));
        } else if (type == Type.ftZonedNumeric) {
          int len = entry.getValue().getLen();
          
          StringBuilder str = new StringBuilder();
          
          for (int j = 0; j < entry.getValue().getLen(); j++) {
            str.append((char)(48 + r.nextInt(9)));
          }
          
          line.setField(entry.getValue(), Long.parseLong(str.toString()));
        }
      }
      
      writer.write(line);
    }
    writer.close();
    
    System.out.println("Closed output file");
    
    AbstractLineReader ret = LineIOProvider.getInstance().getLineReader(
        fileStructure,
        LineIOProvider.getInstance().getLineProvider(fileStructure));

    BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File(outputFile)));
    
    ret.open(input, externalRecord);

    for (int i = 0; i < numberOfRowsToSample; i++) {
      AbstractLine line = ret.read();
      
      boolean isFirst = true;
      StringBuilder str = new StringBuilder("[");
      int index = 0;
      for (ExternalField field : externalRecord.getRecordFields()) {
        
        
        if (isFirst) {
          isFirst = false;
        } else {
          str.append("|");
        }
        str.append(line.getFieldValue(0, index++));
      }
      System.out.println("Line." + i  + ":" + str.toString() + "]");
    }
    
  }
}
