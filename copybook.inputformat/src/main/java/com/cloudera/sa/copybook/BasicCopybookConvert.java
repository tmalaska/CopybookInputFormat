package com.cloudera.sa.copybook;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.Numeric.Convert;

public class BasicCopybookConvert 
{
    public static void main( String[] args ) throws Exception
    {
    	String cbl = "cb.cbl";
    	String dat = "cb.dat";
    	String outputFile = "cb.ascii.converted";
    	
    	if (args.length == 0 ) {
    		System.out.println("BasicCopybookConvert <cbl file> <data file> <output file>");
    		System.out.println("");
    		System.out.println("Using defaults");
    	} else {
    		cbl = args[0];
    		dat = args[1];
    		outputFile = args[2];
    	}
    	
    	
    	int fileStructure = Constants.IO_FIXED_LENGTH;
    	CobolIoProvider ioProvider = CobolIoProvider.getInstance();
    	AbstractLineReader reader  = ioProvider.getLineReader(
    			   fileStructure, Convert.FMT_MAINFRAME,
    			                              CopybookLoader.SPLIT_NONE, cbl, dat
    			                      );
    	
    	CopybookLoader copybookInt = new CobolCopybookLoader();
      ExternalRecord externalRecord = copybookInt.loadCopyBook(cbl, CopybookLoader.SPLIT_NONE, 0, "cp037",
          Convert.FMT_MAINFRAME, 0, null);
    	
    	AbstractLine saleRecord;
    	
    	System.out.println(reader.getClass());
    	
    	BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputFile)));
    	
    	while ((saleRecord = reader.read()) != null) {
    		
    	  int i = 0;
    	  for (ExternalField field: externalRecord.getRecordFields()) {
    	    writer.append(saleRecord.getFieldValue(0, i++).toString() + "\t");
    	  }
    						   
    		writer.newLine();
    	}
    	writer.close();
    	reader.close();
    	
    }
}
