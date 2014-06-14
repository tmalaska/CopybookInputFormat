package com.cloudera.sa.copybook;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class PrepCopybook {
  public static void main(String[] args) throws IOException {
    String cbl = "cb_comp3.cbl";
    String outputFile = "cb.prep.cbl";

    if (args.length == 0) {
      System.out
          .println("PrepCopybook <cbl file> <output file>");
      System.out.println("");
      System.out.println("Using defaults");
    } else {
      cbl = args[0];
      outputFile = args[1];
    }
    
    BufferedReader reader = new BufferedReader(new FileReader(new File(cbl)));
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputFile)));
    
    String line;
    
    boolean isFirst = true;
    int fillerCounter = 0;
    
    while ((line = reader.readLine()) != null) {
      if (isFirst) {
        isFirst = false;
      } else {
        writer.newLine();
      }
      
      line = line.replace(" FILLER ", " FILLER" + fillerCounter++ + " ");
      line = line.replace("-", "_");
      line = line.replace(" COMP_3.", " COMP-3.");
      line = line.replace(" COMP_4.", " COMP-4.");
      line = line.replace(" COMP_5.", " COMP-5.");
      line = line.replace(" PACKED_DECIMAL.", " COMP-3.");
      
      writer.append(line);
    }
    
    writer.close();
    reader.close();
  }
}
