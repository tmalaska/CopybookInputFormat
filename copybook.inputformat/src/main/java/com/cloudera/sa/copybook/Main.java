package com.cloudera.sa.copybook;

import org.apache.hadoop.hive.ql.io.RCFileInputFormat;

public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      printHelp();
      return;
    }
    
    String[] subArgs = new String[args.length - 1];
    System.arraycopy(args, 1, subArgs, 0, subArgs.length);
    
    if (args[0].equals("BasicCopybookConvert")) {
      BasicCopybookConvert.main(subArgs);
    } else if (args[0].equals("GenerateHiveTableDefinitionsFromCopybook")) {
      GenerateHiveTableDefinitionsFromCopybook.main(subArgs);
    } else if (args[0].equals("PrepCopybook")) {
      PrepCopybook.main(subArgs);
    } else {
      printHelp();
      RCFileInputFormat g;
    }
  }

  private static void printHelp() {
    System.out.println("Help:");
    System.out.println("Main {command} {command parameters ...}");
    System.out.println("Commands List");
    System.out.println("BasicCopybookConvert: This is will convert a dat file with a cbl definition.  Single threaded.");
    System.out.println("GenerateHiveTableDefinitionsFromCopybook: This will generate a hive table based on the copy book.");
    System.out.println("PrepCopybook: This will convert the copy book that works with JRecord.");
  }
}
