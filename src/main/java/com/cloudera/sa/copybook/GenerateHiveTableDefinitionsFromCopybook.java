package com.cloudera.sa.copybook;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.Numeric.Convert;
import net.sf.JRecord.Types.Type;

public class GenerateHiveTableDefinitionsFromCopybook {
  public static void main(String[] args) throws Exception {
    String cbl = "cb.cbl";
    String outputFile = "cb.create.hive.table.txt";
    String tableName = "copybook_table";
    String externalLocation = "copybook/table";
    String copybookLocation = "copybool/cb.cbl";

    if (args.length == 0) {
      System.out
          .println("GenerateHiveTableDefinitionsFromCopybook <cbl file> <output file> <tableName> ");
      System.out.println("");
      System.out.println("Using defaults");
    } else {
      cbl = args[0];
      outputFile = args[1];
      tableName = args[2];
      externalLocation = args[3];
      copybookLocation = args[4];
    }

    CopybookLoader copybookInt = new CobolCopybookLoader();
    ExternalRecord externalRecord = copybookInt.loadCopyBook(cbl,
        CopybookLoader.SPLIT_NONE, 0, "cp037", Convert.FMT_MAINFRAME, 0, null);

    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
        outputFile)));

    writer.append("ADD JAR copybookInputFormat.jar;");
    writer.newLine();
    writer.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName);
    writer.newLine();
    writer.append("(");
    writer.newLine();

    boolean isFirst = true;

    int recordLength = 0;

    String lastColumn = "";
    int repeatCounter = 1;

    for (ExternalField field : externalRecord.getRecordFields()) {

      if (isFirst) {
        isFirst = false;
      } else {
        writer.append(",");
        writer.newLine();
      }
      int type = field.getType();
      String typeString = "Unknown";
      String hiveType = "STRING";

      if (type == Type.ftChar) {
        typeString = "ftChar";

      } else if (type == Type.ftPackedDecimal) {
        typeString = "packedDecimal";

      } else if (type == Type.ftBinaryBigEndian) {
        typeString = "BinaryBigEndian";
      } else if (type == Type.ftZonedNumeric) {
        typeString = "ftZonedNumeric";
      }

      System.out.println(field.getCobolName() + "\t" + typeString + "\t" + type
          + "\t" + field.getLen() + "\t" + field.getDescription() + "\t"
          + field.getCobolName());
      recordLength += field.getLen();
      String columnName = field.getCobolName();
      columnName = columnName.replace('-', '_');

      if (lastColumn.equals(columnName)) {
        columnName = columnName + repeatCounter++;
      } else {
        repeatCounter = 1;
        lastColumn = columnName;
      }

      writer.append("  " + columnName + " " + hiveType);

    }
    writer.newLine();
    writer.append(")");
    writer.newLine();
    writer.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '1'");
    writer.newLine();
    writer
        .append("STORED AS INPUTFORMAT 'com.cloudera.sa.copybook.mapred.CopybookInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");
    writer.newLine();
    writer.append("LOCATION '" + externalLocation + "'");
    writer.newLine();
    writer.append("TBLPROPERTIES ('copybook.inputformat.cbl.hdfs.path' = '"
        + copybookLocation + "')");
    writer.close();
    // ROW FORMAT DELIMITED FIELDS TERMINATED BY '1'\nSTORED AS INPUTFORMAT
    // 'com.cloudera.sa.mainframeinputformat.MainframeInputFormat' OUTPUTFORMAT
    // 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\nLOCATION '"
    // + sys.argv[3] + "'\nTBLPROPERTIES ('mainframe.copybook.condensed' = '" +
    // copybook + "'
    System.out.println("recordLength:" + recordLength);
  }
}
