#CopybookInputFormat
===============
##Overview
This project has a collections of tools to allow you to read directly from copybook data files in HDFS, using Map/Reduce, Hive, or Spark

Here is what is in this project:
* BasicCopybookConvert: Example of how to read a copybook data file with the copybook schema with JRecord.  This is single threaded.
* PrepCopybook: This tool with clean up a copybook file so it will work with Hive and JRecord.
* GenTestData: This will take a given cpl file and create sample rows for testing
* GenHiveCreateTable: This will read the copybook schema and generate a Hive table definition.
* mapred.InputFormat & RecordReader: This is an mapped implementation of FileInputFormat and RecordReader.  It also works with Hive.
* mapreduce.InputFormat & RecordReader: This is an mapped implementation of FileInputFormat and RecordReader.  
* Spark Exampl: An example of how to read a cpl data from with Spark.

##Build
JRecord is not on a public maven repo so I have included the JRecord jars.  To build you have to put these jars in your local repo under the following folders

~/.m2/repository/net/sf/JRecord/JRecord/0.80/JRecord-0.80.jar

~/.m2/repository/net/sf/cb2xml/cb2xml/1.0/cb2xml-1.0.jar

After you do that just do maven package and use target/copybookInputFormat.jar

##Credits
Sekou Mckissick, Susan Greslik, Gwen Shapira, Jeremy Beard, and Ted Malaska

##Internal Notes
add jar copybookInputFormat.jar;

set copybook.inputformat.cbl.hdfs.path=cb.cbl;

<property>
<name>hive.aux.jars.path</name>
<value>hdfs:///user/root/copybook-0.0.1-SNAPSHOT.jar</value>
</property>



