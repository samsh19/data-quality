# Data Quality on Misspelled word and Outliers

## Description

This is a simple data quality checker on large scale dataset that goal for correcting the misspelling from the labeled data and cleaning out the outliers. The [Office of Payroll Administration]() is the dataset being applied in here.

### Dataset

### Misspelling on the labeled data

### Outliers


## Local Deployment

### Environment
The code is implemented under NYU Dumbo. Before running the program on Dumbo, make sure to conduct the following steps:

* Create a `.bashrc` file with the following code:

	HADOOP_EXE='/usr/bin/hadoop'
	HADOOP_LIBPATH='/opt/cloudera/parcels/CDH/lib'
	HADOOP_STREAMING='hadoop-mapreduce/hadoop-streaming.jar'
	
	alias hfs="$HADOOP_EXE fs"
	alias hjs="$HADOOP_EXE jar $HADOOP_LIBPATH/$HADOOP_STREAMING"
	export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
	export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'

* Conduct bashrc:

>source .bashrc

By doing so, you can use the `hfs` to manipulate the file in Hadoop. Below are the few commands that most frequent used in Hadoop:
	
	hfs -ls
	hfs -rm -r <filename>
	hfs -get <filename>
	hfs -getmerge <filename> <filename>

* Conduct bashrc:

>module load python/gnu/3.6.5
>module load spark/2.4.0

If you want to enter in the spark environment, type `pyspark2` or `pyspark` (some of the MLlib package cannot be implemented in `pyspark`, so enter `pyspark2` instead)

Note that all these command and tutorial are all from the class **Big Data** taught by **Prof. Juliana Freire**.

### Run Program