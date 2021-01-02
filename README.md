# Data Quality on Misspelled word and Outliers

## Description

This is a simple data quality checker on large scale dataset using **PySpark**. The goal is correcting the misspelling from the labeled data and cleaning out the outliers. In this repository, Levenshtein distance is the main method for the former and the K-means is for the latter.


### Dataset

The repository will use [Office of Payroll Administration](https://data.cityofnewyork.us/City-Government/Citywide-Payroll-Data-Fiscal-Year-/k397-673e) as the dataset. It consists of 3.9M rows and 17 columns with different types which listed below:
* Integer: `Fiscal Year`, `Payroll Number`
* Double: `Base Salary`, `Regular Hours`, `Regular Gross Paid`, `OT Hours`, `Total OT Paid`, `Total Other Pay`
* String: `Agency Name`, `Last Name`, `First Name`, `Mid Init`, `Agency Start Date`, `Work Location Borough`, `Title Description`, `Leave Status as of June 30`, `Pay Basis`

This repository only briefly explain *Misspelling on the Labeled Data* and *Outliers on Numerical Data*. For more detail please click [here](https://github.com/samsh19/data-quality/blob/main/docs/DataQualityAssessment.pdf/) for reference

### Misspelling on the labeled data
For example, the column, `Agency Name`, has the the following values: *BOARD OF CORRECTION* vs. *BOARD OF CORRECTIONS* and *BRONX COMMUNITY BOARD #1* vs. *BRONX COMMUNITY BOARD #10*. Obviously, the former example show the *s* difference, the latter example has the same alphabet letters but different numbers.

We can regard the *s* difference as the misspelling but the number difference might depend on the definition. To solve these, this repository will group the similar words together and make the word list. For alphabet difference, the most frequent word in one word set will be regard as the correct word; otherwise, keep the same letter as the correct word.

### Outliers on Numerical Data
For outliers, this repository first sampling different data from the original dataset to choose the optimal K for K-means clustering. Then, apply the optimal K to all the data for clustering. Here, the cluster centers and the distance from these centers to the corresponding data point in each cluster can be attained. Finally, the quartilie range method is applied in here. If the distance is larger than Q3+1.5\*(Q3-Q1), the data point will be regard as the outlier.

## Local Deployment

### Environment
The code is implemented under `NYU Dumbo`. Before running the program, make sure to conduct the following steps:

Create a `.bashrc` file with the following code:

	HADOOP_EXE='/usr/bin/hadoop'
	HADOOP_LIBPATH='/opt/cloudera/parcels/CDH/lib'
	HADOOP_STREAMING='hadoop-mapreduce/hadoop-streaming.jar'
	
	alias hfs="$HADOOP_EXE fs"
	alias hjs="$HADOOP_EXE jar $HADOOP_LIBPATH/$HADOOP_STREAMING"
	export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
	export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'

Conduct bashrc:
	
	source .bashrc

By doing so, you can use the `hfs` to manipulate the file in Hadoop with following sample commands:
	
	hfs -ls
	hfs -rm -r <filename>
	hfs -get <filename>
	hfs -getmerge <filename> <filename>

Conduct bashrc:

	module load python/gnu/3.6.5
	module load spark/2.4.0

If you want to use the spark environment, type `pyspark` (if the MLlib package cannot be implemented under the spark environment, try enter `pyspark2` instead)

Note that all these command and tutorial are from the class **Big Data** class taught by **Prof. Juliana Freire**.

### Run the Program