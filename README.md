# Data Quality on Misspelled word and Outliers

## Description

This is a simple data quality checker on the large-scale dataset using **PySpark**. The goal is correcting the misspelling from the labeled data and cleaning out the outliers. In this repository, Levenshtein distance is the main method for the former and the K-means is for the latter.


### Dataset

The repository will use [Office of Payroll Administration](https://data.cityofnewyork.us/City-Government/Citywide-Payroll-Data-Fiscal-Year-/k397-673e) as the dataset. It consists of 3.9M rows and 17 columns with different types listed below:
* Integer: `Fiscal Year`, `Payroll Number`
* Double: `Base Salary`, `Regular Hours`, `Regular Gross Paid`, `OT Hours`, `Total OT Paid`, `Total Other Pay`
* String: `Agency Name`, `Last Name`, `First Name`, `Mid Init`, `Agency Start Date`, `Work Location Borough`, `Title Description`, `Leave Status as of June 30`, `Pay Basis`

This repository only briefly explains *Misspelling on the Labeled Data* and *Outliers on Numerical Data*. For more detail please click [here](https://github.com/samsh19/data-quality/blob/main/docs/DataQualityAssessment.pdf/) for reference

### Misspelling on the labeled data

For example, the column, `Agency Name`, has the following values: `BOARD OF CORRECTION` vs. `BOARD OF CORRECTIONS` and `BRONX COMMUNITY BOARD #1` vs. `BRONX COMMUNITY BOARD #10`. Obviously, the former example shows the `s` difference, the latter example has the same alphabet letters but different numbers.

We can regard the *s* difference as the misspelling but the number difference might depend on the definition. To solve these, this repository will group similar words and make the word list. For alphabet difference, the most frequent word in one word set will be regarded as the correct word; otherwise, keep the same letter as the correct word.

### Outliers on Numerical Data

For outliers, this repository first sampling different data from the original dataset to choose the optimal K for K-means clustering. Then, apply the optimal K to all the data for clustering. Here, the cluster centers and the distance from these centers to the corresponding data point in each cluster can be attained. Finally, the interquartile range method is applied here. If the distance is larger than Q3+1.5\*(Q3-Q1), the data point will be regarded as the outlier.

## Local Deployment

### Environment
The code is implemented under `NYU Dumbo`. Before running the program, make sure to conduct `.bashrc` with the following command:

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

If you want to use the spark environment, type `pyspark` (if the MLlib package cannot be implemented under the spark environment, try to enter `pyspark2` instead)

Note that all these commands and tutorials are from the class **Big Data** class taught by **Prof. Juliana Freire**.

### Run the Program

Misspelling for labeled data:

	python run.py -f misspelling -i <inputDataset> -mis_o <correctOutput> -mis_col <targetColumn> -mis_lev_dist <number>

>`inputDataset`: the input file name is "Citywide_Payroll_Data__Fiscal_Year_.csv"
>`correctOutput`: the output file name for the misspelling output
>`targetColumn`: the target column for misspelling checker, such as "Agency Name"
>`number`: the filter number for Levenshtein distance, which can be set as 2

Outliers:

For optimal K in K-means:
	
	python run.py -f outliers -i <inputDataset> -out_f "optimal K" -out_K <outputKchecker> -out_rate <sampleRate> -out_size <checkSize> -out_ktop <topNumCluster>

>`inputDataset`: the input file name is "Citywide_Payroll_Data__Fiscal_Year_.csv"
>`outputKchecker`: the output file name for the result of the elbow cost and silhouette_list methods
>`sampleRate`: the sample rate of the original dataset
>`checkSize`: the times for run the K-means checker
>`topNumCluster`: the maximum number of the cluster to run the K-means checker

For outliers search:
	
	python run.py -f outliers -i <inputDataset> -out_f "find outliers" --out_opt_k <optimalK> -out_c <clusterCenter> -out_o <outliersOutput>

>`inputDataset`: the input file name is "Citywide_Payroll_Data__Fiscal_Year_.csv"
>`optimalK`: the optimal number for K-means, which can be set as 4
>`clusterCenter`: the file name for the output cluster center list
>`outliersOutput`: the file name for the outliers output

If you have questions, feel free to leave a message [here](https://github.com/samsh19/data-quality/issues).