# Data Quality on Misspelled word and Outliers

## Description

This is a simple data quality checker on large scale dataset that goal for correcting the misspelling from the labeled data and cleaning out the outliers. The []() is the dataset being applied in here.

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

	source .bashrc

By doing so, `hfs` will be the abbreviation to check the file in Hadoop. Below are the few commands that most frequent used in Hadoop:
 

* CentOS 7.7 for production environment

Clone the repository:

    git clone https://github.com/GreatStephen/DSP-Lab.git

Install the dependecies from `requirements.txt`:

    pip install -r requirements.txt

Run the program:

    python effect.py

If you have questions, feel free to leave a message [here](https://github.com/GreatStephen/DSP-Lab/issues).