"""
Author: Yu-Lin Shen
        note that several code is sourced from stackoverflow, github, pyspark repository, blog
"""

from argparse import ArgumentParser
import pandas as pd
import numpy as np
from collections import Counter, defaultdict

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.types import FloatType
from scipy.spatial import distance
from functools import reduce
from pyspark.sql import DataFrame

from outliers import *
from misspelling import *

parser = ArgumentParser()
parser.add_argument("-f", "--function", dest="function") # misspelling or outliers
parser.add_argument("-i", "--inputFileName", dest="inputFileName") # "Citywide_Payroll_Data__Fiscal_Year_.csv"
parser.add_argument("-mis_col", "--misspelling_target_column", dest="misspelling_target_column") # "Agency Name"
parser.add_argument("-mis_o", "--misspelling_correctness_filename", dest="misspelling_correctness_filename") # "Citywide_Payroll_Data__Fiscal_Year_agency_name_misvalue.csv"
parser.add_argument("-mis_lev_dist", help="misspelling_levenshtein_distance", type=int) # 2

parser.add_argument("-out_K", "--outliers_optimal_K_checker", dest="outliers_optimal_K_checker") # "Citywide_Payroll_Data__Fiscal_Year_choose_k.csv"
parser.add_argument("-out_f", "--outliers_function", dest="outliers_function") # "optimal K" or "find outliers"
parser.add_argument("-out_rate", help="--outliers_output_find_K_sample_rate", type=float) # 0.0005
parser.add_argument("-out_size", help="--outliers_times_rerun_elbow_silhouette", type=int) # 5
parser.add_argument("-out_ktop", help="--outliers_max_K_checker", type=int) # 10

parser.add_argument("-out_opt_k", help="--outliers_optimal_K", type=int) # 4
parser.add_argument("-out_o", "--outliers_output_df", dest="outliers_output_df") # "Citywide_Payroll_Data__Fiscal_Year_outliers.csv"
parser.add_argument("-out_c", "--outliers_center_list", dest="outliers_center_list") # "Citywide_Payroll_Data__Fiscal_Year_center_list.csv"

args = parser.parse_args()

# import dataset
df = spark.read.format("csv").options(header="true", inferschema="true").load(args.inputFileName)
df.createOrReplaceTempView("df")
for colname in df.columns:
    df = df.withColumnRenamed(colname, "_".join(colname.split(" "))) # handle the column name with space
# add index to the table
df = df.select("*").withColumn("id", monotonically_increasing_id())

# run function
if args.function == "misspelling":
	
	# misspelling
    _, df_correctness = missepelling(df, args.misspelling_target_column, levenshtein_distance=args.mis_lev_dist)
    df_correctness.coalesce(1).write.format("csv").option("header", "True").mode("append").save(args.misspelling_correctness_filename)

elif args.function == "outliers":
	# outliers
    
    # normalize data
    column_name_list = [i[0] for i in df.dtypes if i[1]=="double"]
    df = df_normalization(df, column_name_list, float_decimal=5)
    
    # get feature column
    scaled_column = [col for col in df_vec.columns if "scaled" in col]
    df = get_feature(df, scaled_column)
    df = df.select("id", "feature")
    
    if args.outliers_function=="optimal K":
	    # pick k for clustering

	    df_pick_k = pick_k(df, column_name_list, sample_rate=args.out_rate, sample_size=args.out_size, ktop=args.out_ktop)
	    # note: remove the original file before save these file
	    df_pick_k.coalesce(1).write.format("csv").option("header", "True").mode("append").save(args.outliers_optimal_K_checker)

    elif args.outliers_function=="find outliers":
	    # run k-means
	    optimalK = args.out_opt_k
	    new_df_kmeans_transformed, model = kmeans(df, optimalK)

	    # compute euclidean distance
	    outlier_pdf, center_pdf = euclidean_distance(new_df_kmeans_transformed, optimalK, model)
	    outlier_pdf.coalesce(1).write.format("csv").option("header", "True").mode("append").save(args.outliers_output_df)
	    center_pdf.coalesce(1).write.format("csv").option("header", "True").mode("append").save(args.outliers_center_list)
    else:
    	print('Wrong function assignment in outliers, try "optimal K" or "find outliers"')
else:
	print('Wrong function assignment, try "misspelling" or "outliers"')