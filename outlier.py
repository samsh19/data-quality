"""
This is the code to find out the outlier.
Author: Yu-Lin Shen
        note that several code is sourced from stackoverflow, github, pyspark repository, blog
"""
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

import pandas as pd
import numpy as np
from collections import Counter, defaultdict

def calculate_bounds(df, check_type="double"):
    """
    Input:
    df: pyspark dataframe
    colname: string

    Output:
    bounds: boundary of quartile range with max, min, Q1, Q3
    """
    bounds = {col:dict(zip(["q1", "q3"], df.approxQuantile(col, [0.25, 0.75], 0))) for col, dtype in zip(df.columns, df.dtypes) if dtype[1] == check_type}
    for col in bounds:
        iqr = bounds[col]["q3"]-bounds[col]["q1"]
        bounds[col]["min"] = bounds[col]["q1"]-(iqr * 1.5)
        bounds[col]["max"] = bounds[col]["q3"]+(iqr * 1.5)
    return bounds 

def quantile_outlier(df, column_name_list, boundary, union=True):
    """
    Input:
    df: pyspark dataframe
    column_name_list: list
    boundary: dict, from calculate_bounds 
    union: bool, in each outlier row from different column condition, union on the condition: True; intersection the condition: False  

    Output:
    df: pyspark dataframe, outlier in quartile range method
    """
    filter_format = []
    for colname in column_name_list:
        filter_format.append("({0} > {1} or {0} < {2})".format(colname, boundary[colname]["max"], boundary[colname]["min"]))
    join_factor = " or " if union else " and "
    spark_sql_format = join_factor.join(filter_format)
    return df.filter(spark_sql_format)

def df_normalization(df, column_name_list, float_decimal=5):
    """
    Input:
    df: pyspark dataframe
    column_name_list: list
    float_decimal: int

    Output:
    df: pyspark dataframe, normalized dataframe
    """
    df = df.select(*(["id"]+column_name_list))
    unlist = udf(lambda x: round(float(list(x)[0]), float_decimal), DoubleType()) # UDF for converting column type from vector to double type
    for col in column_name_list: # Iterating over columns to be scaled
        assembler = VectorAssembler(inputCols=[col], outputCol=col+"_vec") # VectorAssembler Transformation - Converting column to vector type
        scaler = MinMaxScaler(inputCol=col+"_vec", outputCol=col+"_scaled") # MinMaxScaler Transformation
        pipeline = Pipeline(stages=[assembler, scaler]) # Pipeline of VectorAssembler and MinMaxScaler
        df = pipeline.fit(df).transform(df).withColumn(col+"_scaled", unlist(col+"_scaled")).drop(col+"_vec") # Fitting pipeline on dataframe
    return df

def pick_k(df_vec, sample_rate=0.0005, sample_size=5, ktop=10):
    """
    Input:
    df: pyspark dataframe
    sample_rate: float, the ratio rate of sampling df
    sample_size: int, how many time to run the elbow cost and silhouette_list methods
    ktop: int, the top k range for evaluation

    Output:
    df: pyspark dataframe, result for elbow cost and silhouette_list methods
    """
    
    choose_k_list = []
    for seed in range(sample_size):
        df_sample = df_vec.sample(False, sample_rate, seed=seed) # withReplacement: False
        elbow_cost = []
        silhouette = []
        for k in range(2, ktop+1):
            kmeans = KMeans(k=k, seed=seed)
            tmp_model = kmeans.fit(df_sample)
            elbow_cost.append(tmp_model.summary.trainingCost)
            predictions = tmp_model.transform(df_sample)
            evaluator = ClusteringEvaluator()
            silhouette.append(evaluator.evaluate(predictions))
            choose_k_list.append([seed, k, elbow_cost[-1], silhouette[-1]])
    return spark.createDataFrame(pd.DataFrame(choose_k_list, columns = ["seed", "k", "elbow_cost", "silhouette"]))

def get_feature(df, scaled_column): # transfer to "features" for pyspark ML
    """
    Input:
    df: pyspark dataframe
    scaled_column: list, the column that need to be feature

    Output:
    df: pyspark dataframe
    """
    vecAssembler = VectorAssembler(inputCols=scaled_column, outputCol="features")
    return vecAssembler.transform(df).select("id", "features")

def kmeans(df, optimalK, seed=1):
    """
    Input:
    df: pyspark dataframe
    optimalK: int, k for cluster
    seed: int, random seed
    
    Output:
    new_df_kmeans_transformed: pyspark dataframe, with the cluster prediction
    model: k-means model
    """
    
    # transfer to "features" for pyspark ML
    scaled_column = [col for col in df.columns if "scaled" in col]
    df_vec = get_feature(df, scaled_column)
    
    kmeans = KMeans(k=optimalK, seed=seed)
    model = kmeans.fit(df_vec)
    new_df_kmeans_transformed = model.transform(df_vec)
    return new_df_kmeans_transformed, model

def euclidean_distance(new_df_kmeans_transformed, optimalK, model):
    """
    Input:
    new_df_kmeans_transformed: pyspark dataframe, with the prediction value
    optimalK: int, k for cluster
    model: model, trained k-means model
    
    Output:
    outlier_pdf: pyspark dataframe, outlier row
    center_pdf: pyspark dataframe, cluster center
    """
    new_df_kmeans_quantile_outlier_df_list = []
    new_df_kmeans_pred_class_bound = {}
    center_list = model.clusterCenters()
    for class_idx in range(optimalK):
        new_df_kmeans_pred_class_idx = new_df_kmeans_transformed.filter(new_df_kmeans_transformed["prediction"]==class_idx)
        fixed_entry = list(center_list[class_idx])
        distance_udf = F.udf(lambda x: float(distance.euclidean(x, fixed_entry)), FloatType())
        new_df_kmeans_pred_class_idx = new_df_kmeans_pred_class_idx.withColumn('distances', distance_udf(F.col('features')))
        new_df_kmeans_pred_class_bound[class_idx] = calculate_bounds(new_df_kmeans_pred_class_idx.select("distances"), check_type="float")
        new_df_kmeans_quantile_outlier_df_list.append(quantile_outlier(new_df_kmeans_pred_class_idx.filter(new_df_kmeans_pred_class_idx["prediction"]==class_idx), ["distances"], new_df_kmeans_pred_class_bound[class_idx]))
    df_concat = reduce(DataFrame.unionAll, new_df_kmeans_quantile_outlier_df_list)
    # dump all the outlier row
    outlier = df_concat.collect()
    outlier_pdf = spark.createDataFrame(pd.DataFrame([[i["id"], i["prediction"], i["distances"]] for i in outlier], columns = ["id", "prediction", "distances"]))
    
    # dump all the cluster center
    center_list = model.clusterCenters()
    center_pdf = spark.createDataFrame(pd.DataFrame([",".join(i.astype(str)) for i in center_list], columns = ["center_list"]))

    return outlier_pdf, center_pdf
    """
    test.coalesce(1).write.format("csv").option("header", "True").mode("append").save("df_outlier_Citywide_Payroll_Data__Fiscal_Year_.csv")
    center_pdf.coalesce(1).write.format("csv").option("header", "True").mode("append").save("center_list_Citywide_Payroll_Data__Fiscal_Year_.csv")
    """

if __name__ == '__main__':
    file_name = "Citywide_Payroll_Data__Fiscal_Year_.csv" # read the csv file to pyspark dataframe
    file_name_pick_k = "Citywide_Payroll_Data__Fiscal_Year_choose_k.csv"
    file_name_outliers = "Citywide_Payroll_Data__Fiscal_Year_outliers.csv"
    file_name_center_list = "Citywide_Payroll_Data__Fiscal_Year_center_list.csv"

    df = spark.read.format("csv").options(header="true", inferschema="true").load(file_name)
    df.createOrReplaceTempView("df")
    for colname in df.columns: df = df.withColumnRenamed(colname, "_".join(colname.split(" "))) # handle the column name with space
    # add index to the table
    df = df.select("*").withColumn("id", monotonically_increasing_id())
    # normalize data
    df = df_normalization(df, column_name_list, float_decimal=5)
    # get feature column
    scaled_column = [col for col in df_vec.columns if "scaled" in col]
    df = get_feature(df, scaled_column)
    df = df.select("id", "feature")
    
    column_name_list = [i[0] for i in df.dtypes if i[1]=="double"]
    # pick k for clustering
    df_pick_k = pick_k(df, column_name_list, sample_rate=0.0005, sample_size=5, ktop=10)
    # note: remove the original file before save these file
    df_pick_k.coalesce(1).write.format("csv").option("header", "True").mode("append").save(file_name_pick_k)
    # this can be used for further visualization for picking k
    """
    hfs -getmerge Citywide_Payroll_Data__Fiscal_Year_choose_k.csv Citywide_Payroll_Data__Fiscal_Year_choose_k.csv
    """

    # run k-means
    optimalK = 4
    new_df_kmeans_transformed, model = kmeans(df, optimalK)

    # compute euclidean distance
    outlier_pdf, center_pdf = euclidean_distance(new_df_kmeans_transformed, optimalK, model)
    # note: remove the original file before save these file
    outlier_pdf.coalesce(1).write.format("csv").option("header", "True").mode("append").save(file_name_outliers)
    center_pdf.coalesce(1).write.format("csv").option("header", "True").mode("append").save(file_name_center_list)
    """
    hfs -getmerge Citywide_Payroll_Data__Fiscal_Year_outliers.csv Citywide_Payroll_Data__Fiscal_Year_outliers.csv
    hfs -getmerge Citywide_Payroll_Data__Fiscal_Year_center_list.csv Citywide_Payroll_Data__Fiscal_Year_center_list.csv
    """