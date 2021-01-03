"""
This is the code to compute the misspelling using the word frequecy and levenshtein similarity
Author: Yu-Lin Shen
        note that several code is sourced from stackoverflow, github, pyspark repository, blog
"""
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import monotonically_increasing_id 

import pandas as pd
import numpy as np
from collections import Counter, defaultdict

def missepelling(df, colname, levenshtein_distance=2):
    """
    Input:
    df: pyspark dataframe
    colname: string
    levenshtein_distance: number, default is 2, the levenshtein distance for the similarity

    Output:
    df_distribution: pyspark dataframe, column name value distribution 
    df_correctness: pyspark dataframe, misspelling correctness 
    """
    # get the pyspark dataframe
    df.createOrReplaceTempView("df")
    df_distribution = spark.sql("""
        select Agency_Name as {0}, count(*) as count from df
        group by {0}
        """.format(colname))

    # compute the levenshtein similarity and 
    df1 = df_distribution.selectExpr("{} as search_name".format(colname))
    df2 = df_distribution.selectExpr("{} as map_name".format(colname))
    df_colname_join = df1.crossJoin(df2)
    df_colname_check = df_colname_join.withColumn("levenshtein", F.levenshtein(F.col("search_name"), F.col("map_name"))).filter("levenshtein <= {}".format(levenshtein_distance)).filter("levenshtein > 0")
    df_colname_search_map = df_colname_check.groupBy('search_name').agg(F.collect_list("map_name"))
    data_pair = df_colname_search_map.select(*df_colname_search_map.columns).rdd.map(lambda x: set([x["search_name"]]+x["collect_list(map_name)"]))
    
    # join the similar cluster, ((A, B), (B, C), (E, F), (F, G)) -> ((A, B, C), (E, F, G))
    data_pair_test = data_pair.collect()
    cluster = []
    for i in data_pair_test:
        flag = False
        for k in range(len(cluster)):
            if len(cluster[k].intersection(i))>0:
                cluster[k] = cluster[k].union(i)
                flag = True
                break
            else:
                pass
        if flag == False:
            cluster.append(i)
    cluster_idx = [[c, idx] for idx, clu in enumerate(cluster) for c in clu]
    # idx is use to recorde the cluster number, for example (A, B, C) is 1; (E, F, G) is 2
    pdf = pd.DataFrame(cluster_idx, columns = [colname, "idx"])
    
    # transfer back to pyspark dataframe to join the df_distribution
    df_correctness = spark.createDataFrame(pdf)
    df_correctness.createOrReplaceTempView("df_correctness")
    df_distribution.createOrReplaceTempView("df_distribution")
    df_correctness = spark.sql("""
        select df_correctness.{0} {0}, df_correctness.idx idx, df_distribution.count count 
        from df_correctness 
        join df_distribution on df_distribution.{0} = df_correctness.{0} order by {0}
        """.format(colname))
    # create the correctness table with misspelling
    df_correctness_test = df_correctness.collect()
    dfc = pd.DataFrame([[i[colname], i["idx"], i["count"]] for i in df_correctness_test], , columns = [colname, "idx", "count"])
    colname_dict = defaultdict(list)
    for i in np.array(dfc):
        colname_dict[i[1]].append([i[0], i[2]])
    for i in colname_dict:
        colname_dict[i] = np.array(colname_dict[i])
    mapping = []
    for idx in colname_dict:
        check = [set([t for t in j]) for j in colname_dict[idx][:, 0]]
        uni = set()
        for j in check:
            uni = uni.union(j)
        inter = set(uni)
        for j in check:
            inter = inter.intersection(j)
        differ = uni.difference(inter)
        res = "".join(list(differ))
        if res.lower().isalpha():
            correct = sorted(colname_dict[idx], key=lambda x: int(x[1]))[-1][0]
            for wd in colname_dict[idx]:    
                mapping.append([wd[0], correct, wd[1]])
        else:
            correct = colname_dict[idx][0][0]
            for ch in differ:
                correct = correct.replace(ch, "")
            for wd in colname_dict[idx]:    
                mapping.append([wd[0], correct, wd[1]])
    df_correctness = pd.DataFrame(mapping, columns=["original", "correctness", "count"])
    df_correctness = spark.createDataFrame(pdf)
    return df_distribution, df_correctness

if __name__ == '__main__':
    file_name = "Citywide_Payroll_Data__Fiscal_Year_.csv" # read the csv file to pyspark dataframe
    target_colname = "agency_name" # target column, misspelling
    df_distribution_filename = "Citywide_Payroll_Data__Fiscal_Year_agency_name_distribution.csv" # the output file name for df_distribution
    df_correctness_filename = "Citywide_Payroll_Data__Fiscal_Year_agency_name_misvalue.csv" # the output file name for df_correctness

    df = spark.read.format("csv").options(header="true", inferschema="true").load(file_name)
    df.createOrReplaceTempView("df")
    for colname in df.columns:
        df = df.withColumnRenamed(colname, "_".join(colname.split(" "))) # handle the column name with space
    # add index to the table
    df = df.select("*").withColumn("id", monotonically_increasing_id())

    # note: remove the original file before save these file
    df_distribution, df_correctness = missepelling(df, colname, levenshtein_distance=2)
    df_distribution.orderBy(colname).coalesce(1).write.format("csv").option("header", "True").mode("append").save(df_distribution_filename)
    df_correctness.coalesce(1).write.format("csv").option("header", "True").mode("append").save(df_correctness_filename)
    """
    hfs -getmerge agency_name_distribution_Citywide_Payroll_Data__Fiscal_Year_.csv agency_name_distribution_Citywide_Payroll_Data__Fiscal_Year_.csv
    hfs -getmerge Citywide_Payroll_Data__Fiscal_Year_agency_name_misvalue.csv Citywide_Payroll_Data__Fiscal_Year_agency_name_misvalue.csv
    """