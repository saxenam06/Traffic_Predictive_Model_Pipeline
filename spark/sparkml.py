import os
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
import csv
import pyspark.sql.functions as F
from pyspark.sql import functions as sf
from collections import OrderedDict
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LinearRegressionModel
import numpy as np
from functools import reduce
import json

def get_mapping(rdd, idx):
    
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()


def extract_features(record, hour, category_len, mappings):
    cat_vec = np.zeros(category_len)
    i = 0
    step = 0
    for field in record[1:11]:
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)
    num_vec1 = np.array([np.log(float(field.strip("\"")) + 0.01) for field in record[13 : hour + 13]])  # log transformed
    num_vec2 = np.array([np.log(float(field.strip("\"")) + 0.01) for field in record[hour + 14 : 37]])  # skip current time
    return np.concatenate((cat_vec, num_vec1, num_vec2))


def extract_label(record, idx):
    return np.log(float(record[idx].strip("\"")) + 0.01)  # log transformation


def main():
    # Initializing Spark session
    spark = SparkSession\
            .builder\
            .appName("TRAFFIC")\
            .config("spark.executor.cores", "6")\
            .config("spark.executor.memory", "6g")\
            .getOrCreate()
    
    sc = spark.sparkContext


    # Path to traffic data
    text_file_path = "./streaming-pipelines/data"
    
    # read and filter out unwanted rows 
    raw_data = sc.textFile(text_file_path)
    header = raw_data.first()
    filt_records = raw_data.filter(lambda line: line != header).map(lambda x: x.split(","))
    records = filt_records.filter(lambda fields: all(float(field.strip("\"")) >= 0 for field in fields[13:37]))
    records.cache()

    # For records visualization 
    #records_df = records.toDF()
    #records_df.show(5)
   
    # Find distinct categories in each feature vector
    mappings = [get_mapping(records, i) for i in range(1,11)]
    category_len = reduce(lambda x, y: x + y, map(len, mappings))
  
   # For mapping visualization 
   # for idx, mapping in enumerate(mappings, start = 1):
   #     print(f"Mapping for field {idx}:")
   #     for key, value in mapping.items():
   #         print(f"{key}: {value}")
    print("Total number of categories", category_len)

    # Write mappings to text file for later use while inferencing
    with open("./streaming-pipelines/ML_model/mappings.txt", "w") as fp:
        json.dump(mappings, fp)

    # Another way of writing
#        for idx, mapping in enumerate(mappings, start=1):
#            json.dump(mapping, fp)
#            f.write(f"Mapping for field {idx}:\n")
#            for key, value in mapping.items():
#                f.write(f"{key}: {value}\n")

    # Training using Linear regression and save model weights
    for hour in range(0, 24):
         data_log = records.map(lambda r: LabeledPoint(extract_label(r, hour + 13), extract_features(r, hour, category_len, mappings)))   #log transformed data
         linear_model_log = LinearRegressionWithSGD.train(data_log, iterations=100, step=0.01, intercept=True)
         linear_model_log.save(sc, "./streaming-pipelines/ML_model/linear_model_log_"+str(hour))

    sc.stop()


if __name__ == '__main__':
    main()
