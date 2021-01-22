# spark script for running batch transform and post-processing

import findspark
findspark.init('/Users/matt.wheeler/spark')

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
spark = SparkSession.builder.appName('ops').getOrCreate()

sc=spark.sparkContext

from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from dateutil.parser import parse

import boto3
import sagemaker.amazon.common as smac
from sagemaker import get_execution_role
import re
import sagemaker
import numpy as np
import json
from time import gmtime, strftime

bucket = 'den-crime'

region = boto3.Session(region_name='us-west-2', profile_name='mateosanchez') 
smclient = boto3.Session(region_name='us-west-2', profile_name='mateosanchez').client('sagemaker')

sess = sagemaker.Session(boto_session=boto3.Session(region_name='us-west-2', profile_name='mateosanchez'))
role ='arn:aws:iam::353643785282:role/service-role/AmazonSageMaker-ExecutionRole-20210115T123894'

job_name = 'dencrime-jan16-dec19-v3-21-01-004-0cbcbb23'

model_name=job_name + '-model'
print(model_name)

model_data = r's3://{0}/{1}/{2}'.format('den-crime/training_output', job_name, 'output/model.tar.gz')

containers  = {
    'us-east-1': '522234722520.dkr.ecr.us-east-1.amazonaws.com/forecasting-deepar:latest',
    'us-east-2': '566113047672.dkr.ecr.us-east-2.amazonaws.com/forecasting-deepar:latest',
    'us-west-2': '156387875391.dkr.ecr.us-west-2.amazonaws.com/forecasting-deepar:latest',
    'ap-northeast-1': '633353088612.dkr.ecr.ap-northeast-1.amazonaws.com/forecasting-deepar:latest'
}

container = containers['us-west-2']

primary_container = {
    'Image': container,
    'ModelDataUrl': model_data
}

create_model_response = smclient.create_model(
    ModelName = model_name,
    ExecutionRoleArn = role,
    PrimaryContainer = primary_container)

print(create_model_response['ModelArn'])

transformer = sagemaker.transformer.Transformer(
    model_name=model_name,
    instance_count=1,
    instance_type='ml.m4.10xlarge',
    strategy='SingleRecord',
    env= {
      "DEEPAR_FORWARDED_FIELDS" : '["OFFENSE_CATEGORY_ID"]'
    },
    assemble_with='Line',
    output_path="s3://{0}/batch_transform/predictions/raw/{1}".format(bucket, model_name),
    accept='application/jsonlines',
    max_payload=1,
    base_transform_job_name=job_name,
    sagemaker_session=sess,
)

input_key = 'training_data/test/'
input_location = 's3://{}/{}'.format(bucket, input_key)

transformer.transform(
    data=input_location,
    content_type='application/jsonlines',
    split_type='Line',
)

transformer.wait()


model_name = job_name + '-model'

#TODO: read from S3
#prediction_df=spark.read.json("s3a://den-crime/batch_transform/predictions/raw/"+model_name+"/*json.out")
prediction_df=spark.read.json("batch_transform/*json.out")

prediction_df.count()


#build time series date array
start_dt = parse('2020 01 01 23:59:59')
origin= parse('1970 01 01 00:00:00')

#Formula to calculate end_dt should be start_Dt + prediction length
end_dt=parse('2020 12 31 23:59:59')

# Set weekly cadence for datetime reference
step = 60 * 60 * 24 * 7
 
t1=(start_dt-origin).total_seconds()
t2=(end_dt-origin).total_seconds()

reference = spark.range(
    (t1 / step) * step, ((t2 / step) + 1) * step, step
).select(col("id").cast("timestamp").alias("clndr_week"))
reference=reference.withColumn('clndr_week',reference.clndr_week.cast('string'))
reference=reference.orderBy('clndr_week').agg(f.collect_list(reference.clndr_week)).collect()
reference

date_array = np.array(reference)
date_array_sort = np.sort(date_array[0][0])

# Create week_of column in prediction results dataframe
prediction_df=prediction_df.withColumn('week_of', f.array([f.lit(x) for x in date_array_sort]))


df_q50=(prediction_df
 .withColumn('Q5', prediction_df.quantiles['0.5'])
 .drop('quantiles')
)

df_q50.show(5)


combine = f.udf(lambda x, y: list(zip(x, y)),
              ArrayType(StructType([StructField("week_of", StringType()),
                                    StructField("Q5", DoubleType())])))

df_pred_combined = df_q50.withColumn("new", combine("week_of", "Q5"))\
       .withColumn("new", f.explode("new"))\
       .select("OFFENSE_CATEGORY_ID", f.col("new.week_of").alias("week_of"), f.col("new.Q5").alias("Q5"))

df_pred_combined = df_pred_combined.withColumn("week_of", date_format(col('week_of'),"yyyy-MM-dd").cast("date"))

# hack together a year_week field
df_pred_combined = df_pred_combined.withColumn("week", weekofyear(col("week_of")))
df_pred_combined = df_pred_combined.withColumn("week_pad", f.lpad(col("week"), 2, '0'))
df_pred_combined = df_pred_combined.withColumn("year", year(col("week_of")))
df_pred_combined = df_pred_combined.withColumn("year_week", concat(col("year"), lit("-"), col("week_pad")))

df_pred_combined.show(10)

# Read in data with actual target to compare to predictions
test_df = spark.read.csv("crime_grouped.csv", header=True)

# Join predictions to actual target
cond = [test_df.year_week == df_pred_combined.year_week, test_df.OFFENSE_CATEGORY_ID == df_pred_combined.OFFENSE_CATEGORY_ID]

df1 = test_df.alias('df1')
df2 = df_pred_combined.alias('df2')

results = df1.join(df2, cond, 'left').select('df1.*', df2.Q5.alias("predicted"), df2.week_of)

results.where(col('predicted').isNotNull()).show(10)

results.coalesce(1).write.mode('overwrite').csv("prediction_results/", header=True)