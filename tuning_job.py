# python script for running Sagemaker tuning job

import boto3
import sagemaker.amazon.common as smac
from sagemaker import get_execution_role
#import re
import sagemaker
import json
from time import gmtime, strftime, sleep                 
import os 

region = boto3.Session(region_name='us-west-2', profile_name='mateosanchez') 
smclient = boto3.Session(region_name='us-west-2', profile_name='mateosanchez').client('sagemaker')

sess = sagemaker.Session(boto_session=boto3.Session(region_name='us-west-2', profile_name='mateosanchez'))
role ='arn:aws:iam::353643785282:role/service-role/AmazonSageMaker-ExecutionRole-20210115T123894'

tuning_job_name = 'dencrime-jan16-dec19-v2-' + strftime("%H-%m", gmtime())

print("Tuning Job Name:")
print(tuning_job_name)

#Specify tuning hyperparameters
tuning_job_config = {
    "ParameterRanges": {
      "CategoricalParameterRanges": [],
      "ContinuousParameterRanges": [          
        {
          "MinValue": "0.0001",
          "MaxValue": "0.001",
          "Name": "learning_rate"  
          },
      ],
      "IntegerParameterRanges": [
        {
          "MaxValue": "300",
          "MinValue": "200",
          "Name": "mini_batch_size"
        },
        {
          "MaxValue": "100",
          "MinValue": "70",
          "Name": "num_cells"
        },
        {
          "MaxValue": "110",
          "MinValue": "70",
          "Name": "epochs"
        }
      ]
    },
    "ResourceLimits": {
      "MaxNumberOfTrainingJobs": 8,
      "MaxParallelTrainingJobs": 2
    },
    "Strategy": "Bayesian",
    "HyperParameterTuningJobObjective": {
      "MetricName": "test:RMSE",
      "Type": "Minimize"
    }
  }

containers  = {
    'us-east-1': '522234722520.dkr.ecr.us-east-1.amazonaws.com/forecasting-deepar:latest',
    'us-east-2': '566113047672.dkr.ecr.us-east-2.amazonaws.com/forecasting-deepar:latest',
    'us-west-2': '156387875391.dkr.ecr.us-west-2.amazonaws.com/forecasting-deepar:latest',
    'ap-northeast-1': '633353088612.dkr.ecr.ap-northeast-1.amazonaws.com/forecasting-deepar:latest'
}

training_image = containers['us-west-2']


s3_training_file=r's3://{0}/{1}'.format('den-crime','training_data/train/')
s3_validation_file=r's3://{0}/{1}'.format('den-crime','training_data/test/')
s3_model_output_location = r's3://{0}/training_output/'.format('den-crime')

prediction_length = 52
context_length = 155 # 209 weeks in training set, means max(context) = 209-52 = 157

training_job_definition = {
    "AlgorithmSpecification": {
      "TrainingImage": training_image,
      "TrainingInputMode": "File"
    },
    "InputDataConfig": [
      {
        "ChannelName": "train",
        "CompressionType": "None",
        "ContentType": "json",
        "DataSource": {
          "S3DataSource": {
            "S3DataDistributionType": "ShardedByS3Key",
            "S3DataType": "S3Prefix",
            "S3Uri": s3_training_file
          }
        }
      },
      {
        "ChannelName": "test",
        "CompressionType": "None",
        "ContentType": "json",
        "DataSource": {
          "S3DataSource": {
            "S3DataDistributionType": "ShardedByS3Key",
            "S3DataType": "S3Prefix",
            "S3Uri": s3_validation_file
          }
        }
      }
    ],
    "OutputDataConfig": {
      "S3OutputPath": s3_model_output_location
    },
    "ResourceConfig": {
      "InstanceCount": 1,
      "InstanceType":  "ml.m4.xlarge",
      "VolumeSizeInGB": 32
    },
    "RoleArn": role,
    "StaticHyperParameters": {
    "time_freq": 'W',
    "context_length": str(context_length),
    "prediction_length": str(prediction_length),
    #"num_cells": "40",
    "num_layers": "1", 
    "likelihood": "negative-binomial",
    #"epochs": "75", 
    #"mini_batch_size": "32",
    #"learning_rate": "0.001",
    "dropout_rate": "0.15", 
    "early_stopping_patience": "20",
    #"num_dynamic_feat": ""
    },
    "StoppingCondition": {
      # Max 30m per training job
      "MaxRuntimeInSeconds": 1800
    }
}


#Launch tuning job
smclient.create_hyper_parameter_tuning_job(HyperParameterTuningJobName = tuning_job_name,
                                           HyperParameterTuningJobConfig = tuning_job_config,
                                            TrainingJobDefinition = training_job_definition)

print("Don't be lazy - Go to the Sagemaker console to monitor the tuning job!")