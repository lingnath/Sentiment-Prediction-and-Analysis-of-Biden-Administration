# Sentiment-Prediction-and-Analysis-of-Biden-Administration

This project's goal is to collect tweets data from Twitter API and then analyze sentiments. 
In a high level overview, the steps are highlighted below.

1. Data was collected from Twitter API using an EC2 and kinesis firehose instance. <br>
2. Sentiments were generated based on the collected tweet data using PySpark. <br>
3. The data was further cleaned using SQL Athena. <br>
4. Dashboard was developed to support the sentiments analyses, which could be found in the screenshot within one of the folders.

Each folder represents each major step in the project pipeline.
