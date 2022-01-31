# Sentiment-Prediction-and-Analysis-of-Biden-Administration

This project's goal is to collect tweets data from Twitter API and then analyze sentiments. Various steps are involved, which are highlighted below.

First, the data was collected from Twitter API using an EC2 and kinesis firehose instance. <br>
Then sentiments were generated based on the collected tweet data using PySpark. <br>
Next, the data was further cleaned using SQL Athena. <br>
Finally, a dashboard was developed to support the sentiments analyses.
