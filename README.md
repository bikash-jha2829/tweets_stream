# tweets_stream

# Tech stack used 
1. kafka-confluent
2. Spark Streaming
3. Elastic stack (Elastic search + Kibana)

# Library used
1. Kafka-confluent
2. Tweepy 
3. pySpark


# basic Flow of our code 
  ## authenticate using tweepy and use kafka-confluent publish to load data to kafka 
    #### kafka_producer_tweets.py : fetch all tweets with language = en and hastags passed as list (refer config.yaml)
  
  
  ![image](https://user-images.githubusercontent.com/79247013/166160195-e783a8f8-097c-4c0b-a2cc-37c039c04e56.png)

  
     
  ## spark streaming to consume tweets from kafka_topic = 'tweets'
  
      kafka_consumer_tweets.py
      note : for geolocation we fetched 'value.geo', 'value.coordinates', 'value.place'
      clean the message/text 
      load it to delta lake ( s3 in this  case) - can be configured 
  ## Experimental directory (needs more improvement) :
  
      create elastic search index and load the json records using kafka handler.
        refer : kafka_handler.py
    
  
# How to Run :
 Install Docker-desktop or kind ( run the command present below)
 install kafka and zookeeper : docker-compose up -d  (go to container folder first)
 make sure you have spark installed and place the jars in /opt/spark/jars folder 
 Alternatively you can run spark via docker image on kubernetes
 
# How will you monitor the platform 
Please find attached files 
  1. using Elastic Stack (open source monitoring tool with kubernetes)
  2. If using AWS ( cloudwatch events --attached monitoring doc for the same with code.)
  3. Add a Dead Letter queue whenever a exception happen to put the records in Dead Letter Queue

# Data Quality : Attached documents for the data quality 
   in the code we are writing offset back to kafka after consuming (refer delivery callback)
   in future : we need to add data quality check before pushing to/from delta lake (sample test case written to ensure )
   
   ![image](https://user-images.githubusercontent.com/79247013/166160202-38edf184-79c8-43af-a666-ee1ba0e47d46.png)


# for large amount of data 
1. kafka is scabale solution itself (we need to increase the number of partions )
2. all solution is kept under kubernetes (Consider using EKS cluster with autoscaling on )
3. Spark ( refer spark-pi.py yaml file )
   
 ```
   i. Install sparkoperator microsrevice ( it will give all installation and enable spark features)
        helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
        helm install spark spark-operator/spark-operator --namespace spark-operator --create-namespace --set sparkJobNamespace=default
   ii.  kubectl create -f containers/spark-pi.yaml  (change mainApplicationFile for any python code )
   
   Airflow : please refer code for airflow dag in experimental
```


# testing : Please refer tests directory containing all test cases
 1. test cases are added , need more test cases (mostly pytest and integration testing)
 2. testing - data quality on kafka 

# in Production 
1. conf file will be replaced by configMap.yaml in kubernetes(change the hashtags on thr fly)
2. Need to create helm charts and Ci-CD pipeline 
3. May be will consider using airflow on k8s for batch streaming



# Improvements on the existing code :
- 1.Addition of Dead Letter Queue 
2. More test cases 
3. Duplicate records handling 
4. Designing of Delta lake (Robust)
  
