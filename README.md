
# Yelp Document Streaming: A Data Pipeline Project

![yelp](/images/Yelp.jpg)

# Introduction & Goals

>>This project will show how to stream, process, and visualize JSON documents from the Yelp dataset using various tools 
and technologies. We can see how the Yelp data can be integrated and enriched by merging the data coming from different 
files into a single database. This way, we can avoid the complexity and inefficiency of doing a JSON merge on voluminous
files, which can be very difficult and time-consuming

# Contents

- [The Data Set](#the-data-set)
- [Used Tools](#used-tools)
  - [Connect](#connect)
  - [Buffer](#buffer)
  - [Processing](#processing)
  - [Storage](#storage)
  - [Connect to Storage](#connect-to-storage)
  - [Visualization](#visualization)
- [Setup](#setup)
- [Pipelines](#pipelines)
  - [Stream Processing](#stream-processing)
    - [Storing Data Stream](#storing-data-stream)
    - [Processing Data Stream](#processing-data-stream)
  - [Batch Processing](#batch-processing)
  - [Visualizations](#visualizations)
- [Demo](#demo)
- [Conclusion](#conclusion)
- [Follow Me On](#follow-me-on)
- [Appendix](#appendix)


# The Data Set
- The dataset is a subset of Yelp’s businesses, reviews, user, checkin and tip data that can be used for various 
research projects
- Data Source : https://www.yelp.com/dataset/download 
- Documentation of Yelp dataset : https://www.yelp.com/dataset/documentation/main

This documentation explain the structure of business.json, review.json, user.json, checkin.json and tip.json

Here the content of a **business** document
![business](/images/business.png)
The content of a **review** document
![review](/images/review.png)
The content of a **user** document
![user](/images/user.png)
The content of a **checkin** document
![checkin](/images/checkin.png)
The content of a **tip** document
![tip](/images/tip.png)

# Used Tools
## Connect
- FastAPI to validate the schema of JSON documents and send them to a Kafka producer
## Buffer
- Kafka producer to distributes the JSON documents to PySpark
## Processing
- PySpark to processes and stores the JSON documents in MongoDB database
## Storage
- MongoDB database that persists the JSON documents based on the keys (user_id or business_id)
## Connect to Storage
- FastAPI to retrieves data from MongoDB and sends it to a Streamlit application
## Visualization
- Streamlit application that visualizes the Yelp data

# Setup
Since we have all services dockerized, we need to follow these steps to properly configure all 
the images and then deploy the containers:
- Clone the git repository, create a 'dataset' directory and put all the Yelp datasets in there,
that way you don't need to change anything in the code
- As we have a GUI and two APIs, all built from python code, we need to build their docker images
first. To do this :
  1.  In the terminal, go to `API-Kafka-Ingest` directory  and type: `docker build -t api-kafka-ingest .`
  2.  In the terminal, go to `API-Streamlit-output` directory  and type: `docker build -t api-streamlit-output .`
  3.  In the terminal, go to `Streamlit` directory  and type: `docker build -t streamlit .`
- We are now ready to build containers from our docker-compose file, to do this go to the main project directory
and type: `docker-compose up -d`
- Finally, create the Kafka topics **Yelp-topic** and **spark-output** with **3 partitions** and a **replication 
factor of 1** and the **Kafka broker** runs on **localhost:9092**. To do this : 
  1.  In the terminal, type `docker ps` to see running container
  2.  Copy the name of the kafka container, in my case it was `yelp-kafka-1`
  3.  Type `docker exec -it yelp-kafka-1 bash`
  4.  Type `cd opt/bitnami/kafka/bin/`
  5.  We can create now Topics, to do that, type : 
```shell
./kafka-topics.sh --create --topic Yelp-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```
```shell
./kafka-topics.sh --create --topic spark-output --bootstrap-server localhost:9092
```

# Pipelines
- Explain the pipelines for processing that you are building
- Go through your development and add your source code

## Stream Processing
### Storing Data Stream
### Processing Data Stream
## Batch Processing
## Visualizations

# Demo
- You could add a demo video here
- Or link to your presentation video of the project

# Conclusion
Write a comprehensive conclusion.
- How did this project turn out
- What major things have you learned
- What were the biggest challenges

# Follow Me On
Add the link to your LinkedIn Profile

# Appendix

[Markdown Cheat Sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
