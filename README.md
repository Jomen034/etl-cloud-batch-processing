# etl-cloud-batch-processing
# ETL Cloud and Batch Processing Case

## Overview
Tech companies used to require having their own on-premise servers to operate ETLs on their data. Of course, numerous companies would have to face issues on scalability, data loss, hardware failure, etc. With the appearance of cloud services offered by major tech companies, this was changed, as they provided shared-computing resources on the cloud which is able to solve most issues found on on-premise servers.
This repo is to set up a Google Cloud Composer environment and solve several batch-processing cases by creating DAGs to run ETL jobs in the cloud. The data processing consists of ETLs with data going in and from GCS and BigQuery

## ETL Cloud
Cloud ETL entails extracting data from diverse source systems, transforming it to a common format, and loading the consolidated data into the data warehouse platform to best serve the needs of enterprise business intelligence, reporting and analytics.

## Batch Processing
Batch processing is a method of running high-volume, repetitive data jobs. The batch method allows users to process data when computing resources are available, and with little or no user interaction.

# Installation

## GCP Setup
If you don't have GCP acoount yet, create one with the free trial from [Google Cloud Platform](https://cloud.google.com/composer). After you have acces to your GCP, let's do some following setup:
1. **Service Account**
    * go to `IAM & Admin`
    * choose `Service Accounts`, and create your service account.

2. **Environment Set Up**
    * go to `Composer`
    * enable the `Cloud Composer API`
    * create your environment. I used this set up for this task

```
Location : us-central1
Zone : us-central1-f
Node count : 3
Disk size (GB) : 20
Machine type : n1-standard-1
Cloud SQL machine type : db-n1-standard-2 (2 vCPU, 7.5 GB memory)
Web server machine type : composer-n1-webserver-2 (2 vCPU, 1.6 GB memory)
```

3. **Create Bucket**
    * go to `Cloud Storage`
    * create your bucket

4. **Variable Set Up**
    * go to `composer`
    * click your env
    * go to `Environment Variables` and set up your variables

![image](https://user-images.githubusercontent.com/71366136/115995646-4904d480-a606-11eb-8104-1ca75ff11e42.png)


## Google Composer Airflow
Open the `Airflow Web UI` by clicking `Airflow` section on your list of environments

![image](https://user-images.githubusercontent.com/71366136/115995577-fcb99480-a605-11eb-870a-056170ea8636.png)

**Define Airflow Variables**
![image](https://user-images.githubusercontent.com/71366136/115995708-836e7180-a606-11eb-82d5-1f509a8e5bc1.png)

**Define Airflow COnnections**

![image](https://user-images.githubusercontent.com/71366136/115995730-9d0fb900-a606-11eb-889b-0ce1efb1754f.png)

## Writing DAGs
When creating your DAGs, make sure you do this 5 actions:
1. Import module
2. Define the `default_args`
3. Instantiate DAG
4. Define your task
5. Define the dependencies

## ETL Cloud & Batch Processing Case
**Integrate daily search history**

*Case : A website has been keeping track of their user’s search history. They process their data day-by-day and upload it into a GCS bucket.*

1. Run Dataflow job on CSV into a Bigquery table. Make the BQ schema the same as the csv.
2. Create another Bigquery table that reports the most searched keyword for each day. Schema is up to you as long as it solves the task.
3. The DAG is scheduled to run day-by-day for each csv.

Load the `daily_search_history.py` to the `dags` folder on the bucket object. After loading the file, the Airflow Web UI will be update

![image](https://user-images.githubusercontent.com/71366136/115996439-7a32d400-a609-11eb-9295-7f7131f779a5.png)

## Result
If all is clear, the Airflow Web UI will show following information

![image](https://user-images.githubusercontent.com/71366136/115995976-b06f5400-a607-11eb-8961-e8e0326642d6.png)

**All the scheduler task are done**

In the Google Cloud Big Query, the result is

![image](https://user-images.githubusercontent.com/71366136/115997040-d0a11200-a60b-11eb-9d33-4daf3c2f6998.png)

# Conclusion
* ETL in traditional way was made use of physical warehouses to store the integrated data from various sources. 
* **With Cloud ETL, both, the sources from where companies bring in the data and the destination data warehouses are purely online.** 
* There is no physical data warehouse or any other hardware that a business needs to maintain. 
* **Cloud ETL** manages these dataflows with the help of robust Cloud ETL Tools that **allows users to create and monitor automated ETL data pipelines, also allowing users to use diverse data for analysis all through a single user interface.** 
