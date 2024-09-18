# 📊 Updating tables daily

This Google Cloud Function retrieves the daily top videos from YouTube, processes the data, and stores it in BigQuery. The function is scheduled to run daily via Google Cloud Scheduler.

### Overview
The `updating_tables_daily` function performs the following tasks:

* Queries the YouTube API for the top daily videos.
* Retrieves additional channel information.
* Stores the video and channel data in BigQuery.
* Ensures BigQuery tables exist or creates them if they do not.
* 
### Prerequisites
* **Google Cloud Project**: You need a Google Cloud Project with billing enabled.
* **BigQuery Dataset**: Your project must have the necessary BigQuery datasets and tables (yt_channel_info and yt_daily_top_videos).
* **YouTube API Access**: Your project must have access to the YouTube Data API v3.

### Setup
#### 1. Configure Environment Variables

Set the necessary environment variables for your function. You can set these variables directly in your Google Cloud Function configuration or locally using a .env file.

```env
# Google Cloud Project and BigQuery details
PROJECT_ID=your_google_cloud_project_id
DATASET_NAME=your_bigquery_dataset_name
TABLE_CHANNEL_INFO=your_channel_info_table_name
TABLE_DAILY_TOP_VIDEOS=your_daily_top_videos_table_name
```

#### 2. Deploy the Google Cloud Function

Deploy the function to Google Cloud:

```bash
gcloud functions deploy youtube_data_pipeline \
  --runtime python310 \
  --trigger-http \
  --region=us-central1 \
  --allow-unauthenticated
 ```
#### 3. Grant Invoke Permission

Allow the Cloud Scheduler to invoke your Cloud Function:

```bash
gcloud functions add-iam-policy-binding youtube_data_pipeline \
  --region="us-central1" \
  --member="SERVICE_ACCOUNT_NAME" \
  --role="roles/cloudfunctions.invoker"
 ```

#### 4. Grant Cloud Scheduler Permission

Ensure Cloud Scheduler has the necessary permissions:

```bash
gcloud functions add-iam-policy-binding youtube_data_pipeline \
  --member="serviceAccount:YOUR_PROJECT_ID@appspot.gserviceaccount.com" \
  --role="roles/cloudscheduler.admin" \
  --region=us-central1
```
Replace `YOUR_PROJECT_ID` with your Google Cloud project ID.

#### 5. Schedule the Function with Cloud Scheduler

Create a Cloud Scheduler job to invoke your Cloud Function daily:

```bash
gcloud scheduler jobs create http youtube-daily-top \
  --schedule="YOUR_CRON_EXPRESSION" \
  --uri="https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/youtube_data_pipeline" \
  --http-method=POST \
  --time-zone="YOUR_TIME_ZONE"
```
Replace `YOUR_PROJECT_ID` and `YOUR_TIME_ZONE` with your specific details.