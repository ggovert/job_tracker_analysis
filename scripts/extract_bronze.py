import requests
from datetime import datetime
from bs4 import BeautifulSoup
import html
import json
import boto3
import os
from botocore.client import Config
from airflow.models import Variable
import time


# Config
RUSTFS_ENDPOINT = "http://rustfs:9000" 
ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin")
SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin123")
BUCKET_NAME = "bronze"

def upload_to_rustfs(data, folder_name= "hackernews"):
    '''
    Uploads list of JSON to RistFS as a JSON object
    '''
    s3 = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )


    # Safety check: create bucket if it does not exist 
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except s3.exceptions.BucketNotFound:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} created")

    # Generate key for the file
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_key = f"{folder_name}/jobs_{timestamp}.json"


    # Upload 
    try:
        # Convert list to JSON string and upload
        s3.put_object(
            Bucket = BUCKET_NAME,
            Key = file_key,
            Body = json.dumps(data, indent=4).encode("utf-8"),
            ContentType = "application/json"
        )
        print(f"Successfully uploaded to rustfs://{BUCKET_NAME}/{file_key}")
    except Exception as e:
        print(f"Failed to upload to rustfs: {str(e)}")



def get_hackernews_jobs():
    '''
    fetch the latest threadid of ASK HN: Who is hiring? from HackerNews API
    '''
    url = "https://hn.algolia.com/api/v1/search_by_date"
    params = {"tags": "story,author_whoishiring"}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()        
        jobs = []
        if 'hits' in data:
            for item in data['hits']:
                if item['title'].startswith("Ask HN: Who is hiring?"):
                    jobs.append({
                        'source': 'hackernews',
                        'title': item.get('title', 'No Title'),
                        'date': item.get('created_at'),
                        'url': f"https://news.ycombinator.com/item?id={item.get('objectID')}",
                        'objectID': item.get('objectID')
                    })
        return jobs[0] if jobs else None
    except Exception as e:
        print(f"Error fetching HN jobs: {e}")
        return None

def clean_html_text(text):
    """
    Clean HTML text by removing HTML tags and decoding HTML entities.
    But still left the /n for filtering the text
    """
    if not text: return ""
    text = html.unescape(text)
    soup = BeautifulSoup(text, 'html.parser')
    cleaned = soup.get_text(separator='\n')
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    return '\n'.join(lines)

def get_thread_comments(thread_id, last_checkpoint):
    """
    Fetch comments from a specific / latest thread about hiring on HackerNews
    Fetch the comments incrementally with Search API filter date
    """

    url = "https://hn.algolia.com/api/v1/search_by_date"
    all_comments = []
    page = 0

    while True:
        params = {
            "tags": f"comment,story_{thread_id}",
            "numericFilters": f"created_at_i>{last_checkpoint}",
            "hitsPerPage": 1000,
            "page": page
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            hits = data.get('hits', [])
            if not hits:
                break

            for hit in hits:
                # get the parent id of the comment so we didnt get the sub-comments
                parent_id = hit.get('parent_id')

                if str(parent_id) != str(thread_id):
                    continue
                
                raw_text = hit.get('text') or hit.get('comment_text')
                if raw_text:
                    cleaned_text = clean_html_text(raw_text)
                    if len(cleaned_text.split()) > 50:
                        all_comments.append(
                            {
                            'id': hit.get('objectID'),
                            'author': hit.get('author'),
                            'text': cleaned_text,
                            'created_at': datetime.fromtimestamp(hit.get('created_at_i')).isoformat()
                                            if hit.get('created_at_i') else None
                            })
            
            # check if there is more pages
            if page >= data.get('nbPages', 0) -1:
                break

            page += 1
            
        except Exception as e:
            print(f"Error fetching comments: {e}")
            break

    return all_comments




    # url = f"https://hn.algolia.com/api/v1/items/{thread_id}"
    # params = {
    #     "tags" : f"comment, story_{thread_id}",
    #     "numericFilters": f"created_at_i>{last_checkpoint}",
    #     "hitsPerPage": 1000
    # }
    # try:
    #     response = requests.get(url, timeout=10)
    #     data = response.json()
    #     comments = []
    #     if 'children' in data:
    #         for comment in data['children']:
    #             if comment.get('text'):
    #                 cleaned_text = clean_html_text(comment.get('text'))
    #                 word_count = len(cleaned_text.split())
    #                 if word_count > 30:
    #                     comments.append({
    #                         'id': comment.get('id'),
    #                         'author': comment.get('author'),
    #                         'text': cleaned_text,
    #                         'created_at': datetime.fromtimestamp(comment.get('created_at_i')).isoformat() if comment.get('created_at_i') else None
    #                     })
    #     return comments
    # except Exception as e:
    #     print(f"Error fetching comments: {e}")
    #     return []

def run_extraction(date = None, **kwargs):
    print(f"Running extraction for date: {date}")
    job_thread = get_hackernews_jobs()
    if not job_thread:
        print("No job thread found today")
        return 
    
    thread_id = job_thread['objectID']

    # Get the last successful checkpoint from Airflow
    checkpoint_key = f"hn_checkpoint_{thread_id}"

    # 1. Get the ISO String from Airflow (Default to start of month if missing)
    default_iso = datetime.now().replace(day=1, hour=0, minute=0, second=0).isoformat()
    last_checkpoint_iso = Variable.get(checkpoint_key, default_var=default_iso)

    # 2. CONVERT ISO STRING -> UNIX TIMESTAMP (for the API)
    # This turns "2026-01-14T..." into 1736812800
    try:
        last_checkpoint_ts = int(datetime.fromisoformat(last_checkpoint_iso).timestamp())
    except (ValueError, TypeError):
        # If the string is broken, go back to the start of the month
        print("Invalid checkpoint string. Resetting to start of month.")
        last_checkpoint_ts = int(datetime.now().replace(day=1, hour=0, minute=0, second=0).timestamp())
    
    print(f"Checking thread {thread_id} since {last_checkpoint_iso}")

    # 3. GET COMMENTS FROM THE API
    comments = get_thread_comments(thread_id, last_checkpoint_ts)

    if comments:
        upload_to_rustfs(comments)
        
        # 1. FIND THE MAX TIMESTAMP USING THE INTEGER FIELD
        # Note: We use 'created_at_i' which we saved in get_thread_comments
        new_checkpoint_int = max(c['created_at'] for c in comments)
        
        # 2. SAVE IT AS A RAW NUMBER
        Variable.set(checkpoint_key, new_checkpoint_int)
        
        print(f"Successfully landed {len(comments)} comments. New checkpoint: {new_checkpoint_int}")
        if len(comments) > 0:
            return True
        else:
            return False
    else:
        print("No new comments found")
