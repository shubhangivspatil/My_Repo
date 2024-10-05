import streamlit as st
import pandas as pd
from pymongo import MongoClient
import psycopg2
from googleapiclient.discovery import build
from datetime import datetime
from sqlalchemy import create_engine

# MongoDB connection setup
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["youtube_data"]
collection = mongo_db["youtube_collection"]

# YouTube API key
api_key = "AIzaSyAl7an1MCUecVDN4TDSkb9Kme_eJoW3iS0"
youtube = build("youtube", "v3", developerKey=api_key)

# PostgreSQL connection
def get_postgres_conn():
    return psycopg2.connect(host="localhost", user="postgres", password="admin", database="Youtube_1", port="5432")

# Handle API exceptions
def handle_api_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"An API exception occurred: {e}")
    return wrapper

@handle_api_exceptions
def fetch_and_save_videos_from_channel_upload(playlist_id):
    next_page_token = None
    video_ids = []

    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return video_ids

# Fetch channel details
def get_channel_stst(channel_id):
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    response = request.execute()
    data = {
        'channel_id': response['items'][0]['id'],
        'channel_name': response['items'][0]['snippet']['title'],
        'channel_description': response['items'][0]['snippet']['description'],
        'subscribers_count': int(response['items'][0]['statistics']['subscriberCount']),
        'channel_views': int(response['items'][0]['statistics']['viewCount']),
        'video_count': int(response['items'][0]['statistics']['videoCount']),
        'playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'published_at': response['items'][0]['snippet']['publishedAt'].replace('Z', '')
    }
    return data

@handle_api_exceptions
def fetch_and_save_video_data(video_ids):
    video_datas = []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id)
        response = request.execute()
        video_stats = response['items'][0]['statistics']
        video_snippet = response['items'][0]['snippet']

        video_data = {
            "video_id": video_id,
            "video_name": video_snippet['title'],
            "video_description": video_snippet['description'],
            "tags": ",".join(video_snippet.get('tags', [])),
            "published_at": datetime.strptime(video_snippet['publishedAt'], "%Y-%m-%dT%H:%M:%SZ"),
            "view_count": int(video_stats.get('viewCount', 0)),
            "like_count": int(video_stats.get('likeCount', 0)),
            "dislike_count": int(video_stats.get('dislikeCount', 0)),
            "favorite_count": int(video_stats.get('favoriteCount', 0)),
            "comment_count": int(video_stats.get('commentCount', 0)),
            "duration": response['items'][0]['contentDetails']['duration'],
            "thumbnail": video_snippet['thumbnails']['medium']['url'],
            "caption_status": response['items'][0]['contentDetails']['caption'],
            "channel_id": response['items'][0]['snippet']['channelId']
        }

        video_datas.append(video_data)

    return video_datas

def comment_details(video_ids):
    comments = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(part='snippet,replies', videoId=video_id, maxResults=100)
            response = request.execute()

            for item in response['items']:
                comments.append({
                    'comment_id': item['snippet']['topLevelComment']['id'],
                    'video_id': video_id,
                    'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName '],
                    'comment_text': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'comment_published_at': item['snippet']['topLevelComment']['snippet']['publishedAt'].replace('Z', ''),
                    'comment_likes': int(item['snippet']['topLevelComment']['snippet']['likeCount']),
                })

        except Exception as e:
            if 'commentsDisabled' in str(e):
                print(f"Comments are disabled for video ID: {video_id}")
            else:
                print(f"An error occurred while fetching comments for video ID {video_id}: {e}")

    return comments

# Main data extraction function
def main(channel_id):
    channel_data = get_channel_stst(channel_id)
    video_ids = fetch_and_save_videos_from_channel_upload(channel_data["playlist_id"])
    video_data = fetch_and_save_video_data(video_ids)
    comments_data = comment_details(video_ids)

    return {"channel_data": channel_data, "video_data": video_data, "comments_data": comments_data}

# MongoDB insert
def insert_mongodb(channel_id):
    data = main(channel_id)
    collection.insert_one(data)
    print("Data saved to MongoDB successfully!")

    return data

# Function to check if table exists
def check_table_exists(cursor, table_name):
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='{table_name}')")
    return cursor.fetchone()[0]

# Create PostgreSQL tables
def create_tables(cursor):
    create_channels_query = '''
        CREATE TABLE IF NOT EXISTS channels (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(80) PRIMARY KEY,
            Subscribers_count BIGINT,
            Views BIGINT,
            Total_Videos INT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(80)
        )
    '''
    create_videos_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(30) PRIMARY KEY,
            Title VARCHAR(150),
            Tags TEXT,
            Thumbnail VARCHAR(200),
            Description TEXT,
            Published_Date TIMESTAMP,
            Duration INTERVAL,
            Views BIGINT,
            Likes BIGINT,
            Comments INT,
            Favorite_Count INT,
            Caption_Status VARCHAR(50)
        )
    '''
    create_comments_query = '''
        CREATE TABLE IF NOT EXISTS comments (
            Comment_Id VARCHAR(255) PRIMARY KEY,
            Video_Id VARCHAR(30),
            Comment_Author VARCHAR(100),
            Comment_Text TEXT,
            Comment_PublishedAt TIMESTAMP,
            Comment_Likes INT
        )
    '''
    cursor.execute(create_channels_query)
    cursor.execute(create_videos_query)
    cursor.execute(create_comments_query)
    print("Tables created successfully.")

# PostgreSQL insert
def insert_postgres(data):
    mydb = get_postgres_conn()
    cursor = mydb.cursor()

    if not check_table_exists(cursor, 'channels'):
        create_tables(cursor)

    channel = data['channel_data']
    insert_channel_query = '''INSERT INTO channels (Channel_Name, Channel_Id, Subscribers_count, Views, Total_Videos, Channel_Description, Playlist_Id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)'''

    try:
        cursor.execute(insert_channel_query, (
            channel['channel_name'],
            channel['channel_id'],
            channel['subscribers_count'],
            channel['channel_views'],
            channel['video_count'],
            channel['channel_description'],
            channel['playlist_id']
        ))
        mydb.commit()
    except psycopg2.IntegrityError as e:
        print(f"Failed to insert channel data: {e}")
        mydb.rollback()

    for video in data['video_data']:
        insert_video_query = '''INSERT INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Caption_Status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        try:
            cursor.execute(insert_video_query, (
                video['video_name'],
                video['channel_id'],
                video['video_id'],
                video['video_name'],
                video.get('tags', ''),
                video['thumbnail'],
                video['video_description'],
                video['published_at'],
                video['duration'],
                video['view_count'],
                video['like_count'],
                video['comment_count'],
                video['favorite_count'],
                video.get('caption_status', '')
            ))
            mydb.commit()
        except psycopg2.IntegrityError as e:
            print(f"Failed to insert video data: {e}")
            mydb.rollback()

    for comment in data.get('comments_data', []):
        insert_comment_query = '''INSERT INTO comments (Comment_Id, Video_Id, Comment_Author, Comment_Text, Comment_PublishedAt, Comment_Likes)
                                  VALUES (%s, % s, %s, %s, %s, %s)'''

        try:
            cursor.execute(insert_comment_query, (
                comment['comment_id'],
                comment['video_id'],
                comment['comment_author'],
                comment['comment_text'],
                comment['comment_published_at'],
                comment['comment_likes']
            ))
            mydb.commit()
        except psycopg2.IntegrityError as e:
            print(f"Failed to insert comment data: {e}")
            mydb.rollback()

    cursor.close()
    mydb.close()
    print("Data saved to PostgreSQL successfully!")

# Streamlit UI
def display_mongodb_data():
    st.subheader("MongoDB Data")
    docs = collection.find()
    data = pd.DataFrame(docs)
    st.dataframe(data)

def display_postgresql_data(table_name):
    st.subheader(f"PostgreSQL Data - {table_name.capitalize()}")
    try:
        mydb = get_postgres_conn()
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql(query, mydb)
        st.dataframe(data)
        mydb.close()
    except Exception as e:
        st.error(f"Error fetching data from PostgreSQL: {e}")

# Streamlit app interface
def app():
    st.title("YouTube Data Harvesting and Warehousing using SQL, Streamlit, and MongoDB")
    st.markdown(""" 
    **Skills Takeaway From This Project:**
    - Python scripting
    - Data Collection
    - Streamlit
    - MongoDB
    - API Integration
    - Data Management using SQL

    **Domain:**
    - Social Media

    **Problem Statement:**
    - Create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

    **Creator:**
    - Shubhangi Patil

    **Project:**
    - Data Science

    **GitHub Link:**
    - [GitHub Repository](https://github.com/shubhangivspatil)
    """)

    # Create a selectbox for navigating between pages
    page = st.selectbox("Select Page", ["Data Fetching", "Data Display", "Queries"])

    if page == "Data Fetching":
        channel_id = st.text_input("Enter YouTube Channel ID", "")
        if st.button("Fetch and Save Data"):
            if channel_id:
                data = insert_mongodb(channel_id)
                insert_postgres(data)
                st.success("Data fetched and saved successfully!")
            else:
                st.error("Please enter a valid YouTube Channel ID")

    elif page == "Data Display":
        # Display MongoDB and PostgreSQL data
        if st.checkbox("View MongoDB Data"):
            display_mongodb_data()

        table_choice = st.selectbox("Select PostgreSQL Table", ["channels", "videos", "comments"])
        if st.checkbox("View PostgreSQL Data"):
            display_postgresql_data(table_choice)

    elif page == "Queries":
        # Create SQLAlchemy engine
        engine = create_engine('postgresql://postgres:admin@localhost:5432/Youtube_1')  # Replace with your database URI

        # Function to display DataFrame within an expander with custom styling
        def display_expander_dataframe(df):
            with st.expander("Click here to view the data"):
                # Applying style to the DataFrame
                styled_df = df.head(1000).style.set_properties(**{'text-align': 'left'}) \
                                .set_table_styles([{'selector': 'th', 'props': [('text-align', 'left')]}]) \
                                .set_table_attributes('style="font-family: Arial, sans-serif;"')
                # Displaying the styled DataFrame
                st.table(styled_df)

        # Add tabs for different tables
        table_tabs = st.sidebar.radio("Select Table", ("Videos Table", "Channels Table", "Comments Table"))
        
        if table_tabs == "Videos Table":
            st.header('Videos Table')
            df = pd.read_sql_table("videos", engine)
            display_expander_dataframe(df)
            
        elif table_tabs == "Channels Table":
            st.header('Channels Table')
            df = pd.read_sql_table("channels", engine)
            display_expander_dataframe(df)
            
        elif table_tabs == "Comments Table":
            st.header('Comments Table')
            df = pd.read_sql_table("comments", engine)
            display_expander_dataframe(df)
        
        # 1. Names of all the videos and their corresponding channels
        st.header('1. Names of all the videos and their corresponding channels:')
        query1 = """
        SELECT v.title AS video_name, c.channel_name 
        FROM videos v 
        INNER JOIN channels c ON v.channel_id = c.channel_id
        """
        df1 = pd.read_sql_query(query1, engine)
        display_expander_dataframe(df1)

        # 2. Which channels have the most number of videos, and how many videos do they have?
        st.header('2. Channels with the most number of videos and their counts:')
        query2 = """
        SELECT c.channel_name, COUNT(v.video_id) AS num_videos 
        FROM channels c 
        LEFT JOIN videos v ON c.channel_id = v.channel_id 
        GROUP BY c.channel_name 
        ORDER BY num_videos DESC 
        LIMIT 5
        """
        df2 = pd.read_sql_query(query2, engine)
        display_expander_dataframe(df2)

        # 3. Top 10 most viewed videos and their respective channels
        st.header('3. Top 10 most viewed videos and their respective channels:')
        query3 = """
        SELECT v.title AS video_name, c.channel_name, v.views 
        FROM videos v 
        INNER JOIN channels c ON v.channel_id = c.channel_id 
        ORDER BY v.views DESC 
        LIMIT 10
        """
        df3 = pd.read_sql_query(query3, engine)
        display_expander_dataframe(df3)

        # 4. How many comments were made on each video, and what are their corresponding video names?
        st.header('4. Number of comments on each video:')
        query4 = """
        SELECT v.title AS video_name, COUNT(co.comment_id) AS num_comments 
        FROM comments co 
        INNER JOIN videos v ON co.video_id = v.video_id 
        GROUP BY v.title
        """
        df4 = pd.read_sql_query(query4, engine)
        display_expander_dataframe(df4)

        # 5. Which videos have the highest number of likes, and what are their corresponding channel names?
        st.header('5. Videos with the highest number of likes and their corresponding channel names:')
        query5 = """
        SELECT v.title AS video_name, c.channel_name, v.likes 
        FROM videos v 
        INNER JOIN channels c ON v.channel_id = c.channel_id 
        ORDER BY v.likes DESC 
        LIMIT 5
        """
        df5 = pd.read_sql_query(query5, engine)
        display_expander_dataframe(df5)

        # 6. What is the total number of likes for each video, and what are their corresponding video names?
        st.header('6. Total number of likes for each video:')
        query6 = """
        SELECT v.title AS video_name, SUM(v.likes) AS total_likes 
        FROM videos v 
        GROUP BY v.title
        """
        df6 = pd.read_sql_query(query6, engine)
        display_expander_dataframe(df6)

        # 7. What is the total number of views for each channel, and what are their corresponding channel names?
        st.header('7. Total number of views for each channel:')
        query7 = """
        SELECT c.channel_name, SUM(v.views) AS total_views 
        FROM channels c 
        LEFT JOIN videos v ON c.channel_id = v.channel_id 
        GROUP BY c.channel_name
        """
        df7 = pd.read_sql_query(query7, engine)
        display_expander_dataframe(df7)

        # 8. What are the names of all the channels that have published videos in the year 2022?
        st.header('8. Channels that published videos in the year 2022:')
        query8 = """
        SELECT DISTINCT c.channel_name 
        FROM videos v 
        INNER JOIN channels c ON v.channel_id = c.channel_id 
        WHERE EXTRACT(YEAR FROM v.published_date) = 2022
        """
        df8 = pd.read_sql_query(query8, engine)
        display_expander_dataframe(df8)

        # 9. What is the average duration of all videos in each channel, and what are their corresponding channel names?
        st.header('9. Average duration of all videos in each channel:')
        query9 = """
        SELECT c.channel_name, AVG(v.duration) AS avg_duration 
        FROM videos v 
        INNER JOIN channels c ON v.channel_id = c.channel_id 
        GROUP BY c.channel_name
        """
        df9 = pd.read_sql_query(query9, engine)
        display_expander_dataframe(df9)

        # 10. Which videos have the highest number of comments, and what are their corresponding channel names?
        st.header('10. Videos with the highest number of comments and their corresponding channel names:')
        query10 = """
        SELECT v.title AS video_name, c.channel_name, COUNT(co.comment_id) AS comment_count 
        FROM videos v 
        INNER JOIN channels c ON v.channel_id = c.channel_id
        INNER JOIN comments co ON co.video_id = v.video_id
        GROUP BY v.title, c.channel_name
        ORDER BY comment_count DESC 
        LIMIT 5
        """
        df10 = pd.read_sql_query(query10, engine)
        display_expander_dataframe(df10)

if __name__ == "__main__":
    app()