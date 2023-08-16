from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import mysql.connector
from datetime import datetime
import streamlit as st
from streamlit import session_state
import pandas as pd
import time

# Provide your API key
api_key = 'AIzaSyCM6pL44BGZUVLK0NEBr5Y45EPZT1mDmiA'

# Build the YouTube service
youtube = build('youtube', 'v3', developerKey=api_key)

# Connect to the MongoDB server
mongo_client = MongoClient('mongodb+srv://kapil:srk012126@newcluster.ataklof.mongodb.net/')
mongo_db = mongo_client['youtube_data_harvesting']
mongo_collection = mongo_db['channel_11']

# Connect to the MySQL server
mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='srk012126',
    database='channel_1'
)
mysql_cursor = mysql_connection.cursor()

def transfer_data_to_mysql(mysql_cursor, mysql_connection, mongo_collection):

    # Fetch the channel data from MongoDB
    channel_data = mongo_collection.find_one()

    # Insert channel data into the channel_data table
    mysql_cursor.execute('''
        INSERT INTO channel_data (
            channel_id, channel_name, subscription_count,
            channel_views, channel_description
        ) VALUES (
            %s, %s, %s, %s, %s
        )
    ''', (
        channel_data['Channel_Id'],
        channel_data['Channel_Name'],
        channel_data['Subscription_Count'],
        channel_data['Channel_Views'],
        channel_data['Channel_Description']
    ))

    # Retrieve channel ID
    channel_id = channel_data['Channel_Id']


    # Retrieve playlist information
    playlists = channel_data.get('Playlists', {})

    # Iterate over the playlists and insert into MySQL
    for playlist_id, playlist_name in playlists.items():
        # Insert playlist data into MySQL
        mysql_cursor.execute('''
            INSERT INTO playlist_data (playlist_id, playlist_name, channel_id)
            VALUES (%s, %s, %s)
        ''', (playlist_id, playlist_name, channel_id))
        mysql_connection.commit()


    # Retrieve video data from MongoDB
    video_details = list(mongo_collection.find())

    # Iterate over the videos and insert them into MySQL
    for video in video_details:
        videos = video.get('Videos', [])

        for video_info in videos:
            playlist_id = video.get('Playlist_ID')
            video_id = video_info.get('Video_ID')
            title = video_info.get('Title')
            description = video_info.get('Description')
            published_date_str = video_info.get('Published_Date')

            # Convert published date string to datetime
            published_date = datetime.strptime(published_date_str, "%Y-%m-%dT%H:%M:%SZ") if published_date_str else None
            view_count = video_info.get('View_Count')
            like_count = video_info.get('Like_Count')
            dislike_count = video_info.get('Dislike_Count')
            favorite_count = video_info.get('Favorite_Count')
            comment_count = video_info.get('Comment_Count')
            duration = video_info.get('Duration')
            thumbnail = video_info.get('Thumbnail')
            caption_status = video_info.get('Caption_Status')

            mysql_cursor.execute('''
                INSERT INTO video_data (
                    video_id, playlist_id, title, description, published_date, view_count,
                    like_count, dislike_count, favorite_count, comment_count, duration,
                    thumbnail, caption_status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    playlist_id = VALUES(playlist_id),
                    title = VALUES(title),
                    description = VALUES(description),
                    published_date = VALUES(published_date),
                    view_count = VALUES(view_count),
                    like_count = VALUES(like_count),
                    dislike_count = VALUES(dislike_count),
                    favorite_count = VALUES(favorite_count),
                    comment_count = VALUES(comment_count),
                    duration = VALUES(duration),
                    thumbnail = VALUES(thumbnail),
                    caption_status = VALUES(caption_status)
            ''', (
                video_id, playlist_id, title, description, published_date, view_count,
                like_count, dislike_count, favorite_count, comment_count, duration,
                thumbnail, caption_status
            ))
            mysql_connection.commit()

    
    # Retrieve comments from MongoDB
    comments = mongo_collection.aggregate([
        {"$unwind": "$Videos"},
        {"$unwind": "$Videos.Comments"}
    ])

    # Iterate over the comments and insert them into MySQL
    for comment in comments:
        video_id = comment['Videos']['Video_ID']
        comment_id = comment['Videos']['Comments']['Comment_ID']
        comment_text = comment['Videos']['Comments']['Comment_Text']
        comment_author = comment['Videos']['Comments']['Comment_Author']
        comment_published_date = comment['Videos']['Comments']['Comment_Published_Date']
        comment_published_date = datetime.strptime(comment_published_date, "%Y-%m-%dT%H:%M:%SZ")  # Convert to datetime object

        mysql_cursor.execute('''
            INSERT INTO comment_data (
                comment_id, comment_text, comment_author, comment_published_date, video_id
            ) VALUES (
                %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                comment_text = VALUES(comment_text),
                comment_author = VALUES(comment_author),
                comment_published_date = VALUES(comment_published_date),
                video_id = VALUES(video_id)
        ''', (
            comment_id, comment_text, comment_author, comment_published_date, video_id
        ))
        mysql_connection.commit()


def get_video_details(api_key, playlist_id):
    youtube = build('youtube', 'v3', developerKey=api_key)

    try:
        response = youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=playlist_id,
            maxResults=40  # Adjust as per your requirement, maximum is 50
        ).execute()

        video_details = []

        if 'items' in response:
            videos = response['items']

            video_ids = [video['snippet']['resourceId']['videoId'] for video in videos]

            video_response = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=','.join(video_ids)
            ).execute()

            video_items = video_response['items']

            for video, item in zip(videos, video_items):
                video_dict = {
                'Playlist_ID': playlist_id,
                'Video_ID': video['snippet']['resourceId']['videoId'],
                'Title': item['snippet']['title'],
                'Description': item['snippet']['description'],
                'Published_Date': item['snippet']['publishedAt'],
                'View_Count': item['statistics']['viewCount'],
                'Like_Count': item['statistics'].get('likeCount', 0),
                'Dislike_Count': item['statistics'].get('dislikeCount', 0),
                'Favorite_Count': item['statistics'].get('favoriteCount', 0),
                'Comment_Count': item['statistics'].get('commentCount', 0),
                'Duration': item['contentDetails']['duration'],
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Caption_Status': item['contentDetails']['caption']
            }
            video_details.append(video_dict)

        return video_details
    except HttpError as e:
        print(f"An error occurred: {e}")
        return None


def get_channel_info(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)

    try:
        response = youtube.channels().list(
            part='snippet,contentDetails,topicDetails,status,statistics',
            id=channel_id
        ).execute()

        channel_info = {}

        if 'items' in response and len(response['items']) > 0:
            channel = response['items'][0]

            channel_info['Channel_Name'] = channel['snippet']['title']
            channel_info['Channel_Id'] = channel['id']
            channel_info['Subscription_Count'] = channel['statistics']['subscriberCount']
            channel_info['Channel_Views'] = channel['statistics']['viewCount']
            channel_info['Channel_Description'] = channel['snippet']['description']

            playlist_response = youtube.playlists().list(
                part='snippet',
                channelId=channel_id,
                maxResults=40  # Adjust as per your requirement, maximum is 50
            ).execute()

            playlists = playlist_response.get('items', [])
            playlist_info = {}

            for playlist in playlists:
                playlist_id = playlist['id']
                playlist_name = playlist['snippet']['title']
                playlist_info[playlist_id] = playlist_name

            channel_info['Playlists'] = playlist_info

        return channel_info
    except HttpError as e:
        print(f"An error occurred: {e}")
        return None


def get_video_comments(api_key, video_id):
    try:
        response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id, 
            maxResults=20  # Adjust as per your requirement, maximum is 100
        ).execute()

        comments = []

        if 'items' in response:
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comment_id = item['id']
                comment_text = comment['textDisplay']
                comment_author = comment['authorDisplayName']
                comment_published_date = comment['publishedAt']

                # Check if comment_text is not null
                if comment_text:
                    comments.append({
                        'Comment_ID': comment_id,
                        'Comment_Text': comment_text,
                        'Comment_Author': comment_author,
                        'Comment_Published_Date': comment_published_date,
                        'Video_ID': video_id
                    })

        return comments
    except HttpError as e:
        if e.resp.status == 403 and 'commentsDisabled' in str(e):
            # Handle commentsDisabled error
            print(f"Comments are disabled for Video ID: {video_id}")
        elif e.resp.status == 404:
            # Handle the HTTP 404 error
            return None
        else:
            # Handle other HttpError exceptions
            print(f"An error occurred: {e}")
        return None
    
def get_table_names(mysql_cursor):
    mysql_cursor.execute("SHOW TABLES")
    result = mysql_cursor.fetchall()
    table_names = [row[0] for row in result]
    return table_names

def get_table_data(mysql_cursor, table_name):
    # Retrieve column names
    mysql_cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = mysql_cursor.fetchall()
    column_names = [column[0] for column in columns]

    # Retrieve table data
    mysql_cursor.execute(f"SELECT * FROM {table_name}")
    result = mysql_cursor.fetchall()

    return column_names, result

def fetch_query_result(query):
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    column_names = [i[0] for i in mysql_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    return df

# Query 1: Names of all the videos and their corresponding channels
query1 = '''SELECT ch.channel_name, vi.title as video_name FROM channel_data AS ch
JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
JOIN comment_data AS cm ON vi.video_id = cm.video_id order by ch.channel_name;'''

# Query 2: Channels with the most number of videos and the number of videos
query2 = '''SELECT ch.channel_name, COUNT(vi.video_id) AS number_of_videos
FROM channel_data AS ch
JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
GROUP BY ch.channel_name;'''

# Query 3: Top 10 most viewed videos and their respective channels
query3 = '''SELECT channel_name, video_name, view_count
FROM (
  SELECT ch.channel_name, vi.title AS video_name, vi.view_count,
         ROW_NUMBER() OVER (PARTITION BY ch.channel_id ORDER BY vi.view_count DESC) AS rn
  FROM channel_data AS ch
  JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
  JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
) ranked
WHERE rn <= 10;'''

# Query 4: Number of comments on each video and their corresponding video names
query4 = '''SELECT ch.channel_name, vi.title AS video_name, COUNT(cm.comment_id) AS number_of_comments
FROM channel_data AS ch
JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
JOIN comment_data AS cm ON vi.video_id = cm.video_id 
GROUP BY ch.channel_name, vi.title
ORDER BY ch.channel_name;'''

# Query 5: Videos with the highest number of likes and their corresponding channel names
query5 = '''SELECT channel_name, video_name, like_count
FROM (
  SELECT ch.channel_name, vi.title AS video_name, vi.like_count,
         RANK() OVER (PARTITION BY ch.channel_id ORDER BY vi.like_count DESC) AS video_rank
  FROM channel_data AS ch
  JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
  JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
) ranked_videos
WHERE video_rank = 1
ORDER BY like_count DESC
LIMIT 0, 50000;'''

# Query 6: Total number of likes and dislikes for each video and their corresponding video names
query6 = '''SELECT vi.title as video_name, vi.like_count, vi.dislike_count FROM video_data AS vi;'''

# Query 7: Total number of views for each channel and their corresponding channel names
query7 = '''SELECT ch.channel_name, SUM(vi.view_count) as total_view_count FROM channel_data AS ch
JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
GROUP BY ch.channel_name;'''

# Query 8: Channels that published videos in the year 2022
query8 = '''SELECT ch.channel_name, vi.title as video_name, year(vi.published_date)as published_year FROM channel_data AS ch
JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
WHERE YEAR(vi.published_date) = 2022;'''

# Query 9: Average duration of all videos in each channel and their corresponding channel names
query9 = '''SELECT ch.channel_name, AVG(durations.duration_minutes * 60 + durations.duration_seconds) AS average_duration_seconds
FROM channel_data AS ch
JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
JOIN (
  SELECT 
    SUBSTRING_INDEX(SUBSTRING_INDEX(vi.duration, 'M', 1), 'T', -1) AS duration_minutes,
    SUBSTRING_INDEX(SUBSTRING_INDEX(vi.duration, 'S', 1), 'M', -1) AS duration_seconds,
    vi.playlist_id
  FROM video_data AS vi
  WHERE vi.duration IS NOT NULL
) AS durations ON pl.playlist_id = durations.playlist_id
GROUP BY ch.channel_name;'''

# Query 10: Videos with the highest number of comments and their corresponding channel names
query10 = '''SELECT channel_name, video_name, highest_number_of_comment
FROM (
  SELECT ch.channel_name, vi.title AS video_name, COUNT(cm.comment_id) AS highest_number_of_comment,
         RANK() OVER (PARTITION BY ch.channel_id ORDER BY COUNT(cm.comment_id) DESC) AS comment_rank
  FROM channel_data AS ch
  JOIN playlist_data AS pl ON ch.channel_id = pl.channel_id
  JOIN video_data AS vi ON pl.playlist_id = vi.playlist_id
  JOIN comment_data AS cm ON vi.video_id = cm.video_id
  GROUP BY ch.channel_id, vi.title
) ranked
WHERE comment_rank = 1;'''

def main():
    # Streamlit app
    st.title("YouTube Data Harvesting")

    # Initialize session state if not present
    if 'mysql_data_loaded' not in st.session_state:
        st.session_state.mysql_data_loaded = False

    # Page navigation
    page = st.radio("Navigation", ['Extraction_Of_Data', 'Data_Migration', 'Mysql_Tables','Query_Execution'])

    if page == 'Extraction_Of_Data':
        page1()
    elif page == 'Data_Migration':
        page2()
    elif page == 'Mysql_Tables':
        page3()
    elif page == 'Query_Execution':
        page4()

def page1():
    # Channel ID input
    channel_id = st.text_input("Enter Channel ID 👇")

    if channel_id:
        if st.button("Load into MongoDB"):
            progress_bar = st.progress(0)  # Initialize the progress bar
            progress_text = st.empty()  # Placeholder to update progress text
            with st.spinner("Loading data into MongoDB..."):
                 # Retrieve channel information
                 channel_info = get_channel_info(api_key, channel_id)
                 st.session_state.mongodb_data_loaded = True
                 for i in range(100):  # Simulating progress from 0 to 100%
                    time.sleep(0.1)  # Simulating some work being done
                    progress_bar.progress(i + 1)
                    progress_text.text(f"Progress: {i+1}%")
            progress_text.empty()  # Clear the progress text
                 
            # Print the channel information
            if channel_info is not None:
                # Insert channel information into the collection b
                mongo_collection.insert_one(channel_info)

               
                image_path = 'C:/Users/KAPIL/Downloads/YT.png'
                # Define the layout with columns
                col1, col2 = st.columns([0.5, 0.3])

                # Place the subheader in the first column
                col1.subheader(f"Channel Name: {channel_info['Channel_Name']}")

                # Load and display the image in the second column
                @st.cache_data()
                def load_image(image_path):
                    with open(image_path, "rb") as f:
                        return f.read()

                image_data = load_image(image_path)

                # Specify the image width
                image_width = 50  # Adjust this value to control the image size

                # Display the image with the specified width
                col2.image(image_data, use_column_width=False, width=image_width)
                   

                # Retrieve playlist information
                playlists = channel_info.get('Playlists', {})
                if playlists:
                    for playlist_id, playlist_name in playlists.items():
                        playlist_info = { 
                            'Playlist_ID': playlist_id,
                            'Playlist_Name': playlist_name,
                            'Videos': []
                        }

                        # Call the function to get the video details for the current playlist
                        video_details = get_video_details(api_key, playlist_id)

                        if video_details is not None:
                            for video in video_details:
                                # Retrieve video comments for the current video
                                video_comments = get_video_comments(api_key, video['Video_ID'])

                                # Check if there are comments and filter out null comments
                                if video_comments is not None:
                                    video_comments = [comment for comment in video_comments if comment['Comment_Text']]
                                    video['Comments'] = video_comments
                                else:
                                    video['Comments'] = []

                                # Add the video to the playlist info
                                playlist_info['Videos'].append(video)

                        # Insert the playlist information into MongoDB
                        mongo_collection.insert_one(playlist_info)

                st.success("Data loaded into MongoDB successfully!",icon='✅')
                st.snow()
                st.write("\n\n\n\n")  # Create a gap of 3 lines
                st.write("To fetch the data from MongoDB and load into MySQL, click ----> page2.")

def page2():
    st.subheader("To Load The Youtube Details Into Mysql Click The Button")
    if st.button("Load into MySQL"):
        with st.spinner("Loading data into MySQL..."):
            transfer_data_to_mysql(mysql_cursor, mysql_connection, mongo_collection)
            st.session_state.mysql_data_loaded = True
       
        st.write("\n\n\n\n") 
        st.success("Data loaded into MySQL successfully!",icon='✅')
        st.snow()
        st.write("\n\n\n\n")  # Create a gap of 3 lines
        st.write("To see the Channel Details, click ----> page3.")

def page3():
    st.subheader("To See The Channel Details Use The SelectBox")
    if st.session_state.mysql_data_loaded:
        # Retrieve table names from MySQL
        table_names = get_table_names(mysql_cursor)

        # Select box for table selection
        selected_table = st.selectbox("Select Table", table_names)

        if selected_table:
            # Retrieve column names and table data from the selected table
            column_names, table_data = get_table_data(mysql_cursor, selected_table)

            # Create a DataFrame using the table data and column names
            df = pd.DataFrame(table_data, columns=column_names)

            # Display the table data with column names as column index
            st.write(df)

    else:
        st.write("MySQL data not loaded yet.")

def page4():
    st.title("Query Execution")

    # Define the list of query names and their corresponding SQL queries
    queries = {
        "Query 1: Names of all the videos and their corresponding channels": query1,
        "Query 2: Channels with the most number of videos and the number of videos": query2,
        "Query 3: Top 10 most viewed videos and their respective channels": query3,
        "Query 4: Number of comments on each video and their corresponding video names": query4,
        "Query 5: Videos with the highest number of likes and their corresponding channel names": query5,
        "Query 6: Total number of likes and dislikes for each video and their corresponding video names": query6,
        "Query 7: Total number of views for each channel and their corresponding channel names": query7,
        "Query 8: Channels that published videos in the year 2022": query8,
        "Query 9: Average duration of all videos in each channel and their corresponding channel names": query9,
        "Query 10: Videos with the highest number of comments and their corresponding channel names": query10
    }

    # Display the selectbox to choose the query
    selected_query_name = st.selectbox("Select Query", list(queries.keys()))
 
    if st.button("Execute Query"):
        query = queries[selected_query_name]
        df = fetch_query_result(query)
        st.dataframe(df)

# Entry point
if __name__ == '__main__':
    main()
