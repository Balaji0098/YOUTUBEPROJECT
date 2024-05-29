import googleapiclient.discovery
import pandas as pd
import pymysql 
import streamlit as st

###########################################


api_service_name = "youtube"
api_version = "v3"
api_key = 'AIzaSyC5GvYQ0iMIw6FtnAsx2Bn32tS9dnzPxAE'
youtube = googleapiclient.discovery.build(
api_service_name, api_version, developerKey= api_key)

################################################


# to get all multiple channel data

def channel_data(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id)
    response = request.execute()

    data = {'channel_name': response['items'][0]['snippet']['title'],
            'channel_ds': response['items'][0]['snippet']['description'],
            'channel_pat': response['items'][0]['snippet']['publishedAt'],
            'channel_pid' : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'video_count': response['items'][0]['statistics']['videoCount'],
            'view_count': response['items'][0]['statistics']['viewCount'],
            'sub_count': response['items'][0]['statistics']['subscriberCount']
            
            
           }
    
    return data


################################################################


# to get all multiple channel video ids 

def get_videos_ids(channel_id):

    video_ids =[]

    response = youtube.channels().list(id= channel_id,
                                      part='contentDetails').execute()
    
    if 'items' in response:

          playlists_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:

        response1 = youtube.playlistItems().list(
                                                                            part='snippet',
                                                                            playlistId = playlists_Id,
                                                                            maxResults=50 , 
                                                                            pageToken=next_page_token).execute()

        for item in response1.get('items', []): 


            video_ids.append(item['snippet']['resourceId']['videoId'])

        next_page_token=response1.get('nextPageToken')   

        if  next_page_token is None:

            break

    return video_ids




##################################################################


def convert_duration(duration_str):
    # Remove the 'PT' prefix and 'S' suffix
    duration_str = duration_str[2:-1]

    # Initialize variables
    hours = 0
    minutes = 0
    seconds = 0

    # Split the duration string by 'H', 'M', and 'S' if present
    if 'H' in duration_str:
        hours, duration_str = duration_str.split('H')
        hours = int(hours)
    if 'M' in duration_str:
        minutes, duration_str = duration_str.split('M')
        minutes = int(minutes)
    if 'S' in duration_str:
        seconds = int(duration_str[:-1])  # Remove the 'S' suffix

    # Convert the duration to total seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds

    # Convert total seconds to HH:MM:SS format
    formatted_duration = f"{total_seconds // 3600:02d}:{(total_seconds % 3600) // 60:02d}:{total_seconds % 60:02d}"
    return formatted_duration


#######################################################


# to get video information we adding nested forloop

def get_video_info(video_IDS):

    video_data=[]
    
    for video_id in video_IDS :
        request = youtube.videos().list(
        part='snippet, contentDetails,statistics',
        id=video_id 
        
        )

        response = request.execute()

        for item in response['items']:
            video_duration = convert_duration(item['contentDetails']['duration'])

            data = dict (
                                            channel_name = item['snippet']['channelTitle'],
                                            channel_Id = item['snippet']['channelId'],
                                            video_Id = item['id'],
                                            video_title = item['snippet']['title'],
                                            video_tags = ',' .join(item['snippet'].get('tags',[])),
                                            video_thumbnail = item['snippet']['thumbnails']['default']['url'],
                                            video_description =item['snippet'].get('description'),
                                            video_published = item['snippet']['publishedAt'],
                                            video_duration =  convert_duration(item['contentDetails']['duration']),
                                            video_view_count = item['statistics'].get('viewCount'),
                                            video_like_count = item['statistics'].get('likeCount'),
                                            video_favorite_count = item['statistics']['favoriteCount'],
                                            video_comment_count = item['statistics'].get('commentCount'),
                                            video_quality = item['contentDetails']['definition'],
                                            video_caption_status = item['contentDetails'].get('caption')
                                )


        video_data.append(data)


    return video_data   




#####################################################################



def get_comment_info(video_ids):

    comment_data = []

    try:

        for video_id in video_ids :
                request = youtube.commentThreads().list(
                    part = 'snippet',
                    videoId= video_id,
                    maxResults = 50 )

                response = request.execute()

                for item in response.get('items' , []):
                    data = dict(comment_id = item['snippet']['topLevelComment']['id'],
                                video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                                comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                comment_publish_date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                               )

                    comment_data.append(data)

    except:
        pass
    
    return comment_data




#####################################################################


# to get playlist details

def get_playlist_details(channel_id):

        next_page_token = None
        ALL_data = []
        while True:

            request = youtube.playlists().list(
                    part = 'snippet,contentDetails',
                    channelId = channel_id,
                    maxResults = 50,
                    pageToken = next_page_token,
            )
            response = request.execute()

            for item in response ['items']:
                data = dict( playlists_Id =  item['id'],
                            Title = item['snippet']['title'],
                            channel_id = item['snippet']['channelId'],
                            channel_Name = item['snippet']['channelTitle'],
                            published_date = item['snippet']['publishedAt'],
                            video_count = item['contentDetails']['itemCount']
                            )


                
                ALL_data.append(data)

            next_page_token = response.get('nextPageToken') 

            if next_page_token is None:

                    break    

        return ALL_data


#####################################################################


def channel_details(channel_id):
    CH_details =channel_data(channel_id)
    PL_details =get_playlist_details(channel_id)
    VI_ids =get_videos_ids(channel_id)
    VI_details =get_video_info(VI_ids)
    COM_details =get_comment_info(VI_ids)

    DF_ALL_channel_details = pd.DataFrame([CH_details])
    DF_ALL_playlists_details = pd.DataFrame(PL_details)
    DF_ALL_video_details = pd.DataFrame(VI_details)
    DF_comment_data_video_IDS = pd.DataFrame(COM_details)


#####################################################################

# TO CREATE TABLE IN MY SQL (BEGINNING)


    connection = pymysql.connect(host = 'localhost',user = 'root',password = 'Bala@0706',database = 'capstone')

    cursor = connection.cursor()

    return  DF_ALL_channel_details , DF_ALL_playlists_details ,DF_ALL_video_details ,DF_comment_data_video_IDS 

def channels_table(DF_ALL_channel_details):


    connection = pymysql.connect(host = 'localhost',user = 'root',password = 'Bala@0706',database = 'capstone')

    cursor = connection.cursor()



    

    create_query = '''create table if not exists channels(channel_name varchar(100),
                                                            channel_ds text,
                                                            channel_pat varchar (50),
                                                            channel_pid varchar(80) primary key,
                                                            video_count int,
                                                            view_count bigint,
                                                            sub_count bigint
                                                            )'''
    cursor.execute(create_query)
    connection.commit()


      




    for index , row in  DF_ALL_channel_details.iterrows():
        insert_query = '''insert into channels(channel_name,
                                            channel_ds,      
                                            channel_pat, 
                                            channel_pid,
                                            video_count,
                                            view_count,
                                            sub_count)
                                            
                                            values(%s, %s, %s, %s, %s, %s, %s )'''
        
        values = (row['channel_name'],
                row['channel_ds'],
                row['channel_pat'],
                row['channel_pid'],
                row['video_count'],
                row['view_count'],
                row['sub_count'])
        
        
        cursor.execute( insert_query , values )
        connection.commit()


def playlist_table( DF_ALL_playlists_details ):

    connection = pymysql.connect(host = 'localhost',user = 'root',password = 'Bala@0706',database = 'capstone')

    cursor = connection.cursor()

    




    create_query = '''create table if not exists playlists(playlists_Id varchar(100) primary key,
                                                            Title text,
                                                            channel_id varchar (100),
                                                            channel_Name varchar(100),
                                                            published_date varchar(50),
                                                            video_count int
                                                            )'''



    cursor.execute(create_query)
    connection.commit()



    
    for index , row in DF_ALL_playlists_details.iterrows():
            insert_query = '''insert into playlists(playlists_Id,
                                            Title,      
                                            channel_id, 
                                            channel_Name,
                                            published_date,
                                            video_count)
                                            
                                            values(%s, %s, %s, %s, %s, %s )'''
            
            
            
            values = (row['playlists_Id'],
                    row['Title'],
                    row['channel_id'],
                    row['channel_Name'],
                    row['published_date'],
                    row['video_count'])


            cursor.execute( insert_query , values )
            connection.commit()



def videos_table(DF_ALL_video_details):

    connection = pymysql.connect(host = 'localhost',user = 'root',password = 'Bala@0706',database = 'capstone')

    cursor = connection.cursor()

   




    create_query = '''create table if not exists videos( 
                                                channel_name varchar(100),
                                                channel_Id varchar(100),
                                                video_Id varchar(40) primary key,
                                                video_title varchar(150),
                                                video_tags  text,
                                                video_thumbnail varchar(200),
                                                video_description text,
                                                video_published varchar(100),
                                                video_duration  varchar(10),
                                                video_view_count bigint,
                                                video_like_count bigint,
                                                video_favorite_count int,
                                                video_comment_count int,
                                                video_quality varchar(20),
                                                video_caption_status varchar(50)
                                                
                                                            )'''



    cursor.execute(create_query)
    connection.commit()



    for index , row in DF_ALL_video_details.iterrows():
                            insert_query = '''insert into videos( 
                                                    channel_name,
                                                    channel_Id,
                                                    video_Id,
                                                    video_title,
                                                    video_tags,
                                                    video_thumbnail,
                                                    video_description,
                                                    video_published,
                                                    video_duration,
                                                    video_view_count,
                                                    video_like_count,
                                                    video_favorite_count,
                                                    video_comment_count,
                                                    video_quality,
                                                    video_caption_status
                                                    )
                                                            
                                                            values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    
                    
                    
                            values = (  
                                    row['channel_name'],
                                    row['channel_Id'],
                                    row['video_Id'],
                                    row['video_title'],
                                    row['video_tags'],
                                    row['video_thumbnail'],
                                    row['video_description'],
                                    row['video_published'],
                                    row['video_duration'],
                                    row['video_view_count'],
                                    row['video_like_count'],
                                    row['video_favorite_count'],
                                    row['video_comment_count'],
                                    row['video_quality'],
                                    row['video_caption_status'] 
                                    )
                    
                    
                            cursor.execute(insert_query , values)
                            connection.commit()
                            

def comments_table(DF_comment_data_video_IDS):

  connection = pymysql.connect(host = 'localhost',user = 'root',password = 'Bala@0706',database = 'capstone')

  cursor = connection.cursor()


  




  create_query = '''create table if not exists comments(comment_id varchar(100) primary key,
                                                        video_id varchar(50),
                                                      comment_text text,
                                                      comment_author varchar(150),
                                                      comment_publish_date varchar(120)
                                                  )'''



  cursor.execute(create_query)
  connection.commit()



  for index, row in DF_comment_data_video_IDS.iterrows():
          insert_query = '''insert into comments( comment_id,
                                          video_id,
                                          comment_text,
                                          comment_author,
                                          comment_publish_date
                                      )

                                      
                                      values(%s ,%s ,%s ,%s ,%s)'''

          values = (row['comment_id'],
          row['video_id'],
          row['comment_text'],
          row['comment_author'],
          row['comment_publish_date']
          )

          cursor.execute(insert_query , values)
          connection.commit()



def tables(DF_ALL_channel_details , DF_ALL_playlists_details ,DF_ALL_video_details ,DF_comment_data_video_IDS):
    channels_table(DF_ALL_channel_details)
    playlist_table(DF_ALL_playlists_details)
    videos_table(DF_ALL_video_details )
    comments_table(DF_comment_data_video_IDS)

    return 'tables created successfully'


#####################################################################

# STREAMLIT STARTING CODE


def show_channels_table(DF_ALL_channel_details):

    df1 = st.dataframe(DF_ALL_channel_details)

    return df1

def show_playlists_table( DF_ALL_playlists_details):

    df2 = st.dataframe(DF_ALL_playlists_details)

    return df2

def show_videos_table(DF_ALL_video_details):

    df3 = st.dataframe(DF_ALL_video_details)

    return df3

def show_comments_table(DF_comment_data_video_IDS):


    df4 = st.dataframe(DF_comment_data_video_IDS)

    return df4



with st.sidebar:
    st.title(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('MYSQL')
    st.caption('API Integration')
    st.caption('Data Management using MYSQL')

channel_id = st.text_input('Enter the channel ID')



if st.button('collect and store data'):
    DF_ALL_channel_details , DF_ALL_playlists_details ,DF_ALL_video_details ,DF_comment_data_video_IDS=channel_details(channel_id)
    show_channels_table(DF_ALL_channel_details)
    show_playlists_table(DF_ALL_playlists_details)
    show_videos_table(DF_ALL_video_details)  
    show_comments_table(DF_comment_data_video_IDS) 


    tables = tables(DF_ALL_channel_details , DF_ALL_playlists_details ,DF_ALL_video_details ,DF_comment_data_video_IDS)
    st.success(tables)

       

  # STREAMLIT QUERY AND ANSWER PART BEGGINNING



connection = pymysql.connect(host = 'localhost',user = 'root',password = 'Bala@0706',database = 'capstone')

cursor = connection.cursor()

questions = st.selectbox('Select your question' , ('1. All the videos and channel name',
                                                   '2. channels with most number of videos',
                                                   '3. 10 most viewed videos',
                                                   '4. comments in each videos',
                                                   '5. videos with highest likes',
                                                   '6. likes of all videos',
                                                   '7. views of each channels',
                                                   '8. videos published in the year of 2022',
                                                   '9. average duration of all videos in each channels',
                                                   '10. videos with highest number of comments'))



if questions == '1. All the videos and channel name':
    query = "SELECT video_title, channel_name FROM videos"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '2. channels with most number of videos':
    query = "SELECT channel_name, COUNT(*) AS video_count FROM videos GROUP BY channel_name ORDER BY video_count DESC LIMIT 10"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '3. 10 most viewed videos':
    query = "SELECT video_title, video_view_count FROM videos ORDER BY video_view_count DESC LIMIT 10"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '4. comments in each videos':
    query = "SELECT video_title, video_comment_count FROM videos"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '5. videos with highest likes':
    query = "SELECT video_title, video_like_count FROM videos ORDER BY video_like_count DESC LIMIT 10"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '6. likes of all videos':
    query = "SELECT SUM(video_like_count) AS video_like_count FROM videos"
    cursor.execute(query)
    result = cursor.fetchone()
    st.write(result[0])

elif questions == '7. views of each channels':
    query = "SELECT channel_name, SUM(video_view_count) AS video_view_count FROM videos GROUP BY channel_name"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '8. videos published in the year of 2022':
    query = "SELECT video_title, video_published FROM videos WHERE YEAR (str_to_date(video_published, '%Y-%m-%dT%H:%i:%sZ')) = 2023"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '9. average duration of all videos in each channels':
    query = "SELECT channel_name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video_duration)))), '%H:%i:%s') AS video_duration FROM videos GROUP BY channel_name"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

elif questions == '10. videos with highest number of comments':
    query = "SELECT video_title, COUNT(*) AS video_comment_count FROM videos GROUP BY video_title ORDER BY video_comment_count DESC LIMIT 10"
    cursor.execute(query)
    result = cursor.fetchall()
    st.write(result)

    # only 9th query pending



cursor.close()
connection.close()s





