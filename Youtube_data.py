import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Create SQLAlchemy engine
engine = create_engine('postgresql://postgres:admin@localhost:5432/my_db')  # Replace with your database URI
# Function to display DataFrame within an expander with custom styling
def display_expander_dataframe(df):
    with st.expander("Click here to view the data"):
        # Applying style to the DataFrame
        styled_df = df.head(1000).style.set_properties(**{'text-align': 'left'}) \
                            .set_table_styles([{'selector': 'th', 'props': [('text-align', 'left')]}]) \
                            .set_table_attributes('style="font-family: Arial, sans-serif;"')
        # Displaying the styled DataFrame
        st.table(styled_df)

# Streamlit app
def main():
   
    st.title('YouTube Data Harvesting and Warehousing using SQL and Streamlit')
    
    # Project Details Section
    st.sidebar.title('Project Details')
    st.sidebar.subheader('**Project Title:** YouTube Data Harvesting and Warehousing using SQL and Streamlit')
    st.sidebar.subheader('**Skills Takeaway:**')
    st.sidebar.markdown('- Python scripting\n- Data Collection\n- Streamlit\n- API integration\n- Data Management using SQL')
    st.sidebar.subheader('**Domain:**')
    st.sidebar.markdown('Social Media')

      # Add tabs for different tables
    table_tabs = st.sidebar.radio("Select Table", ("Video Table", "Channel Table", "Comment Table"))
    
    if table_tabs == "Video Table":
        st.header('Video Table')
        # Dropdown for selecting tables
        df = pd.read_sql_table("video_new", engine)
        display_expander_dataframe(df)
        
    elif table_tabs == "Channel Table":
        st.header('Channel Table')
        # Dropdown for selecting tables
        df = pd.read_sql_table("channel_new", engine)
        display_expander_dataframe(df)
        
    elif table_tabs == "Comment Table":
        st.header('Comment Table')
        # Dropdown for selecting tables
        df = pd.read_sql_table("comment_new", engine)
        display_expander_dataframe(df)
    
    # 1. Names of all the videos and their corresponding channels
    st.header('1. Names of all the videos and their corresponding channels:')
    query1 = "SELECT video_name, channel_name FROM channel_new, video_new"
    df1 = pd.read_sql_query(query1, engine)
    display_expander_dataframe(df1)
     # 2. Which channels have the most number of videos, and how many videos do they have?
    st.header('2. Channels with the most number of videos and their counts:')
    query2 = "SELECT channel_name, video_count AS num_videos FROM channel_new ORDER BY video_count DESC LIMIT 5"
    df2 = pd.read_sql_query(query2, engine)
    display_expander_dataframe(df2)
    # 3. Top 10 most viewed videos and their respective channels
    st.header('3. Top 10 most viewed videos and their respective channels:')
    query3 = "SELECT v.video_name, c.channel_name, COUNT(v.video_id) AS video_count FROM video_new v INNER JOIN channel_new c ON v.channel_id = c.channel_id GROUP BY v.video_name, c.channel_name ORDER BY COUNT(v.video_id) DESC LIMIT 10"
    df3 = pd.read_sql_query(query3, engine)
    display_expander_dataframe(df3)
    # 4. How many comments were made on each video, and what are their corresponding video names?
    st.header('4. Number of comments on each video:')
    query4 = "SELECT v.video_name, COUNT(*) AS num_comments FROM comment_new co INNER JOIN video_new v ON co.video_id = v.video_id GROUP BY v.video_name"
    df4 = pd.read_sql_query(query4, engine)
    display_expander_dataframe(df4)
    # 5. Which videos have the highest number of likes, and what are their corresponding channel names?
    st.header('5. Videos with the highest number of likes and their corresponding channel names:')
    query5 = "SELECT v.video_name, c.channel_name, v.like_count FROM video_new v INNER JOIN channel_new c ON v.channel_id = c.channel_id ORDER BY v.like_count DESC LIMIT 5"
    df5 = pd.read_sql_query(query5, engine)
    display_expander_dataframe(df5)
     # 6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?
    st.header('6. Total number of likes and dislikes for each video:')
    query6 = "SELECT v.video_name, SUM(v.like_count) AS total_likes, SUM(v.dislike_count) AS total_dislikes FROM video_new v GROUP BY v.video_name"
    df6 = pd.read_sql_query(query6, engine)
    display_expander_dataframe(df6)
    # 7. What is the total number of views for each channel, and what are their corresponding channel names?
    st.header('7. Total number of views for each channel:')
    query7 = "SELECT c.channel_name, SUM(v.view_count) AS total_views FROM channel_new c LEFT JOIN video_new v ON c.channel_id = v.channel_id GROUP BY c.channel_name"
    df7 = pd.read_sql_query(query7, engine)
    display_expander_dataframe(df7)
    # 8. What are the names of all the channels that have published videos in the year 2022?
    st.header('8. Channels that published videos in the year 2022:')
    query8 = "SELECT DISTINCT c.channel_name ,v.published_at FROM video_new v INNER JOIN channel_new c ON v.channel_id = c.channel_id WHERE EXTRACT(YEAR FROM v.published_at) = '2022' "
    df8 = pd.read_sql_query(query8, engine)
    display_expander_dataframe(df8)
    # 9. What is the average duration of all videos in each channel, and what are their corresponding channel names?
    st.header('9. Average duration of all videos in each channel, and what are their corresponding channel names:')
    query9 = "SELECT c.channel_name, v.duration AS avg_duration FROM video_new v INNER JOIN channel_new c ON v.channel_id = c.channel_id GROUP BY c.channel_name, v.duration"
    df9 = pd.read_sql_query(query9, engine)
    display_expander_dataframe(df9)
     # 10. Which videos have the highest number of comments, and what are their corresponding channel names?
    st.header('10. Videos with the highest number of comments, and their corresponding channel names:')
    query10 = "SELECT v.video_name, c.channel_name, v.comment_count FROM video_new v INNER JOIN channel_new c ON v.channel_id= c.channel_id ORDER BY v.comment_count DESC LIMIT 5"
    df10 = pd.read_sql_query(query10, engine)
    display_expander_dataframe(df10)
if __name__ == "__main__":
    main()
    
    

    
    
    
    
    
    
   
    
    
    
    
    
    
    
    
    
    
    
    
    
   
    
   
    
   


