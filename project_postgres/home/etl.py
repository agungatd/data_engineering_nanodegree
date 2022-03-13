import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function is responsible
    Arguments:
        cur: database cursor to execute the queries
        filepath: path of the data stored
    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function is responsible
    Arguments:
        cur: database cursor to execute the queries
        filepath: path of the data stored
    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df.loc[:,'timestamp'] = pd.to_datetime(df['ts'], unit='ms')
    t = df[['timestamp']]
    
    # insert time data records
    t.loc[:,'hour'] = t['timestamp'].dt.hour
    t.loc[:,'day'] = t['timestamp'].dt.day
    t.loc[:,'week'] = t['timestamp'].dt.week
    t.loc[:,'month'] = t['timestamp'].dt.month
    t.loc[:,'year'] = t['timestamp'].dt.year
    t.loc[:,'weekday'] = t['timestamp'].dt.weekday
    
    time_data = t.values.tolist()
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    time_df = pd.DataFrame(time_data, columns=column_labels)
    time_df.drop_duplicates(inplace=True)
    time_df.dropna(how='all', inplace=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.timestamp, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible
    Arguments:
        cur: database cursor to execute the queries
        conn: databse connection to open/close database or to commit the queries
        filepath: path of the data stored
        func: external function to process the datafile 
    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: 
        - This main function that will run the entire script and 
            will run for the very first time the script run.
        - conducts the ETL process to load song data into the database
        - conducts the ETL process to load log data into the database
    Arguments:
        None
    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
    