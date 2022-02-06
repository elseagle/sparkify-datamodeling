import glob
import os
import sys
import time

import pandas as pd
import psycopg2

from create_tables import create_tables
from sql_queries import *

sys.getdefaultencoding()


def process_song_file(cur, conn, filepath):
    """Processes each song file in the folder and
         insert into corresponding data into songs 
         and artists tables respectively.

        Parameters:
            cur: cursor from db connection
            conn: db connection
            filepath (str): directory/path to song files 

        Returns:
    """

    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title",
                    "artist_id", "year", "duration"]].values
    song_data_ = tuple([list(song) for song in song_data])
    for data in song_data_:
        cur.execute(song_table_insert, data)
        print("Succesful song update")
    # insert artist record
    artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ].values
    artist_data_ = tuple([list(artist) for artist in artist_data])
    for data in artist_data_:
        try:
            cur.execute(artist_table_insert, data)
            print("Succesful artist update")
        except Exception as e:
            print(e)


# conn.commit()


def process_log_file(cur, conn, filepath):
    """Processes each log file in the folder and
         insert into corresponding data into time,  
        users and songplay tables respectively.

        Parameters:
            cur: The cursor from db connection
            conn: The db connection
            filepath (str): directory/path to song files 
        Returns:
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    df["ts"] = t
    column_labels = ["time", "hour", "day",
                     "week_of_year", "month", "year", "weekday"]
    # insert time data records
    time_df = pd.DataFrame([], columns=column_labels)
    time_df["time"] = t
    time_df["hour"] = t.dt.hour

    time_df["day"] = t.dt.day
    time_df["week_of_year"] = t.dt.weekofyear
    time_df["month"] = t.dt.month
    time_df["year"] = t.dt.year
    time_df["weekday"] = t.dt.weekday
    print("Time DF: ", len(time_df))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()
        print("Time Upsert Succesful")

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    user_data_ = list([list(tt) for tt in user_df.values])
    for data in user_data_:
        cur.execute(user_table_insert, data)

        print("Succesful user update")

        # insert user records

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        print(index, ">>>", row.song, "------", row.artist)
        try:
            cur.execute(song_select, (row.song, row.artist))
            results = cur.fetchone()

        except Exception as e:
            print(e)
            results = None

        if results:
            print(results)
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)
        print("Succesful songplay update")


def process_data(cur, conn, filepath, func):
    """Processes functions that can process the song and log files.

        Parameters:
            cur: The cursor from db connection
            conn: The db connection
            filepath (str): directory/path to song files
            func: process function to be used
        Returns:
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, conn, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()
    create_tables(cur, conn)
    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)
    cur.execute(
        "select * from songplays WHERE song_id is not null and artist_id is not null")
    results = cur.fetchall()
    print()
    print()
    time.sleep(2)
    print("Result of `select * from songplays WHERE song_id is not null and artist_id is not null`:")
    print(results)

    conn.close()


if __name__ == "__main__":
    main()
