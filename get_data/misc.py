import sqlite3

import numpy as np
import pandas as pd


def top_tags_data(conn: sqlite3.Connection, amount: int):
    query = """SELECT t.tag AS Tag FROM
    videos_timestamps vt JOIN videos_tags vtgs ON vtgs.video_id = vt.video_id 
    JOIN tags t ON vtgs.tag_id = t.id
    WHERE NOT t.tag = 'the';"""
    results = pd.read_sql(query, conn)
    print(results.memory_usage(index=True, deep=True).sum() / (1024 * 2))
    results['Tag'] = results['Tag'].str.lower()
    results = results.assign(Count=np.ones(len(results.index)))
    results = results.groupby('Tag')
    results = results.agg(np.sum).sort_values(by='Count', ascending=False)
    print(results.memory_usage(index=True, deep=True).sum() / (1024 * 2))
    return results[:amount]


def top_watched_videos(conn: sqlite3.Connection, amount: int):
    query = """SELECT v.title AS Title, c.title AS Channel,
        count(v.title) AS Views FROM 
        videos_timestamps vt JOIN videos v ON v.id = vt.video_id 
        JOIN channels c ON v.channel_id = c.id
        WHERE NOT v.title = 'unknown'
        GROUP BY v.id
        ORDER BY Views DESC
        LIMIT ?; 
        """
    df = pd.read_sql(query, conn, params=(amount,))
    return df


def top_watched_channels(conn: sqlite3.Connection, amount: int):
    query = """SELECT c.title AS Channel,
        count(c.title) AS Views FROM 
        videos_timestamps vt JOIN videos v ON v.id = vt.video_id 
        JOIN channels c ON v.channel_id = c.id
        WHERE NOT v.title = 'unknown'
        GROUP BY c.id
        ORDER BY Views DESC
        LIMIT ?; 
        """
    df = pd.read_sql(query, conn, params=(amount,))
    return df
