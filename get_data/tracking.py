import sqlite3
import pandas as pd
import dash_table


style_cell_cond_main = [
    {'if': {'column_id': 'Tag'}, 'textAlign': 'left',
     'width': '300px', 'maxWidth': '300px', 'minWidth': '300px'
     },
    {'if': {'column_id': 'Channel'}, 'textAlign': 'left',
     'width': '50px', 'maxWidth': '300px', 'minWidth': '300px'
     },
    {'if': {'column_id': 'Video'}, 'textAlign': 'left',
     'width': '50px', 'maxWidth': '300px', 'minWidth': '300px'
     },
    {'if': {'column_id': 'Topic'}, 'textAlign': 'left',
     'width': '50px', 'maxWidth': '300px', 'minWidth': '300px'
     },
    {'if': {'column_id': 'Category'}, 'textAlign': 'left',
     'width': '50px', 'maxWidth': '300px', 'minWidth': '300px'
     },
    {'if': {'column_id': 'Views'},
     'width': '50px', 'maxWidth': '50px', 'minWidth': '50px'
     },
]

generic_table_settings = dict(
    merge_duplicate_headers=True,
    css=[{'selector': '.dash-cell div.dash-cell-value',
          'rule': 'display: inline; white-space: inherit;'
                  'overflow: inherit; text-overflow: inherit;'}],
    style_cell={
        'whiteSpace': 'no-wrap',
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 0,
    })

top_watched_queries = {
    'Tags': '''SELECT t.tag AS Tag, vtts.watched_at AS Timestamp FROM tags t 
    JOIN videos_tags vt ON t.id = vt.tag_id 
    JOIN videos_timestamps vtts ON vt.video_id = vtts.video_id''',

    'Channels': '''SELECT c.title AS Channel, count(v.id) AS Views FROM 
    videos_timestamps vt JOIN videos v ON vt.video_id = v.id
    JOIN channels c ON v.channel_id = c.id 
    WHERE NOT v.id = 'unknown' and NOT c.id = 'unknown'
    GROUP BY Channel
    ORDER BY Views DESC
    LIMIT ?;''',

    'Videos':  '''SELECT v.title AS Video, count(v.title) AS Views, v.id as Id
    FROM videos_timestamps vt JOIN videos v ON v.id = vt.video_id 
    WHERE NOT v.title = 'unknown'
    GROUP BY v.id
    ORDER BY Views DESC
    LIMIT ?; 
        ''',

    'Topics': '''SELECT t.topic AS Topic, count(Topic) AS Views FROM topics t 
    JOIN videos_topics vt ON t.id = vt.topic_id
    JOIN videos_timestamps vtts ON vt.video_id = vtts.video_id
    GROUP BY Topic
    ORDER BY Views DESC
    LIMIT ?;''',
    
    'Categories': '''SELECT c.title AS Category, count(c.title) AS Views
    FROM categories c JOIN videos v on c.id = v.category_id
    JOIN videos_timestamps vtts ON v.id = vtts.video_id
    GROUP BY Category
    ORDER BY Views DESC
    LIMIT ?;'''
}


class DataKeeper:
    pass


data_keeper = DataKeeper()


def get_top_results(conn: sqlite3.Connection,
                    query_type: str,
                    number_of_records: int = 200):
    conn.create_function('py_lower', 1, str.lower)
    if getattr(data_keeper, query_type.lower(), None) is None:
        if query_type == 'Tags':
            params = tuple()
        else:
            params = (number_of_records,)
        df = pd.read_sql(
            top_watched_queries[query_type], conn, params=params)
        if query_type == 'Tags':
            df.Timestamp = pd.to_datetime(df.Timestamp)
            df.Tag = df.Tag.str.lower()
            df.drop_duplicates(inplace=True)
            # important the below setattrs is exactly here
            all_df: pd.DataFrame = df.copy(deep=True)
            all_df.set_index(all_df.Timestamp, inplace=True)
            all_df.drop('Timestamp', axis=1, inplace=True)
            setattr(data_keeper, 'all_tags_ungrouped', all_df)
            df.drop('Timestamp', axis=1, inplace=True)
            df = df.groupby(by='Tag').size().reset_index(name='Views')
            df.sort_values(by='Views', inplace=True, ascending=False)
            df = df[df.Views > 50]
        setattr(data_keeper, query_type.lower(), df)
    else:
        df = getattr(data_keeper, query_type.lower())

    cols = [{'name': n, 'id': n} for n in df.columns]
    if query_type == 'Videos':
        cols[2]['hidden'] = True
    table = dash_table.DataTable(
        id='top-watched-table',
        data=df.to_dict('rows'),
        columns=cols,
        row_selectable='multi',
        sorting=True,
        sorting_type="multi",
        style_as_list_view=True,
        style_table={'maxHeight': '377', 'minWidth': '500', 'maxWidth': '500',
                     'margin': 5},
        n_fixed_rows=1,
        selected_rows=[0, 1, 2],
        style_cell_conditional=style_cell_cond_main,
        **generic_table_settings
    )

    return table


history_charts_queries = {
    'Channels': '''SELECT vt.watched_at AS Timestamp, c.title as Channel FROM 
    channels c JOIN videos v ON c.id = v.channel_id
    JOIN videos_timestamps vt ON v.id = vt.video_id 
    WHERE c.title IN ?
    ORDER BY c.title DESC;''',

    'Videos': '''SELECT v.id as Id, vt.watched_at AS Timestamp,
    v.title AS Video FROM 
videos v JOIN videos_timestamps vt ON v.id = vt.video_id 
WHERE v.id IN ?; ''',

    'Topics': '''SELECT vtts.watched_at AS Timestamp, t.topic AS Topic FROM 
    topics t JOIN videos_topics vt ON t.id = vt.topic_id
    JOIN videos_timestamps vtts ON vt.video_id = vtts.video_id 
    WHERE t.topic IN ?
    ORDER BY t.topic DESC;''',

    'Categories': '''SELECT vt.watched_at AS Timestamp, c.title AS Category FROM 
    categories c JOIN videos v ON c.id = v.category_id
    JOIN videos_timestamps vt ON v.id = vt.video_id 
    WHERE c.title IN ?
    ORDER BY c.title DESC;'''
}


def selected_history_charts_mass(conn: sqlite3.Connection,
                                 entries: list,
                                 query_type: str) -> pd.DataFrame:
    in_part = '(' + ('?, ' * len(entries)).strip(' ,') + ')'
    if query_type == 'Tags':
        df = getattr(data_keeper, 'all_tags_ungrouped')
        df = df.loc[df.Tag.isin(entries), :]
    else:
        query = history_charts_queries[query_type]
        query = query.replace('?', in_part)
        df = pd.read_sql(query, conn,
                         params=tuple(entries),
                         index_col='Timestamp', parse_dates='Timestamp')
    return df
