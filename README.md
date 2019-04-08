## The goal of the app
is to gather some data about your YouTube watch history (available via Google Takeout) and do some light visualization
of it. There's a few built-in interactive graphs and tables, delivered via a web page, and then there's the 
data itself that can be used for more. An SQLite browser, such as [DB Browser for SQLite](https://sqlitebrowser.org/),
could be used for viewing and filtering it like a spreadsheet, as well as making simple graphs.  

Outside of requests to YouTube Data API, the whole thing is run locally.

This is *not* a tool for exhaustive data gathering/archiving and records keeping. Even if it tried to be, inaccuracies
in Takeout would not allow for [that](#takeout-quirks-and-data-accuracy).

## What you'll need

In addition to **Python 3.6+** and installing the package (preferably in a 
[virtual environment](https://docs.python.org/3/library/venv.html)):

```
pip install youtubewatched
```

you'll need two things:
 - Your [Google Takeout](https://takeout.google.com/settings/takeout) YouTube data
 - have YouTube Data API enabled and an **API key** for the app to make requests for information on 
each video. The first part from **Before you start** section from 
[Google's guide](https://developers.google.com/youtube/v3/getting-started) on the matter explains how to do that (should
 only be a few minutes):

> 1. You need a Google Account to access the Google API Console, request an API key, and register your application.
> 2. Create a project in the [Google Developers Console](https://console.developers.google.com/)
  and [obtain authorization credentials](https://developers.google.com/youtube/registering_an_application)
  so your application can submit API requests.
> 3. After creating your project, make sure the YouTube Data API is one of the services that your application is 
> registered to use:
>>  a. Go to the [API Console](https://console.developers.google.com/) and select the project that you just registered.  
>>  b. Visit the [Enabled APIs page](https://console.developers.google.com/apis/enabled). In the list of APIs, make
>>  sure the status is ON for the YouTube Data API v3.

\**the above block of text is a modification based on work created and shared by Google and used according to terms 
described in the Creative Commons 3.0 Attribution License.*\*

## Limitations

Only Takeout files in English can be parsed by the app, though adjusting code in convert_takeout.py (functions 
get_watch_history_files and get_all_records) could get it to work with another language.  
Alternatively, switching your [language](https://myaccount.google.com/language?utm_source=google-account&utm_medium=web)
to English should make the Takeout archives created afterwards be in English.  
It's also possible the format of watch-history.html files used to be different in the past, in which case
those would likely not be parsable either. If that happens, open an
[issue](https://github.com/VldmrB/youtube-watched/issues) and I'll see about making it work.

## Running the app

From your terminal, enter:
```
youtubewatched
```
That'll start up the app on `http://127.0.0.1:5000`.  
Enter `youtubewatched --help` for some limited server startup options.

The rest (there isn't much) is explained on the web page itself.

#### Browser compatibility

Chrome, Firefox, Opera, Brave and hopefully Safari should all work fine as long as not terribly outdated; Edge and IE
will not.

#### Possible issues
If videos' graphs for 1k+ records show up blank, **WebGL** in your browser is probably disabled or otherwise prevented 
from working.  
In Brave specifically, that could be fixed by clicking on the **Shields** icon in the address bar and 
allowing device recognition.

## Notes on how the app works

### Data retrieval and insertion process

Takeout's watch-history.html file(s) gets parsed for the available info. Some records will only contain a timestamp of 
when the video was opened, presumably when the video itself is no longer available. Most will also contain the video ID,
 title and the channel title.    

All the video IDs are then queried against YouTube Data API for additional information such as likes, tags, number of 
comments, etc. Combined with the timestamps from Takeout, the records are then inserted into a database, located in the 
project directory under the default name of yt.sqlite. Those without any identifying info are collectively inserted as a
 single 'unknown'.

Each successful query to the API uses 11 points, with the standard daily quota being 1M.
The Quotas tab on Google's [Console](https://console.developers.google.com/apis/api/youtube.googleapis.com/overview)
page will show how many have been used up.

Should the process get interrupted for any reason, it's safe to restart it using the same Takeout files; no duplicates 
will be created and no duplicate queries will be made (except 1 every time).

### Takeout quirks and data accuracy

Takeout works strangely (badly). The first three times I've used it, varying numbers of records were returned each time.
The second time returned fewer than the first and the last returned more than the first two, including records older 
than the ones in the first two as well as additional records throughout the whole watch history.
The next few I paid attention to also varied in the amount returned in a similar manner.

The oldest records were from April of 2014, with the Youtube account having been created and first used sometime in 2011.

In addition to that, all the archives were missing some amount of records for each year they did cover, when compared 
to the History page on Youtube, though the difference wasn't drastic. Curiously, the History page was also missing some 
videos that were present in Takeout.

#### Timestamps

In short, the timestamps can be very inaccurate and the app doesn't fix that. They shouldn't be relied on for anything
precise, but would work fine for a rough overview of activity over a given period of time, etc.

There is no timezone information coming from Takeout beyond abbreviations like EDT/PST/CET, some of which may refer to 
multiple different timezones. The timestamps seem to be returned in local time of what's used to browse YouTube 
(or perhaps use Google products in general), including those for videos that were watched in a different timezone.
However, temporarily changing the timezone on my PC, or in Google Calendar, or the region in Google Search Settings
never triggered a change in the timestamps.

One of the worse things happens with DST zones. In the case of zones observing Daylight Saving Time (DST), all of the
timestamps seem to be set to either the DST timezone or the non-DST one, depending on the date the archive was created.
That is, if someone who lives on the East coast of US were to create an archive in May, all the timestamps, including
ones that should be in EST (November - March) would be set to EDT, and vice versa if they were to create it in February.

#### Avoiding duplicate timestamps because of potential different timezones for different Takeout archives

Since different Takeout archives may have different timezones, depending on when/where they were downloaded, there may 
be duplicate timestamps in different timezones. To weed out them out, any timestamps for the same video ID that have
been watched at the same year, month, minute and second as well as less than 26 hours apart are treated as one. This may
 also block a limited amount of legitimate timestamps from being entered. Most if not all of them would be the ones
 attached to the 'unknown' record. Considering the records returned by Takeout are not complete as is, a few
 (7 unknown ones for me) extra lost timestamps seemed like a good trade-off.

## Built with signficant use of the following packages
 - [Flask](http://flask.pocoo.org/) - the app itself
 - [Dash](https://plot.ly/products/dash/) / Plotly - visualizing data and making interactive graphs, constructing the
  visualization web page
 - [Pandas](https://pandas.pydata.org/) & [NumPy](https://www.numpy.org/) - data wrangling
 - [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/) - parsing Google Takeout
