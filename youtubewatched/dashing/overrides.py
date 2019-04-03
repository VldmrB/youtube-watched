import json
import plotly
from flask import Response, url_for
from plotly.utils import iso_to_plotly_time_string, NotEncodable
from dash import Dash
from dash_html_components import Div, A
from youtubewatched.utils.sql import db_has_records


class Dashing(Dash):
    """
    Makes the layout database-presence aware. Done this way since an active
    request is needed for db_has_records to work (relies on a cookie)
    """
    def serve_layout(self):
        if db_has_records():
            layout = self.layout
        else:
            layout = Div(['No data found. Make sure to first import your ',
                          A('Takeout', href=url_for(
                              'records.index'))])

        return Response(json.dumps(layout, cls=plotly.utils.PlotlyJSONEncoder),
                        mimetype='application/json')


# remove lines 294-306 in the below function (in plotly.utils.py) to stop
# Plotly from implicitly converting datetime objects to the UTC timezone in most
# cases, even those without a timezone
# https://github.com/plotly/plotly.py/issues/209
def encode_as_datetime(obj):
    """Attempt to convert to iso time string using datetime methods."""

    # now we need to get a nicely formatted time string
    try:
        time_string = obj.isoformat()
    except AttributeError:
        raise NotEncodable
    else:
        return iso_to_plotly_time_string(time_string)


encode_as_datetime = staticmethod(encode_as_datetime)
plotly.utils.PlotlyJSONEncoder.encode_as_datetime = encode_as_datetime
