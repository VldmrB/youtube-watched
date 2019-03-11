import json
import plotly
from flask import Response
from plotly.utils import iso_to_plotly_time_string, NotEncodable
from dash import Dash
from dash_html_components import Div, A
from flask_utils import db_has_records


# Makes the layout database-presence aware. Done this way since an active
# request is needed for db_has_records to work (relies on a cookie)
class Dashing(Dash):

    def serve_layout(self):
        if db_has_records():
            layout = self.layout
        else:
            layout = Div(['No data found. Make sure to first import your ',
                          A('Takeout data', href='http://127.0.0.1:5000')])

        return Response(json.dumps(layout, cls=plotly.utils.PlotlyJSONEncoder),
                        mimetype='application/json')


# remove lines 294-306 (in plotly.utils.py) to stop Plotly from implicitly
# converting datetime objects to the UTC timezone in most cases, even those
# without a timezone
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
