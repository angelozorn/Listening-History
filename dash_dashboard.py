import dash
from dash import dash_table
from dash import html, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
import mysql.connector
import pandas as pd

# Replace these values with your actual database credentials
db_config = {
    'host': "localhost",
    'user': "root",
    'password': "WearyBunny12916",
    'database': "spotify_listening_history"
}

# Establish a connection to the database
connection = mysql.connector.connect(**db_config)

try:
    if connection.is_connected():
        print("Connected to the MySQL database")

        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()

        # Query for the 'songs' table
        songs_query = """ SELECT DISTINCT
                            Artist,
                            songName,
                            DATE_FORMAT(playTime, '%m/%d/%Y') AS playDate,
                            COUNT(Duration) AS listenCount,
                            SUM(Duration) AS timeListened
                        FROM
                            songs_test
                        GROUP BY
                            Artist, songName, playDate;"""

        # Execute the 'songs' query
        cursor.execute(songs_query)

        # Fetch all the results for 'songs'
        songs_result = cursor.fetchall()

        # Get column names for 'songs'
        songs_columns = [desc[0] for desc in cursor.description]

        # Create a Pandas DataFrame for 'songs'
        songs_df = pd.DataFrame(songs_result, columns=songs_columns)

        # Group by "Artist" and "songName" and sum the columns for initialization
        initial_grouped_songs_df = songs_df.groupby(['Artist', 'songName']).agg({
            'listenCount': 'sum',
            'timeListened': 'sum'
        }).reset_index()

        # Sort by listenCount in descending order and take the top 10
        initial_top_songs_df = initial_grouped_songs_df.sort_values(by='listenCount', ascending=False).head(10)

        # Add a rank column
        initial_top_songs_df['Rank'] = initial_top_songs_df['listenCount'].rank(ascending=False, method='first')

        # Convert the dataframe to dictionary for DataTable
        initial_songs_data = initial_top_songs_df.copy()
        initial_songs_data['timeListened'] = (initial_songs_data['timeListened'] / 60000).round().astype(int)
        initial_songs_data = initial_songs_data.to_dict('records')

        # Query for the new analytics query
        analytics_query = """
            SELECT DISTINCT
                Artist,
                DATE_FORMAT(playTime, '%m/%d/%Y') AS playDate,
                COUNT(Duration) AS listenCount,
                SUM(Duration) AS timeListened
            FROM
                split_songs_test
            
            GROUP BY
                Artist, playDate;
        """

        # Execute the analytics query
        cursor.execute(analytics_query)

        # Fetch all the results for analytics
        analytics_result = cursor.fetchall()

        # Get column names for analytics
        analytics_columns = [desc[0] for desc in cursor.description]

        # Create a Pandas DataFrame for analytics
        analytics_df = pd.DataFrame(analytics_result, columns=analytics_columns)

        # Group by "Artist" and sum the columns for initialization
        initial_grouped_artists_df = analytics_df.groupby(['Artist']).agg({
            'listenCount': 'sum',
            'timeListened': 'sum'
        }).reset_index()

        # Sort by listenCount in descending order and take the top 10
        initial_top_artists_df = initial_grouped_artists_df.sort_values(by='listenCount', ascending=False).head(10)

        # Add a rank column
        initial_top_artists_df['Rank'] = initial_top_artists_df['listenCount'].rank(ascending=False, method='first')

        # Convert the dataframe to dictionary for DataTable
        initial_artists_data = initial_top_artists_df.copy()
        initial_artists_data['timeListened'] = (initial_artists_data['timeListened'] / 60000).round().astype(int)
        initial_artists_data = initial_artists_data.to_dict('records')


except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the cursor and connection in the finally block
    if 'cursor' in locals() and cursor is not None:
        cursor.close()

    if connection.is_connected():
        connection.close()
        print("Connection closed")

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
app.layout = dbc.Container(
    [
        dcc.DatePickerRange(
            id='date-range-picker',
            start_date=songs_df['playDate'].min(),
            end_date=songs_df['playDate'].max(),
            display_format='MM/DD/YYYY',
            style={
                'font-family': 'Arial, sans-serif',
                'margin-bottom': '20px',
                'background-color': 'black',
                'color': 'white !important'
                }
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.H3("Most Played Songs", style={'color': '#1DB954'}),
                    width=12,  # Full width for the header
                )
            ],
            style={'backgroundColor': 'black', 'margin-bottom': '10px'}  # Set the background color for the row
        ),
        dbc.Row(
            [
                dbc.Col(
                    dash_table.DataTable(
                        id='songs-table',
                        columns=[
                            {'name': 'Rank', 'id': 'Rank'},
                            {'name': 'Artist', 'id': 'Artist'},
                            {'name': 'Song', 'id': 'songName'},  # Update column header
                            {'name': 'Plays', 'id': 'listenCount'},  # Update column header
                            {'name': 'Time Listened', 'id': 'timeListened'},  # Update column header
                        ],
                        data=initial_songs_data,  # Initial data for the table
                        style_table={'backgroundColor': 'black'},
                        style_header={'backgroundColor': 'black', 'color': 'white'},
                        style_cell={
                            'backgroundColor': 'black',
                            'color': 'white',
                            'textAlign': 'center',  # Center text horizontally
                            'font-family': 'Arial, sans-serif'
                        },
                    ),
                    width=12,  # Full width for the table
                ),
            ],
            style={'backgroundColor': 'black', 'margin-bottom': '20px'}  # Set the background color for the row
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.H3("Most Played Artists", style={'color': '#1DB954'}),
                    width=12,  # Full width for the header
                )
            ],
            style={'backgroundColor': 'black', 'margin-bottom': '10px'}  # Set the background color for the row
        ),
        dbc.Row(
            [
                dbc.Col(
                    dash_table.DataTable(
                        id='artists-table',
                        columns=[
                            {'name': 'Rank', 'id': 'Rank'},
                            {'name': 'Artist', 'id': 'Artist'},
                            {'name': 'Plays', 'id': 'listenCount'},  # Update column header
                            {'name': 'Time Listened', 'id': 'timeListened'},  # Update column header
                        ],
                        data=initial_artists_data,  # Initial data for the table
                        style_table={'backgroundColor': 'black', 'margin-bottom': '200px'},
                        style_header={'backgroundColor': 'black', 'color': 'white'},
                        style_cell={
                            'backgroundColor': 'black',
                            'color': 'white',
                            'textAlign': 'center',  # Center text horizontally
                            'font-family': 'Arial, sans-serif'
                        },
                    ),
                    width=12,  # Full width for the table
                ),
            ],
            style={'backgroundColor': 'black', 'margin-bottom': '200px'}  # Set the background color for the row
        ),
    ],
    fluid=True,
    style={'backgroundColor': 'black', 'margin-bottom': '200px'},  # Set the background color of the container
)

# Define callback to update songs table based on date range
@app.callback(
    Output('songs-table', 'data'),
    [Input('date-range-picker', 'start_date'), Input('date-range-picker', 'end_date')],
    prevent_initial_call=True,
)
def update_songs_table(start_date, end_date):
    # Convert date strings to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convert playDate column to datetime objects in songs_df
    songs_df['playDate'] = pd.to_datetime(songs_df['playDate'])

    # Filter songs data based on date range
    filtered_songs_df = songs_df[
        (songs_df['playDate'] >= start_date) & (songs_df['playDate'] <= end_date)
    ]

    # Group by "Artist" and "songName" and sum the columns
    grouped_songs_df = filtered_songs_df.groupby(['Artist', 'songName']).agg({
        'listenCount': 'sum',
        'timeListened': 'sum'
    }).reset_index()

    # Sort by listenCount in descending order and take the top 10
    top_songs_df = grouped_songs_df.sort_values(by='listenCount', ascending=False).head(10)

    # Add a rank column
    top_songs_df['Rank'] = top_songs_df['listenCount'].rank(ascending=False, method='first')

    # Convert the timeListened column to minutes and round to the nearest whole number
    top_songs_df['timeListened'] = (top_songs_df['timeListened'] / 60000).round().astype(int)

    # Convert the dataframe to dictionary for DataTable
    top_songs_data = top_songs_df.to_dict('records')

    return top_songs_data

# Define callback to update artists table based on date range
@app.callback(
    Output('artists-table', 'data'),
    [Input('date-range-picker', 'start_date'), Input('date-range-picker', 'end_date')],
    prevent_initial_call=True,
)
def update_artists_table(start_date, end_date):
    # Convert date strings to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convert playDate column to datetime objects in analytics_df
    analytics_df['playDate'] = pd.to_datetime(analytics_df['playDate'])

    # Filter artists data based on date range
    filtered_artists_df = analytics_df[
        (analytics_df['playDate'] >= start_date) & (analytics_df['playDate'] <= end_date)
    ]

    # Group by "Artist" and sum the columns
    grouped_artists_df = filtered_artists_df.groupby(['Artist']).agg({
        'listenCount': 'sum',
        'timeListened': 'sum'
    }).reset_index()

    # Sort by listenCount in descending order and take the top 10
    top_artists_df = grouped_artists_df.sort_values(by='listenCount', ascending=False).head(10)

    # Add a rank column
    top_artists_df['Rank'] = top_artists_df['listenCount'].rank(ascending=False, method='first')

    # Convert the timeListened column to minutes and round to the nearest whole number
    top_artists_df['timeListened'] = (top_artists_df['timeListened'] / 60000).round().astype(int)

    # Convert the dataframe to dictionary for DataTable
    top_artists_data = top_artists_df.to_dict('records')

    return top_artists_data

if __name__ == '__main__':
    app.run_server(debug=True)
