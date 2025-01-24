import json

import pandas as pd
from geopy import Nominatim

# Default image URL for cases where no image is available
NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


#  Fetching the HTML content of the Wikipedia page
def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")

# Extracting table rows from the provided HTML content using BeautifulSoup.
def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table")
    table_rows = table[2].find_all('tr')
    
    return table_rows


#  Cleaning and formating the given text by removing unwanted characters
def clean_text(text):
    text = str(text).strip()
    text = text.replace("&nbsp", '')
    if text.find(' ♦'):
      text = text.replace(' ♦', '')
    if text.find('[') != -1:
      text = text.replace('[', '')
    if text.find(' (formerly)') != -1:
      text = text.split(' (formerly)')[0]

    return text.replace('\n', '')

# Extracting stadium data from the specified Wikipedia page and 
# saving it to a CSV file.
def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)


    json_rows = json.dumps(data)
    data = json.loads(json_rows)
    stadium_df = pd.DataFrame(data)

    directory = 'data'

     # Create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    
    stadium_df.to_csv(os.path.join(directory, 'stadiums.csv'), index=False)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
 
    return stadium_df
    # return "OK"

# Retrieves the latitude and longitude of a city in a country using the OpenCage Geocoding API.
def get_lat_long(country, city):
    import requests
    # Replace 'YOUR_API_KEY' with your actual OpenCage API key
    api_key = '78f4190bf13b4495a7abf1e940b5666e'
    url = f'https://api.opencagedata.com/geocode/v1/json?q={city},{country}&key={api_key}'
    # 'https://api.opencagedata.com/geocode/v1/json?q=52.3877830%2C9.7334394&key={api_key}'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses

        data = response.json()  # Parse the JSON response

        # Check if results are found
        if data['results']:
            # Get the first result's geometry
            latitude = data['results'][0]['geometry']['lat']
            longitude = data['results'][0]['geometry']['lng']
            return latitude, longitude
        else:
            return None

    except Exception as e:
        print(f"Error: {e}")


# Transforms the extracted Wikipedia data by adding location information
    # and cleaning the capacity data.
def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    # Remove unwanted characters (e.g., brackets, spaces) and convert to numeric
    stadiums_df['capacity'] = stadiums_df['capacity'].str.replace(']', '', regex=False)  # Remove closing brackets
    stadiums_df['capacity'] = stadiums_df['capacity'].str.replace('[', '', regex=False)  # Remove opening brackets
    stadiums_df['capacity'] = stadiums_df['capacity'].str.strip()  # Remove leading/trailing spaces
    stadiums_df['capacity'] = pd.to_numeric(stadiums_df['capacity'], errors='coerce')
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"

    # Writes the transformed stadium data to a CSV file and saves it to Azure Blob Storage.
def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    data.to_csv('data/' + file_name, index=False)
    # DpWln5buRUp3aaI/qGUgiGkIgODIzNNpVSS8zUVHQM74MMERAkasb1fPB6fLxlY1e0Xbk8Z+rUWl+AStLAIofw==

    # Save the file to Azure Blob Storage
    data.to_csv('abfs://footballeng@footballeng.dfs.core.windows.net/data/' + file_name,
        storage_options={
            'account_key': 'DpWln5buRUp3aaI/qGUgiGkIgODIzNNpVSS8zUVHQM74MMERAkasb1fPB6fLxlY1e0Xbk8Z+rUWl+AStLAIofw=='
        }, index=False
    )

    # Loads the transformed stadium data into a PostgreSQL database.
def load_data_to_postgres(ti):
    data = ti.xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    
    df = json.loads(data)
    df = pd.DataFrame(df)
    df.columns = df.columns.str.strip()


    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    conn = pg_hook.get_conn()

    try:
        cursor = conn.cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS WikipediaStadiums; ')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS WikipediaStadiums.stadiums(
                rank int,
                stadium text,
                capacity int,
                region varchar(50),
                country varchar(50),
                city varchar(50),
                images text,
                home_team varchar(200),
                location text[]
            );
        ''')

        for index, row in df.iterrows():
            cursor.execute('''
                INSERT INTO WikipediaStadiums.stadiums (rank, stadium, capacity, region, country, city, images, home_team, location)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            ''', (row['rank'], row['stadium'], row['capacity'], row['region'], row['country'], row['city'], row['images'], row['home_team'], row['location']))
        
        
        conn.commit()
        logging.info("Data loaded to Postgres Successfully...")
    except Exception as e:
        logging.error("An error occured: %s", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
