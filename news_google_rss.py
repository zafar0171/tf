import pymysql
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import pandas as pd

def get_connection():
    try:
        connection = pymysql.connect(
        user="root",
        password="Thermodynamics@1",
        host="localhost",    
        database="stocks", 
        )
        
        print("Connection to MySQL database was successful")
        return connection
    except pymysql.MySQLError as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

# Function to fetch and parse the news
def fetch_news(rss_url):
    response = requests.get(rss_url)
    root = ET.fromstring(response.content)
    
    headlines = []
    urls = []
  
    for item in root.findall('./channel/item'):
        headline = item.find('title').text
        url = item.find('link').text
        headlines.append(headline)
        urls.append(url)
    
    return headlines, urls

rss_url = 'https://news.google.com/news/rss/headlines/section/topic/BUSINESS.IN'
headlines, urls = fetch_news(rss_url)

data = {'Headline': headlines, 'URL': urls}
df = pd.DataFrame(data)

print(df)
stocksTupleList = list(df.itertuples(index=False, name=None))
#Data insertion
mydb = get_connection()
if mydb:
    mycursor = mydb.cursor()

    query = """INSERT INTO goog_rss (Headline, url) VALUES (%s, %s)"""
    mycursor.executemany(query, stocksTupleList)
    mydb.commit()
    mycursor.close()
    mydb.close()
else:
    print("Failed to connect to MySQL database")
