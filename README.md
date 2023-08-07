# youtube-data-harvesting

# Introduction:
The project name is youtube data harvesting that allows users to retrieve and analyze data from YouTube channels through API key. It utilizes the YouTube Data API to fetch information such as channel statistics, video details, comments, and more.

# Installation:
pip install google-api-python-client,
pip install pymongo,
pip install mysql-connector-python,
pip install streamlit,
pip install pandas.

# Technologies Used:
Python: The project is implemented using the Python programming language.
YouTube Data API: Utilizes the official YouTube Data API to interact with YouTube's platform and retrieve data.
Streamlit: The user interface and visualization are created using the Streamlit framework, providing a seamless and interactive experience.
MongoDB: The collected data can be stored in a MongoDB database for efficient data management and querying.
PyMongo: A Python library that enables interaction with MongoDB, a NoSQL database. It is used for storing and retrieving data from MongoDB in the YouTube Data Scraper.
mysql: Retrieved unstructured datas being analyzed and turned into structured datas and load into mysql in table format.

# Usage:
User should enter the channel id to retrieve the channel details through API, after entered the channel id one button will appear if you click that the channel details will 
load into mongodb all these process will be done in page1. After these process select page2 in that page the user should click the button to fetch the datas from mongodb and load into mysql. After loading the datas into mysql select page3 in that page the user can view the channel details in table format. And finally in page4 the user can do the query execution.



 
