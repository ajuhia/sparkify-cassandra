#!/usr/bin/env python
# coding: utf-8

# # Part I: ETL Pipeline for Pre-Processing the Files

# ####  RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE INPUT FILES

# #### Import Python packages 

# In[21]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[22]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    print(file_path_list)


# ### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[23]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[24]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II: Database modelling
# 
# #### Connect to Apache Cassandra, create tables, populate them as per the query requirement 
# 
# #### Input data has been loaded to a  CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# 
# ##  Part II: Step 1: 
# ## After collecting the dataset in csv file, we will do the following:
# -  Create a connection to Cassandra instance
# -  Create a keyspace
# -  Set up keyspace

# #### Creating a Cluster

# In[25]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[26]:


# Created a Keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[27]:


#Set KEYSPACE
try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)


# ## Part II: Step 2:
# ## Now we need to create tables and populate them with data to run the following queries.
# With Apache Cassandra you model the database tables on the queries you want to run.

# ### The following three questions of the data are to be answered, and tables are to be created accordingly.
# 
# -   Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# - Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# -   Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# ## Query 1: Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4.
# 
# ### Solution Approach:
# For this query:
# - a table <b>sparkify_session_history</b> has been created below.
# - Along with attributes artists, song and length, this table also contains attributes session_id and item_in_session. 
# - Column session_id is marked as its partition key for filtering the rows and item_in_session has been used as clustering column so that each row can be uniquely identified.

# In[28]:


# STEP 1: Create the table
query = "CREATE TABLE IF NOT EXISTS sparkify_session_history"
query = query + "(session_id int, item_in_session int,artist text, song text, length float,  PRIMARY KEY (session_id,item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)
                    


# In[29]:


# STEP 2:Add insert statement
# Read the dataset file and create entries in sparkify_session_history table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ##Assign the INSERT statements into the `query` variable
        query = "INSERT INTO sparkify_session_history(session_id,item_in_session,artist,song,length)"
        query = query + "VALUES (%s, %s, %s, %s,%s)"
        session.execute(query, (int(line[8]),int(line[3]),line[0], line[9],float(line[5])))


# In[30]:


#STEP 3: Run the query
query = "select artist, song, length from sparkify_session_history WHERE session_id = 338 and item_in_session = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist+'\t', row.song+'\t',row.length)


# ## Query2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182.
# 
# ### Solution Approach:
# For this query:
# - a table <b>sparkify_songplays_history</b> has been created below.
# - Along with attributes artists, song, user first_name and  user last_name, this table also contains attributes user_id, session_id and item_in_session. 
# - Column (user_id,session_id) is marked as its composite partition key for filtering the rows and item_in_session has been used as clustering column so that each row can be uniquely identified.

# In[31]:


# STEP 1: Create the table
query = "CREATE TABLE IF NOT EXISTS sparkify_songplays_history"
query = query + "(user_id int, session_id int, item_in_session int,artist text, song text, first_name text, last_name text,  PRIMARY KEY ((user_id,session_id),item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)
                 
                    


# In[32]:


# STEP 2:Add insert statement
# Read the dataset file and create entries in sparkify_songplays_history table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO sparkify_songplays_history(user_id,session_id,item_in_session,artist,song,first_name,last_name)"
        query = query + "VALUES (%s, %s, %s,%s, %s,%s,%s)"
        session.execute(query, (int(line[10]),int(line[8]),int(line[3]),line[0], line[9], line[1],line[4]))


# In[33]:


#STEP 3: Run the query
query = "select artist,song,first_name,last_name from sparkify_songplays_history WHERE user_id = 10 and session_id = 182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist+'\t', row.song+'\t', row.first_name+'\t', row.last_name)


# ## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'.
# 
# ### Solution Approach:
# For this query:
# - a table <b>sparkify_user_history</b> has been created below.
# - Along with requested attributes i.e. user's first_name and last_name, this table also contains attributes song and user_id.
# - Column (song) is marked as the partition key for filtering the rows and user_id has been used as clustering column so that each row can be uniquely identified.

# In[34]:


# 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'   
# STEP 1: Create the table
query = "CREATE TABLE IF NOT EXISTS sparkify_user_history"
query = query + "(song text, user_id int, user_first_name text,user_last_name text,PRIMARY KEY (song,user_id))"
try:
    session.execute(query)
except Exception as e:
    print(e)
              


# In[35]:


# STEP 2:Add insert statement
# Read the dataset file and create entries in sparkify_user_history table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        #print(line)

        ##Assign the INSERT statements into the `query` variable
        query = "INSERT INTO sparkify_user_history(song,user_id,user_first_name,user_last_name)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9],int(line[10]),line[1], line[4],))


# In[36]:


#STEP 3: Run the query
## Added in the SELECT statement to return data for query 3
query = "select user_first_name,user_last_name from sparkify_user_history WHERE song='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.user_first_name, row.user_last_name)


# ### Drop the tables before closing out the sessions

# In[37]:


query = "drop table sparkify_session_history"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table sparkify_songplays_history"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table sparkify_user_history"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[38]:


session.shutdown()
cluster.shutdown()

