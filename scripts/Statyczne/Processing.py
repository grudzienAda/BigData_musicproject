#!/usr/bin/env python
# coding: utf-8

# In[112]:


import findspark
findspark.init()


# In[113]:


from pyspark.sql.functions import explode
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from datetime import date
from pyspark.sql.functions import lit, to_date, from_unixtime, unix_timestamp
from datetime import datetime
from pyspark.sql.functions import col, sha2, concat
from pyspark.sql import SparkSession
from os.path import abspath
from pyspark import SparkContext, SparkConf


# In[114]:


master= "local[2]"
spark = SparkSession.builder.master(master).appName("Spark").enableHiveSupport().getOrCreate()


# In[115]:


df = spark.read.parquet("/user/patryk/billboard.parquet")


# In[116]:


df = df.select("Artists","Name","Weekly_rank","Peak_position","Weeks_on_chart","Week", "Date")


# In[117]:


newColumn = ["Artists", "Name", "Rank", "Peak", "Weeks", "Week", "Created"]
df = df.toDF(*newColumn)


# In[118]:


df.write.mode('overwrite').saveAsTable("hot100")
