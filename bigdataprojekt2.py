# Import necessary packages
import glob
import os
from pyspark.sql.functions import explode
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from datetime import date
from pyspark.sql.functions import lit
from datetime import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

spark

#files = glob.glob('hdfs://localhost:9443/user/grudziena/nifi_out/projekt/*.parquet')
#list_of_files = [i for i in files if not i.endswith("tr.parquet")]
#print(files)
#print('--')
#print(list_of_files)
#latest_file = max(list_of_files, key=os.path.getctime)

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path("/user/grudziena/nifi_out/projekt/"))
partitions = [file.getPath().getName() for file in list_status]

latest_file = partitions[0]

#file_name = latest_file.rsplit('/', 1)[1]


df = spark.read.parquet("/user/grudziena/nifi_out/projekt/" + latest_file)



df2 = df.select(explode("tracks.items"))


df2 = df2.select("col.added_at","col.added_by","col.is_local","col.primary_color","col.track","col.video_thumbnail")




df3 = df2.select("track.album.name", "track.duration_ms", "track.episode", "track.popularity", 
           "track.type", "track.uri", "track.album.external_urls.spotify",
           "track.album.album_type", "track.album.release_date", 
           "track.album.total_tracks", "track.album.release_date_precision",
           "track.album.type", "track.artists.name", "track.artists.id",
           "track.artists.href", "track.artists.type", "track.artists.uri")



df4 = df3.withColumn(
    "ranking",
    row_number().over(Window.orderBy(monotonically_increasing_id()))
)



df4 = df4.withColumn("date", lit(str(date.today())))
df5 = df4.withColumn("time", lit(datetime.now().strftime("%H:%M:%S")))


newColumns = ["title","duration_ms","episode","popularity",
              "song_type","song_uri","spotify","album_type","release_date","total_tracks","release_date_precision",
              "type", "artists_name", "id", "href", "artists_types", "artists_uri", "ranking", "date", "time"]
df5 = df5.toDF(*newColumns)

from pyspark.sql.functions import col, sha2, concat

df5 = df5.withColumn("uid", sha2(concat(col("ranking"), col("date"), col("time")), 256))

df5_RankingData = df5.select("ranking", "date", "time", "title")
df5_SongDetails = df5.select("duration_ms","episode","popularity",
              "song_type","song_uri","spotify","album_type","release_date","total_tracks","release_date_precision",
              "type", "href")
df5_ArtistDetails = df5.select("artists_name", "artists_types", "artists_uri", "id")

df5_RankingData.write.parquet('/user/grudziena/nifi_out/projekt/'+latest_file[:-8]+"tr_RankingData"+latest_file[-8:]) 
df5_SongDetails.write.parquet('/user/grudziena/nifi_out/projekt/'+ latest_file[:-8]+"tr_SongDetails"+ latest_file[-8:]) 
df5_ArtistDetails.write.parquet('/user/grudziena/nifi_out/projekt/'+ latest_file[:-8]+"tr_ArtistDetails"+ latest_file[-8:])

