#!/bin/python/

import sys
import itertools
reload(sys)
sys.setdefaultencoding('utf-8')

def MakePair(MovieArtists):
        #The Argument passed as an array from a dataframe would be treated as a list in UDF
        #combination from itertools is a method which will give all combinations of the list passed to it of size n(second argument)
        returnval=itertools.combinations(MovieArtists,2)
        #Converting the returned combination as a list
        returnlist=[ list(t) for t in returnval ]
        return returnlist

from pyspark import SparkContext, SparkConf,SQLContext,sql
from pyspark.sql.functions import split,col,encode,regexp_replace,explode,upper,rank,desc,udf,size,when,trim,array
from pyspark.sql import Window,HiveContext
from pyspark.sql.types import IntegerType,ArrayType,StringType
sparkconfig=SparkConf().setAppName("khushal").setMaster("yarn-client")
sc=SparkContext(conf=sparkconfig)
#sqlContext=sql.SQLContext(sc)
hqlContext = sql.HiveContext(sc)

#sc.addPyFile("/root/Spark-CSV.zip")

#hqlContext.udf.register("Array_Size",Array_Size)
MakePair_udf=udf(MakePair,ArrayType(StringType()))

#Reading data from a File to Create a Dataframe
#Movies_orig=sqlContext.jsonFile("hdfs:///user/Data/movies.json)
Movies_Orig=hqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load('hdfs:///user/Data/Movies.csv')
print type(Movies_Orig)

#To print the Schema of a dataframe
print Movies_Orig.printSchema()

#To print the no .of lines/records in the DataFrame
#print Movies_Orig.count()

#To print a particluar column from a dataframe
print Movies_Orig.select("Actors").show(n=3)

#To print the distinct values in a particular column of a dataframe
print Movies_Orig.select("color").distinct().take(2)

#Remove the unwanted Columns from the df and form a new DF
Movies=Movies_Orig.drop("actor_1_name").drop("actor_2_name").drop("actor_3_name")

#Adding a new column by Splitting a single column into an array typed column based on a delimiter
Movies=Movies.withColumn("Genre", split("genres","\|")).drop("genres").withColumn("Artists", split("Actors","\|")).drop("Actors")

#using UDF and calling it on an array field and in turn getting an array.
#To get the list fo artists pair and the no.of movies they have acted together
Movies=Movies.withColumn("CoArtists",MakePair_udf(Movies["Artists"]))
print Movies.printSchema()
root
 |-- color: string (nullable = true)
 |-- director_name: string (nullable = true)
 |-- num_critic_for_reviews: integer (nullable = true)
 |-- duration: integer (nullable = true)
 |-- gross: integer (nullable = true)
 |-- movie_title: string (nullable = true)
 |-- num_voted_users: integer (nullable = true)
 |-- plot_keywords: string (nullable = true)
 |-- movie_imdb_link: string (nullable = true)
 |-- num_user_for_reviews: integer (nullable = true)
 |-- language: string (nullable = true)
 |-- country: string (nullable = true)
 |-- content_rating: string (nullable = true)
 |-- budget: long (nullable = true)
 |-- title_year: integer (nullable = true)
 |-- imdb_score: double (nullable = true)
 |-- aspect_ratio: double (nullable = true)
 |-- Genre: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- Artists: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- CoArtists: array (nullable = true)
 |    |-- element: string (containsNull = true)



print Movies.select("Artists","CoArtists").show(n=5,truncate=False)
+-------------------------------------------------+--------------------------------------------------------------------------------------------------------+
|Artists                                          |CoArtists                                                                                               |
+-------------------------------------------------+--------------------------------------------------------------------------------------------------------+
|[CCH Pounder, Joel David Moore, Wes Studi]       |[[CCH Pounder, Joel David Moore], [CCH Pounder, Wes Studi], [Joel David Moore, Wes Studi]]              |
|[Johnny Depp&Orlando Bloom&Jack Davenport]       |[]                                                                                                      |
|[Christoph Waltz, Rory Kinnear, Stephanie Sigman]|[[Christoph Waltz, Rory Kinnear], [Christoph Waltz, Stephanie Sigman], [Rory Kinnear, Stephanie Sigman]]|
|[Tom Hardy, Christian Bale, Joseph Gordon-Levitt]|[[Tom Hardy, Christian Bale], [Tom Hardy, Joseph Gordon-Levitt], [Christian Bale, Joseph Gordon-Levitt]]|
|[Doug Walker, Rob Walker, ]                      |[[Doug Walker, Rob Walker], [Doug Walker, ], [Rob Walker, ]]                                            |
+-------------------------------------------------+--------------------------------------------------------------------------------------------------------+
only showing top 5 rows


print Movies.select(explode("CoArtists").alias("costars")).groupBy("Costars").count().orderBy("count",ascending=False).show(n=2,truncate=False)
+-----------------------------+-----+
|Costars                      |count|
+-----------------------------+-----+
|[, ]                         |27   |
|[Steve Buscemi, Adam Sandler]|6    |
+-----------------------------+-----+
only showing top 2 rows


# To project/print selected columns and sort the resulting dataframe with one of these columns
#Sorting based on a column
print Movies.select("color","director_name","Genre","movie_title","imdb_score").orderBy("imdb_score",ascending=False).show(n=3)
+-----+--------------------+--------------+--------------------+----------+
|color|       director_name|         Genre|         movie_title|imdb_score|
+-----+--------------------+--------------+--------------------+----------+
|Color|      John Blanchard|      [Comedy]|Towering Inferno ...|       9.5|
|Color|      Frank Darabont|[Crime, Drama]|The Shawshank Red...|       9.3|
|Color|Francis Ford Coppola|[Crime, Drama]|      The Godfather |       9.2|
+-----+--------------------+--------------+--------------------+----------+
only showing top 3 rows


#To Get the movies done by a particular artist
#Artist names stored in a array form, so multiple artists are associated to a movie,artists:movie is  n:1, remove the nesting using explode and filter for the artists we need
Movies_Johnnydepp=Movies.select(explode("Artists").alias("Actor"),"movie_title").where(upper(col("Actor")) == 'JOHNNY DEPP')
Movies_MorganFreeman=Movies.select(explode("Artists").alias("Actor"),"Movie_title").where(upper(col("Actor")) == 'MORGAN FREEMAN')

#Combining 2 diffrent dataframes using UnionALL or adding rows to a dataframe
print Movies_Johnnydepp.unionAll(Movies_MorganFreeman).dropDuplicates().show(n=4)
+--------------+--------------------+
|         Actor|         movie_title|
+--------------+--------------------+
|   Johnny Depp|              Rango |
|Morgan Freeman|     Chain Reaction |
|Morgan Freeman|Million Dollar Baby |
|   Johnny Depp|            Platoon |
+--------------+--------------------+
only showing top 4 rows


#SQL case When equivalent in Spark
#Generating a new value based on the "imdb_score" using when function,Multiple conditions can be used as below, If Column.otherwise() is not invoked, None is returned for unmatched conditions
Movies=Movies.withColumn("verdict",when(Movies.imdb_score < 4,"FLOP" ).when((Movies.imdb_score < 7) & (Movies.imdb_score >4),"AVERAGE").when(Movies.imdb_score >7,"HIT").otherwise("UNKNOWN"))
print Movies.select("movie_title","imdb_score","verdict").show(n=3)
+--------------------+----------+-------+
|         movie_title|imdb_score|verdict|
+--------------------+----------+-------+
|             Avatar |       7.9|    HIT|
|Pirates of the Ca...|       7.1|    HIT|
|            Spectre |       6.8|AVERAGE|
+--------------------+----------+-------+
only showing top 3 rows


#sampling the data in a dataframe to get a subset based on a strata column
print Movies.sampleBy("verdict",fractions={"FLOP":0.1,"AVERAGE":0.1,"HIT":0.1},seed=101).groupBy("verdict").count().show(n=2)
+-------+-----+
|verdict|count|
+-------+-----+
|   FLOP|   18|
|    HIT|  151|
+-------+-----+


#Accessing individual array elements
#print Movies.select(Movies.Artists.getItem(0)).show(n=3)

#Defininig a window to call the windowing functions on top of it, By default the ordering is ascending, can be changed as below
#Window Functions are supported in only Hive Context 
ColorWindow=Window.partitionBy("color").orderBy(Movies["imdb_score"].desc())
print(type(ColorWindow))
<class 'pyspark.sql.window.WindowSpec'>


#Window Functions
Movies.registerTempTable("MoviesTable")
Colortoprated=hqlContext.sql("select color,movie_title,imdb_score,Artists,ROW_NUMBER() OVER(PARTITION BY color ORDER BY imdb_score DESC) as ranking  from  MoviesTable where color='Color'")
Colortoprated.show(n=4)
+-----+--------------------+----------+--------------------+-------+
|color|         movie_title|imdb_score|             Artists|ranking|
+-----+--------------------+----------+--------------------+-------+
|Color|Towering Inferno ...|       9.5|[Martin Short, An...|      1|
|Color|The Shawshank Red...|       9.3|[Morgan Freeman, ...|      2|
|Color|      The Godfather |       9.2|[Al Pacino, Marlo...|      3|
|Color|Dekalog             |       9.1|[Krystyna Janda, ...|      4|
+-----+--------------------+----------+--------------------+-------+
only showing top 4 rows


print Movies.select("color","movie_title","director_name","imdb_score",rank().over(ColorWindow).alias("rank")).filter("rank <4").show(n=10)
+----------------+--------------------+--------------------+----------+----+
|           color|         movie_title|       director_name|imdb_score|rank|
+----------------+--------------------+--------------------+----------+----+
| Black and White|   Schindler's List |    Steven Spielberg|       8.9|   1|
| Black and White|       12 Angry Men |        Sidney Lumet|       8.9|   1|
| Black and White|       Forrest Gump |     Robert Zemeckis|       8.8|   3|
|                |Kickboxer: Vengea...|      John Stockwell|       9.1|   1|
|                |Daredevil        ...|                    |       8.8|   2|
|                |10,000 B.C.      ...| Christopher Barnard|       7.2|   3|
|           Color|Towering Inferno ...|      John Blanchard|       9.5|   1|
|           Color|The Shawshank Red...|      Frank Darabont|       9.3|   2|
|           Color|      The Godfather |Francis Ford Coppola|       9.2|   3|
+----------------+--------------------+--------------------+----------+----+



#Writing Dataframe to a File, The Directory should not be present
#Movies.write.json("hdfs:///user/Data/Output")
#Movies.write.format('json').save("hdfs:///user/Data/MovieOutputJson")
#Movies.write.format('com.databricks.spark.csv').options(header='true').save("hdfs:///user/Data/MovieOutputCSV")


MovieArtists=Movies.select("movie_title","Artists").where(upper(trim(col("movie_title"))).like('THE%'))
print MovieArtists.show(n=3)
+--------------------+--------------------+
|         movie_title|             Artists|
+--------------------+--------------------+
|The Dark Knight R...|[Tom Hardy, Chris...|
|    The Lone Ranger |[Johnny Depp, Rut...|
|The Chronicles of...|[Peter Dinklage, ...|
+--------------------+--------------------+
only showing top 3 rows

#MovieArtists.rdd.foreach(MakePair)

#grouping Functions , GROUPBY
#only the grouped column and the computed column will be part of the resultant dataframe
print Movies.groupBy("Color").count().show(n=5)
+----------------+-----+
|           Color|count|
+----------------+-----+
| Black and White|  209|
|                |   19|
|           Color| 4815|
+----------------+-----+


print Movies.groupBy("Color").avg("imdb_Score").alias("avg").orderBy("avg(imdb_score)",ascending=False).show(n=5)
+----------------+-----------------+
|           Color|  avg(imdb_Score)|
+----------------+-----------------+
| Black and White|7.227272727272729|
|           Color|6.409532710280376|
|                |6.068421052631579|
+----------------+-----------------+

#End OF Code
print("BYE")
