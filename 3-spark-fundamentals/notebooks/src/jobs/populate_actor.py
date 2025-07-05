from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, ArrayType


query = """
WITH
    last_year AS (
        SELECT
            *
        FROM
            actors
        WHERE
            -- get min year from actor_films
            year = 1969
    ),
    current_year AS (
        SELECT
            actor,
            actorid,
            COLLECT_LIST(STRUCT(film, votes, rating, filmid)) AS films,
            -- Determining the 'quality_class' based on the average film rating
            CASE
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class,
            year
        FROM
           actor_films
        WHERE
            year = 1970
            -- Grouping the results by actor, actor_id, and year for aggregation
        GROUP BY
            actor,
            actorid,
            year
    )
SELECT
    COALESCE(last_year.actor, current_year.actor) AS actor,
    COALESCE(last_year.actor_id, current_year.actorid) AS actor_id,
    CASE
        WHEN last_year.films IS NULL THEN current_year.films
        WHEN current_year.films IS NOT NULL THEN last_year.films || current_year.films
        ELSE last_year.films
    END AS films,
    COALESCE(current_year.quality_class, last_year.quality_class) AS quality_class,
    CAST(CASE WHEN current_year.actorid IS NOT NULL OR last_year.actor_id IS NOT NULL THEN TRUE ELSE FALSE END AS BOOLEAN) AS is_active,
    COALESCE(current_year.year, last_year.year + 1) AS year
FROM
    last_year
    FULL OUTER JOIN current_year ON current_year.actorid = last_year.actor_id
    AND last_year.year = current_year.year - 1
"""

def do_populate_actors(spark, dataframe1):
    dataframe1.createOrReplaceTempView('actor_films')
    #Actors table need to be empyty
    films = StructType([
    StructField("film", StringType(), True),
    StructField("votes", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("filmid", StringType(), True)
    ])
    schema = StructType([
    StructField("actor", StringType(), True),
    StructField("actor_id", StringType(), True),
    StructField("films", ArrayType(films), True),
    StructField("quality_class", StringType(), True),
    #is_active nullable = False (base on sql script)
    StructField("is_active", BooleanType(), False),
    StructField("year", IntegerType(), True)
    ])

    # Create an empty DataFrame with the schema
    empty_df = spark.createDataFrame([], schema)
    empty_df.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actor_films") \
        .getOrCreate()
    output_df = do_populate_actors(spark, spark.table("actor_films"))
    output_df.write.mode("overwrite").insertInto("actors")
