"""This assignment is all about saving the results to a
 permanent, structured location: a MySQL (MariaDB) database."""

import sys
import configparser
sys.path.append('/opt/spark/python')
sys.path.append('/opt/spark/python/lib/py4j-0.10.9.7-src.zip')
from pyspark.sql.functions import col, count, concat, lit, when, sum
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
# the JDBC driver couldn't read .my.cnf directly, so had to use configparser to read a config file

def main():
    """Main function to process the dbNSFP data and save results to MariaDB."""
    # spark = SparkSession.builder \
    # .appName("assignment6_mahsa_zamanifard") \
    # .master("spark://spark.bin.bioinf.nl:7077") \
    # .config("spark.jars", "mariadb-java-client-3.5.0.jar") \
    # .getOrCreate()

    # Local Spark session
    spark = SparkSession.builder \
        .appName('assignment6_mahsa') \
        .master('local[2]') \
        .config('spark.executor.memory', '1g') \
        .config('spark.driver.memory', '1g') \
        .config("spark.jars", "mariadb-java-client-3.5.0.jar") \
        .getOrCreate()

    # to sample data:
    # zcat: uncompresses a gzipped (.gz) file and prints its contents to the terminal,
    # leaving the original compressed file untouched.
    # zcat path/dbNSFP4.9a.txt.gz | head -n 500 | gzip > path/to/copy/to/sample.txt.gz
    # file_path = "/homes/mzamanifard/Documents/prog5/Prog5/assignment6/sample.txt.gz"
    file_path = sys.argv[1]
    print(f"Processing file: {file_path}")
    df = spark.read.csv(
    file_path,
    sep='\t',            # Use tab as the separator
    header=True,         # Use the first row as the header
    inferSchema=True     # Let Spark guess the data types (good for a first look)
)

   # This will show the top 20 rows by default, often in a table format
    # df.show()

    # You can also see the column names and their data types with:
    # df.printSchema()

    # Q1: How many predictions each of the 43 classifiers makes.

    # the file is a database of "functional prediction of non-synonymous
    # single-nucleotide variants (nsSNVs)".
    # what that means:
    # Each row in the df is a single, tiny mutation (an SNV) in the human genome.
    # The columns (classifiers) are different scientific
    # tools (like "SIFT_pred", "PolyPhen2_pred", etc.)
    # that try to predict if that mutation is harmless or if it might cause a disease.
    # So, when the question asks, "How many predictions each of the 43 classifiers makes,"
    # it's asking:
    # Identify the 43 columns that represent these "classifiers."
    # For each of those columns, count how many rows are not empty
    # (i.e., how many rows are not null).
    # Not every tool makes a prediction for every mutation, so many cells will be empty.
    # We just need to count the ones that arent.

    # This will be a Python list of column names
    all_columns = df.columns

    # Filter the list to find names containing '_pred'
    classifier_columns = [col_name for col_name in all_columns if '_pred' in col_name]

    # Let's print them to see what we found
    print("Found classifier columns:")
    print(len(classifier_columns))

    # Now, for each classifier column, we want to count non-null predictions
    # The row labeled "count" is the answer to the first question
    df = df.na.replace('.', None)
    df = df.na.replace('.;', None)
    df = df.na.replace('./.', None)  # Replace nonsense strings with None for accurate null counting
    # df.select(classifier_columns).describe().show()

    # we use * to unpack the list into individual arguments
    # this gives a wide table with one column per classifier and only one row with counts
    spark_row_list = df.select(*[count(col_name) for col_name in classifier_columns]).collect()
    # it's easier to sort it in Python so we collect the result to the driver
    spark_row = spark_row_list[0]
    counts_dict = {}
    for col_name in classifier_columns:
        counts_dict[col_name.replace('_pred', '')] = spark_row[f'count({col_name})']

    print(counts_dict)

    # Now we have a dictionary iwth classifier names as keys and counts as values

    # Q2: Make a top five of classifiers based on the total prediction it makes,
    # and drop all the columns associated with the others.
    # Let's sort it by counts (values)
    sorted_counts = dict(sorted(counts_dict.items(), key=lambda item: item[1], reverse=True))
    print("Classifier counts (sorted):")
    top_5 = []
    i = 0
    for classifier, count_value in sorted_counts.items():
        if i < 5:
            print(f"{classifier}: {count_value}")
            top_5.append(classifier)
        else:
            break
        i += 1
    to_drop = [col_name for col_name,_ in counts_dict.items() if col_name not in top_5]
    to_keep = [col for col in df.columns if col.split('_')[0] not in to_drop]
    df_reduced = df.select(*to_keep)
    # print("Reduced DataFrame schema:")
    # df_reduced.printSchema()

    # Q3: "Merge the chr and pos columns to yield a unique identifier
    # for each position in the genome."
    df_with_id = df_reduced.withColumn(
    "genome_id",             # 1. The name for the new column
    concat(                  # 2. How to build it:
        col("#chr"),          #    Take the '#chr' column
        lit(":"),            #    Add a literal ":" string
        col("pos(1-based)").cast(StringType())
        # Add the 'pos' column, which we cast to string, it won't concatenate otherwise
        # but it won't throw the error until Q4 when we actually use it. REMEMBER: SPARK IS LAZY!
    )
)
    # print("Final DataFrame schema with genome_id:")
    # df_with_id.printSchema()

    #Q4: What position has the most predictions associated with it?
    # 1. Create the expression to count predictions *per row*
    prediction_count_expr = lit(0)
    for classifier_name in top_5:
        classifier_name_complete = classifier_name + '_pred'
        prediction_count_expr = prediction_count_expr + when(
            col(classifier_name_complete).isNotNull(), 1
        ).otherwise(0)

    # 2. Add this as the 'row_pred_count' column
    df_with_counts = df_with_id.withColumn("row_pred_count", prediction_count_expr)
    # This will show all the unique values in that column
    # df_with_counts.select("genome_id", "row_pred_count").show()
    # 3. Group by 'genome_id' and sum the 'row_pred_count' to get total predictions per position
    # and order by that count descending
    top_position_df = df_with_counts.groupBy("genome_id").agg(
        sum("row_pred_count").alias("total_predictions")).orderBy(
        col("total_predictions").desc()).limit(1)

    print("Position with the most predictions:")
    top_position_df.show()

    # Q5: What protein (Ensembl_proteinid) has the most predictions associated with it?
    top_protein_df = df_with_counts.groupBy("Ensembl_proteinid").agg(
        sum("row_pred_count").alias("total_predictions")).orderBy(
        col("total_predictions").desc()).limit(1)
    print("Protein with the most predictions:")
    top_protein_df.show()

    # Finally: Save the resulting DataFrame to a MariaDB database.
    print("Saving results to MariaDB database...")

    # --- Read credentials from config file ---
    config = configparser.ConfigParser()
    config.read('config.ini') # Read the config file

    db_user = config['database']['user']
    db_pass = config['database']['password']

    # --- Use the variables in your .write() options ---
    df_with_id.write \
      .format("jdbc") \
      .option("driver","org.mariadb.jdbc.Driver") \
      .option("url", "jdbc:mariadb://mariadb.bin.bioinf.nl/Mzamanifard") \
      .option("dbtable", "dbnsfp_top5_results") \
      .option("user", db_user) \
      .option("password", db_pass) \
      .mode("overwrite") \
      .save()

    print("Successfully saved data to database.")
    # Don't forget to stop the Spark session
    print("Stopping Spark session...")
    spark.stop()

if __name__ == "__main__":
    main()
