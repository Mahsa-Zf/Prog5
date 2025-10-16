"""Assignment 5: PySpark DataFrame operations on GenBank files."""
import sys
from Bio import SeqIO
sys.path.append('/opt/spark/python')
sys.path.append('/opt/spark/python/lib/py4j-0.10.9.7-src.zip')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, min as spark_min, max as spark_max

def extract_info(record):
    """Extract Species and Protein info from a single GenBank record."""
    species = record.annotations.get("organism", "Unknown")
    accession = record.annotations.get("accessions", ["Unknown"])[0]
    version = record.annotations.get("sequence_version", None)
    accession_version = f"{accession}.{version}" if version else accession

    genome_size = len(record.seq)
    gene_count, coding_count = 0, 0
    for feature in record.features:
        if not any(x in str(feature.location) for x in (">", "<")):
            if feature.type == "gene":
                gene_count += 1
            elif feature.type in ["CDS", "Pro-peptides"]:
                coding_count += 1

    non_coding_count = gene_count - coding_count

    species_data = {
        "accession": accession_version,
        "name": species,
        "genome_size": genome_size,
        "num_genes": gene_count,
        "coding_genes": coding_count,
        "non_coding_genes": non_coding_count,
    }

    return species_data

def print_question_1(species_df):
    """Question 1: Average number of features per genome"""
    print("\n=== Question 1 ===")
    # .collect() brings data from executors to the driver
    # gets the first element of the first row which means it returns only a number
    avg_features = species_df.agg(avg('num_genes')).collect()[0][0] 
    print(f"Average number of features per Archaeal genome: {avg_features:.2f}")

def print_question_2(species_df):
    """Question 2: Ratio between coding and non-coding features"""
    print("\n=== Question 2 ===")
    totals = species_df.agg(
        spark_sum('coding_genes').alias('total_coding'),
        spark_sum('non_coding_genes').alias('total_noncoding')
    ).collect()[0]

    ratio = totals['total_coding'] / totals['total_noncoding'] if totals['total_noncoding'] != 0 else None
    print(f"Total coding genes: {totals['total_coding']}")
    print(f"Total non-coding genes: {totals['total_noncoding']}")
    print(f"Ratio (coding/non-coding): {ratio:.2f}")

def print_question_3(species_df):
    """Question 3: Min and max number of proteins"""
    print("\n=== Question 3 ===")
    stats = species_df.agg(
        spark_min('coding_genes').alias('min_proteins'),
        spark_max('coding_genes').alias('max_proteins')
    ).collect()[0]

    print(f"Minimal number of proteins in a genome: {stats['min_proteins']}")
    print(f"Maximal number of proteins in a genome: {stats['max_proteins']}")

def print_question_4(species_df):
    """Question 4: Average length of a feature"""
    print("\n=== Question 4 ===")
    totals = species_df.agg(
        spark_sum('genome_size').alias('total_bases'),
        spark_sum('num_genes').alias('total_genes')
    ).collect()[0]

    avg_feature_length = totals['total_bases'] / totals['total_genes']
    print(f"Average length of a feature: {avg_feature_length:.2f} bp")

def print_question_5(species_df):
    """Question 5: Create DataFrame without non-coding genes"""
    print("\n=== Question 5 ===")
    coding_df = species_df.select(
        'accession',
        'name',
        'genome_size',
        'coding_genes'
    )
    print("Non-coding (RNA) features removed. Cleaned DataFrame created.")
    print(f"Cleaned DataFrame has {coding_df.count()} genomes")
    return coding_df

def print_question_6(coding_df):
    """Question 6: Average length in cleaned version"""
    print("\n=== Question 6 ===")
    totals_coding = coding_df.agg(
        spark_sum('genome_size').alias('total_bases'),
        spark_sum('coding_genes').alias('total_coding')
    ).collect()[0]

    avg_coding_length = totals_coding['total_bases'] / totals_coding['total_coding']
    print(f"Average length of a coding feature: {avg_coding_length:.2f} bp")

def main():
    """Main function to run the analysis."""
    # Local Spark session
    # spark = SparkSession.builder \
    #     .appName('assignment5_mahsa') \
    #     .master('local[16]') \
    #     .config('spark.executor.memory', '128g') \
    #     .config('spark.driver.memory', '128g') \
    #     .getOrCreate()

    # Remote Spark cluster
    spark = SparkSession.builder \
    .appName("assignment5_mahsa_zamanifard") \
    .master("spark://spark.bin.bioinf.nl:7077") \
    .getOrCreate()

    gbff_file = sys.argv[1]
    print(f"Processing file: {gbff_file}")

    # Parse GenBank file (runs on driver)
    records = list(SeqIO.parse(gbff_file, "genbank"))
    info = [extract_info(record) for record in records]

    # Create DataFrame and optimize
    species_df = spark.createDataFrame(info)
    species_df.cache()  # Cache because we use it multiple times

    print(f"\nTotal genomes: {species_df.count()}\n")
    species_df.show(5, truncate=False)

    # Answer all questions
    print_question_1(species_df)
    print_question_2(species_df)
    print_question_3(species_df)
    print_question_4(species_df)
    coding_df = print_question_5(species_df)
    print_question_6(coding_df)

    # Cleanup: Remove cached data from memory
    species_df.unpersist()

    # Stop Spark session (releases all resources)
    spark.stop()

if __name__ == "__main__":
    main()
