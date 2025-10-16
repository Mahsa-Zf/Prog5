"""Assignment 5: PySpark DataFrame operations on GenBank files."""
import sys
from Bio import SeqIO
sys.path.append('/opt/spark/python')
sys.path.append('/opt/spark/python/lib/py4j-0.10.9.7-src.zip')
from pyspark import SparkFiles
from pyspark.sql import SparkSession, Row 
# , SQLContext

spark = SparkSession.builder.appName("assignment5_mahsa_zamanifard").master("spark://spark.bin.bioinf.nl:7077").getOrCreate()
sc = spark.sparkContext
# sqlContext = SQLContext(sc)

"""Be sure to synchronize the resources your request (local[16] means 16 processes, "128g" 
means that much RAM) with the resources you request via the SLURM script 
e.g. --ntasks and --mem directives!"""

def extract_info(record):
    """Extract Species and Protein info from a single GenBank record."""
    species = record.annotations.get("organism", "Unknown")
    accession = record.annotations.get("accessions", ["Unknown"])[0]
    version = record.annotations.get("sequence_version", None)
    accession_version = f"{accession}.{version}" if version else accession

    genome_size = len(record.seq)
    gene_count, coding_count = 0, 0
    for feature in record.features:
        # to exclude location containing < or > symbols
        if not any(x in str(feature.location) for x in (">", "<")):
            if feature.type.lower() == "gene":
                gene_count += 1
            elif feature.type.lower() in ["cds", "pro-peptides"]:
                coding_count += 1

    # we always have gene in the feature.type that locates the corresponding basepairs
    # for every coding/non-coding sequence, since CDS is the only coding sequence
    # we can find non-coding count by subtracting coding_count from gene_count
    non_coding_count = gene_count - coding_count


    # Species data for insertion
    species_data = {
        "accession": accession_version,
        "name": species,
        "genome_size": genome_size,
        "num_genes": gene_count,
        "coding_genes": coding_count,
        "non_coding_genes": non_coding_count,
    }

    return species_data


########## FOR TESTING PURPOSES#######
# gbff_file = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.2\
# .genomic.gbff"
# with open(gbff_file, "r", encoding="utf-8") as handle:
#             for rec in SeqIO.parse(handle, "genbank"):
#                 sp = extract_info(rec)
#                 print(sp)
#                 break


def main():
    gbff_file = sys.argv[1]  # Get file from command line argument
    print(f"Processing file: {gbff_file}")

    records = list(SeqIO.parse(gbff_file, "genbank"))

    # Parallelize the list of SeqIO records
    records_rdd = sc.parallelize(records, numSlices=16)

    # Apply extraction function to each record in parallel
    species_rdd = records_rdd.map(extract_info)

    # Create DataFrame from Rows RDD
    df = spark.createDataFrame(species_rdd)

    df.show(5, truncate=False)

    # def print_question_1():
    #     # How many features does an Archaeal genome have on average?
    #     avg_features = species_df.groupBy('accession').count().agg({'count': 'avg'}).collect()[0][0]
    #     print(f"Average number of features per Archaeal genome: {avg_features}")

    # def print_question_2():
    #     # What is the ratio between coding and non-coding features?
    #     coding = species_df.filter(species_df.coding_genes > 0).count()
    #     noncoding = species_df.filter(species_df.non_coding_genes > 0).count()
    #     ratio = coding / noncoding if noncoding != 0 else None
    #     print(f"Ratio between coding and non-coding features: {ratio}")

    # def print_question_3():
    #     # Minimal and maximal number of proteins in a genome for all organisms in the file
    #     protein_counts = species_df.filter(species_df.coding_genes > 0).groupBy('accession').count()
    #     min_proteins = protein_counts.agg({'count': 'min'}).collect()[0][0]
    #     max_proteins = protein_counts.agg({'count': 'max'}).collect()[0][0]
    #     print(f"Minimal number of proteins in a genome: {min_proteins}")
    #     print(f"Maximal number of proteins in a genome: {max_proteins}")

    # def print_question_4():
    #     # What is the average length of a feature?
    #     avg_length = species_df.agg({'num_genes': 'avg'}).collect()[0][0]
    #     print(f"Average length of a feature: {avg_length}")

    # def print_question_5():
    #     # Remove all non-coding (RNA) features and save the cleaned-up version as a new DataFrame
    #     coding_df = species_df.drop('non_coding_genes')
    #     coding_df.createOrReplaceTempView("coding_genomes")
    #     print("Non-coding (RNA) features removed. Cleaned DataFrame registered as 'coding_genomes'.")

    # def print_question_6():
    #     # What is the average length of a feature in this cleaned-up version?
    #     avg_coding_length = coding_df.agg({'coding_genes': 'avg'}).collect()[0][0]
    #     print(f"Average length of a feature in the cleaned-up DataFrame: {avg_coding_length}")



    # print_question_1()
    # print_question_2()
    # print_question_3()
    # print_question_4()
    # print_question_5()
    # print_question_6()
if __name__ == "__main__":
    main()
