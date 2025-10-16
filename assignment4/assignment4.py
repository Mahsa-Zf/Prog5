"""
We're going to use sql to create and modify datasets
from a .gbff file of archaea genomes.
We will parse the .gbff file using biopython and extract the following information:
Species names
Species genome accession numbers (including version! e.g. "NZLWMV01000163.1")
Genome size (in basepairs)
Number of Genes found in the genome
Number of Proteins found in the genome
TaxDB Id of the species
All proteins found in the gbff file, including;
proteinid
protein product name
protein location in the genome
locustag
Gene reference
EC number (if present)
GO annotations (if present)
Then we will populate a sql database with this information.
"""
import sys
from Bio import SeqIO
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from mpi4py import MPI
############################# PARALLELIZED VERSION ###########################
def extract_info(record):
    """Extract Species and Protein info from a single GenBank record."""
    species = record.annotations.get("organism", "Unknown")

    taxdb_id = None
    source_feature = record.features[0]
    db_xrefs = source_feature.qualifiers.get("db_xref", [])
    if db_xrefs and db_xrefs[0].startswith("taxon:"):
        taxdb_id = db_xrefs[0].split(":")[1]

    accession = record.annotations.get("accessions", ["Unknown"])[0]
    version = record.annotations.get("sequence_version", None)
    accession_version = f"{accession}.{version}" if version else accession

    genome_size = len(record.seq)

    gene_count, protein_count = 0, 0
    for feature in record.features:
        if feature.type == "gene":
            gene_count += 1
        elif feature.type == "CDS":
            protein_count += 1

    # Species data tuple for insertion
    species_data = {
        "accession": accession_version,
        "name": species,
        "genome_size": genome_size,
        "num_genes": gene_count,
        "num_proteins": protein_count,
        "taxdb_id": taxdb_id
    }

    protein_list = []
    for feature in record.features:
        if feature.type == "CDS":
            qualifiers = feature.qualifiers
            proteinid = qualifiers.get("protein_id", [""])[0]
            product = qualifiers.get("product", [""])[0]
            location = str(feature.location)
            locustag = qualifiers.get("locus_tag", [""])[0]
            gene_ref = qualifiers.get("gene", [""])[0]
            ec_number = qualifiers.get("EC_number", [""])
            ec_number = ec_number[0] if ec_number else ""
            go_terms = qualifiers.get("GO", [""])

            protein_list.append({
                "protein_id": proteinid,
                "product_name": product,
                "location": location,
                "locustag": locustag,
                "gene_ref": gene_ref,
                "ec_number": ec_number,
                "go_annotations": go_terms,
                "accession": accession_version
            })

    return species_data, protein_list

def main():
    """Main function to parse the .gbff file and populate the SQL database."""
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if len(sys.argv) != 2:
        if rank == 0:
            print("Usage: srun python assignment4.py .gbff_file")
        sys.exit(1)

    gbff_file = sys.argv[1]

    # Root rank reads all records and distributes them
    if rank == 0:
        # using "genbank" to tell Biopython’s SeqIO parser that the file is in GenBank format
        records = list(SeqIO.parse(open(gbff_file, "r"), "genbank"))
        # Split records into roughly equal chunks for each rank
        chunks = [records[i::size] for i in range(size)]
    else:
        chunks = None

    # Scatter chunks to all ranks
    local_records = comm.scatter(chunks, root=0)

    # Each rank processes its own chunk
    local_results = [extract_info(rec) for rec in local_records]

    # Gather results at root
    gathered_results = comm.gather(local_results, root=0)

    if rank == 0:
        # Flatten results
        species_rows = []
        protein_rows = []
        for rank_results in gathered_results:
            for species_data, proteins in rank_results:
                species_rows.append(species_data)
                protein_rows.extend(proteins)

        # Set up DB connection and create tables (only at root)
        with open("mysql.txt", encoding="utf-8") as constr:
            connectionstring = constr.read().strip()
        engine = create_engine(connectionstring)
        conn = engine.connect()

        conn.execute(text("DROP TABLE IF EXISTS Protein, Species"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Species (
                accession VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255),
                genome_size INT,
                num_genes INT,
                num_proteins INT,
                taxdb_id VARCHAR(50)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Protein (
                protein_id VARCHAR(50),
                product_name VARCHAR(255),
                location VARCHAR(50),
                locustag VARCHAR(50) PRIMARY KEY,
                gene_ref VARCHAR(50),
                ec_number VARCHAR(50),
                go_annotations TEXT,
                accession VARCHAR(50),
                FOREIGN KEY (accession) REFERENCES Species(accession)
            )
        """))

        # Bulk insert Species data
        species_insert_query = """
            INSERT INTO Species 
            (accession, name, genome_size, num_genes, num_proteins, taxdb_id)
            VALUES 
            (:accession, :name, :genome_size, :num_genes, :num_proteins, :taxdb_id)
        """
        conn.execute(text(species_insert_query), species_rows)

        # Bulk insert Protein data
        protein_insert_query = """
            INSERT INTO Protein 
            (protein_id, product_name, location, locustag, gene_ref, ec_number, go_annotations, accession)
            VALUES 
            (:protein_id, :product_name, :location, :locustag, :gene_ref, :ec_number, :go_annotations, :accession)
        """
        conn.execute(text(protein_insert_query), protein_rows)

        conn.close()

if __name__ == "__main__":
    main()

################## UNPARALLELIZED VERSION ####################

# def main():
#     """Main function to parse the .gbff file and populate the SQL database."""
#     # Creating a connection
#     with open("mysql.txt", encoding="utf-8", mode="r") as constr:
#         connectionstring = constr.read().strip()
#     # connectionstring = open("mysql.txt", encoding="utf-8").read().strip()
#         engine = create_engine(connectionstring)
#         conn = engine.connect()
#         # conn.execute('status') # deprecated

#         # with engine.connect() as conn: # ---> for testing purposes
#         #     result = conn.execute(text("SELECT CONNECTION_ID();"))
#         #     for row in result:
#         #         print("Connection ID:", row[0])


#         # After reviewing the .gbff file, my proposed database schema is as follows:

#         # Species Table:
#         # name,
#         # accession number(PK),
#         # genome size,
#         # number of genes,
#         # number of proteins,
#         # taxdb_id

#         # Protein Table:
#         # proteinid,
#         # product name,
#         # location,
#         # locustag(PK),
#         # gene_ref,
#         # ec_number,
#         # go_annotations,
#         # species_accession(FK)

#         conn.execute(text("DROP TABLE IF EXISTS Protein, Species"))
#         conn.execute(text("""
#         CREATE TABLE IF NOT EXISTS Species (
#             accession VARCHAR(50) PRIMARY KEY,
#             name VARCHAR(255),
#             genome_size INT,
#             num_genes INT,
#             num_proteins INT,
#             taxdb_id VARCHAR(50)
#         )
#         """))

#         conn.execute(text("""
#         CREATE TABLE IF NOT EXISTS Protein (
#             protein_id VARCHAR(50) ,
#             product_name VARCHAR(255),
#             location VARCHAR(50),
#             locustag VARCHAR(50) PRIMARY KEY,
#             gene_ref VARCHAR(50),
#             ec_number VARCHAR(50),
#             go_annotations TEXT,
#             gene_id INT,
#             accession VARCHAR(50),
#             FOREIGN KEY (accession) REFERENCES Species(accession)
#         )
#         """))

#         if len(sys.argv) != 2:
#             print("Usage: srun python assignment4.py .gbff_file")
#             sys.exit(1)

#         gbff_file = sys.argv[1]
#         # Path to the .gbff file for testing purposes
#         # gbff_file = \
#         # "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1\
# .genomic.gbff"

#         # Parse the .gbff file and extract information to fill the database
#         with open(gbff_file, "r", encoding="utf-8") as handle:
#             for record in SeqIO.parse(handle, "genbank"):
#                 # Species name
#                 species = record.annotations.get("organism", "Unknown")

#                 # Taxonomy ID: this id is meant to be unique for
#                 # each species to show its taxonomic position
#                 # Taxonomy position means where the species is located in the tree of life
#                 taxdb_id = None
#                 source_feature = record.features[0]  # 'source' is always first feature
#                 db_xrefs = source_feature.qualifiers.get("db_xref", [])
#                 if db_xrefs and db_xrefs[0].startswith("taxon:"):
#                     taxdb_id = db_xrefs[0].split(":")[1]



#                 # insert Genome information into Genome table
#                 # Accession number with version
#                 accession = record.annotations.get("accessions", ["Unknown"])[0]
#                 version = record.annotations.get("sequence_version", None)
#                 accession_version = f"{accession}.{version}" if version else accession


#                 # Genome size in base pairs, stated in the metadata of the record LOCUS e.g. 15799 bp
#                 genome_size = len(record.seq)


#                 # Count genes and proteins
#                 # proteins = []  # ---> for testing purposes
#                 gene_count = sum(1 for feature in record.features if feature.type == "gene")
#                 protein_count = sum(1 for feature in record.features if feature.type == "CDS")
#                 conn.execute(text(
#                     "INSERT INTO Species (accession, name, genome_size, num_genes, num_proteins, taxdb_id) "
#                     "VALUES (:accession, :name, :genome_size, :num_genes, :num_proteins, :taxdb_id)"
#                 ), {
#                     "accession": accession_version,
#                     "name": species,
#                     "genome_size": genome_size,
#                     "num_genes": gene_count,
#                     "num_proteins": protein_count,
#                     "taxdb_id": taxdb_id
#                 })

#                 # Iterate over features to extract protein information
#                 # and fill the proteins table
#                 for feature in record.features:
#                     # coding sequences that usually represent proteins
#                     if feature.type == "CDS":
#                         qualifiers = feature.qualifiers

#                         proteinid = qualifiers.get("protein_id", [""])[0]
#                         product = qualifiers.get("product", [""])[0]
#                         location = feature.location
#                         locustag = qualifiers.get("locus_tag", [""])[0]
#                         gene_ref = qualifiers.get("gene", [""])[0]
#                         ec_number = qualifiers.get("EC_number", [""])
#                         ec_number = ec_number[0] if ec_number else ""
#                         go_terms = qualifiers.get("GO", [""])

#                         # fill the proteins table
#                         # str type casting for location to store it as a readable short text
#                         conn.execute(text(
#                             "INSERT INTO Protein (protein_id, product_name, location, locustag, gene_ref, ec_number, go_annotations, accession) "
#                             "VALUES (:protein_id, :product_name, :location, :locustag, :gene_ref, :ec_number, :go_annotations, :accession)"
#                         ), {
#                             "protein_id": proteinid,
#                             "product_name": product,
#                             "location": str(location),
#                             "locustag": locustag,
#                             "gene_ref": gene_ref,
#                             "ec_number": ec_number,
#                             "go_annotations": go_terms,
#                             "accession": accession_version
#                         })


#                         # For testing purposes
#                         # Uncomment the following lines to see the proteins being processed
#                         # and to verify the extracted information
#                         # protein = {
#                         #     "proteinid": proteinid,
#                         #     "product": product,
#                         #     "location": str(location),
#                         #     "locustag": locustag,
#                         #     "gene_ref": gene_ref,
#                         #     "ec_number": ec_number,
#                         #     "go_terms": go_terms
#                         # }
#                         # proteins.append(protein)



#                 # print("Proteins found:")
#                 # for p in proteins:
#                 #     print(p)
#                 # Optionally break after first record if file contains multiple genomes
#                 # break


# if __name__ == "__main__":
#     main()
