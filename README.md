# semanticsdata
Evaluation data for SEMANTICS 2015.

##Dependencies
Issue the following commands to install all needed packages in Ubuntu 14.04.

        sudo apt-get install python-pip python-librdf ipython python-psutil

        sudo pip install networkx

##Dataset links
The 100% DBPEDIA dataset can be downloaded from the link below:
* The 100% DBPEDIA dataset: http://benchmark.dbpedia.org/benchmark_100.nt.bz2

##Summary Creation
1. To create the RDF-type summary graph of a given N-Triples RDF dataset, execute the summary creation script:

        create_summary.py benchmark_x.nt
where benchmark_x.nt is the input dataset.

##Shortest Paths Algorithm
An implementation of the shortest paths algorithm presented in the paper is available in the file ksp.py.

##Path-to-SPARQL Converter
The file converter.py contains an implementation of the path-to-SPARQL converter.

