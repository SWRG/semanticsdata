# -*- coding: utf-8 -*-
"""
This class can be used to load an RDF-type summary graph.

:author: Spyridon Kazanas
:contact: s.kazanas@gmail.com
"""
import networkx as nx
import cPickle,csv
from os import path

class RDFTypeSummary():
    # db info
    db_graph = None
    db_file = None

    def __init__(self,inputfile=None):
        if inputfile is not None:
            self.loaddb(inputfile)

    def loaddb(self,inputfile):
        """
        Loads db_graph, shortest path data and labelings from save directory.
        Sorts labelings.

        :param dirpath: The directory containing data.
        """
        inputfile = path.abspath(path.expanduser(inputfile))

        print "Loading RDF-type summary graph: ",inputfile
        print "(please wait)"

        if path.isfile(inputfile):
            self.unloaddb()
            self.db_graph=nx.MultiDiGraph()
            with open(inputfile,"rb") as f:
                csvreader=csv.reader(f,delimiter=' ')
                for row in csvreader:
                    r=cPickle.loads(row[0])
                    self.db_graph.add_edge(r[0],r[1],r[2][3],{2:r[2][2],3:r[2][3]})
        else:
            print "File not found."
            return
        self.db_file = inputfile
        print "Done loading."
        return

    def unloaddb(self):
        self.db_graph = None
        self.db_file = None

    def dbinfo(self):
        print "RDF Type Summary Graph information:"
        print "    File : ",self.db_file
        return