# -*- coding: utf-8 -*-
"""
This program creates the RDF-type summary graph by reading triples from
specified N-triples file. The output graph is saved in the same directory.

:author: Spyridon Kazanas
:contact: s.kazanas@gmail.com
"""
import RDF,argparse,os,sys
import csv,cPickle,datetime

def findtypes(u):
    """
    Finds all rdf-types of input URI (or bnode) u and returns them as a set.

    :param u: Input URI (or bnode)
    :type s: str
    :return: Distinct rdf-types of input URI u.
    :rtype: set
    :requires: types
    """
    if u not in types:
        return None
    return types[u]

unknown_class_name="u"
BYPASS_TYPES=frozenset(["http://www.w3.org/2002/07/owl#Thing"])
rdftype="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
uname = {}
types={}

# substitutions_dict[(st,p,ot)] = number_of_replaced_triples
substitutions_dict = dict()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     epilog='Example: create_summary.py ./input.nt')

    parser.add_argument('inputfile',
                        type=str,
                        metavar='INPUT',
                        help='N-Triples file to be read.')

    args = parser.parse_args()

    # Process command line arguments
    inputfile = os.path.abspath(os.path.expanduser(args.inputfile))

    # Test input file existence
    if not os.path.isfile(inputfile):
        print "Input file not found: ",inputfile
        sys.exit('-1')

    output_dir = os.path.dirname(os.path.abspath(os.path.expanduser(args.inputfile)))
    output_file = os.path.join(output_dir,os.path.split(os.path.splitext(inputfile)[0])[1]+'.summary')

    time_start=datetime.datetime.now()

    # First Pass: Type dictionary creation.
    ERDF=0
    print "First Pass: Type dictionary creation."
    for triple in RDF.NTriplesParser().parse_as_stream("file:"+inputfile):
        ERDF+=1
        if str(triple.predicate) == rdftype and str(triple.object) not in BYPASS_TYPES:
            s=str(triple.subject)
            if s in types:
                types[s].add(str(triple.object))
            else:
                types[s]=set()
                types[s].add(str(triple.object))

    # Freeze sets
    types.update((k, frozenset(v)) for k, v in types.iteritems())

    # Second Pass: Graph creation.
    print "Second Pass: Graph creation."
    for triple in RDF.NTriplesParser().parse_as_stream("file:"+inputfile):
        if str(triple.predicate) != rdftype and not triple.object.is_literal():
            item = (str(triple.subject),str(triple.predicate),str(triple.object))

            # Subject types
            st_set = findtypes(item[0])
            if st_set is None:
                # untyped node
                st = (unknown_class_name,item[0])
            else:
                # typed node
                st = st_set

            # Object types
            ot_set = findtypes(item[2])
            if ot_set is None:
                # untyped node
                ot = (unknown_class_name,item[2])
            else:
                # typed node 
                ot = ot_set

            key = (st, item[1], ot)
            if key not in substitutions_dict:
                substitutions_dict[key] = 0
            substitutions_dict[key]+=1
    del types

    # save data
    print "Saving data."
    total_edges=0
    with open(output_file,'wb') as f:
        csvriter = csv.writer(f,delimiter=' ')
        for (st,p,ot), Nc in substitutions_dict.iteritems():
            csvriter.writerow([cPickle.dumps((st,ot,{2:Nc,3:p}))])
            total_edges+=1

    # Report
    print "REPORT"
    print "  ORIGINAL RDF:"
    print "    Total RDF Edges |E_RDF|=",ERDF
    print ""
    print "  RDF-TYPE SUMMARY:"
    print "    Total Edges |E_S|=",total_edges


    print "RDF-type summary graph file created: " + output_file
    time_duration=datetime.datetime.now()-time_start
    print "Total time: ",time_duration.total_seconds(),"sec"
