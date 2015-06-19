# -*- coding: utf-8 -*-
"""
Path to SPARQL converter.

:author: Spyridon Kazanas
:contact: s.kazanas@gmail.com
"""
unknown_class_name="u"

def get_sparql_from_path(path):
    '''
    Path to SPARQL converter. The first variable is ?a and the last variable is
    ?b.

    :param path: The path to be converted to SPARQL.
    :returns: A tuple that contains the equivalent ASK and SELECT SPARQL queries.
    '''

    def add_triple_pattern(sparql_var,rdf_type):
        if sparql_var not in added_patterns:
            added_patterns[sparql_var] = set()
        added_patterns[sparql_var].add(rdf_type)
        return

    def add_conn_pattern(sparql_var_1,predicate,sparql_var_2):
        if (sparql_var_1,sparql_var_2) not in added_conn_patterns:
            added_conn_patterns[(sparql_var_1,sparql_var_2)] = set()
        added_conn_patterns[(sparql_var_1,sparql_var_2)].add(predicate)
        return

    added_patterns = {}
    added_conn_patterns = {}
    sn=1


    # Sequential iteration over path's edges.
    edges_list=path[1::2]
    last_index=len(path[1::2])-1
    for i,edge in enumerate(edges_list):

        (source,predicate,target) = edge
        # Test for first edge.
        if i==0:
            s_var="?a"
        else:
            s_var="?_"+str(sn)
            sn+=1

        # Test for last edge.
        if i==last_index:
            t_var="?b"
        else:
            t_var="?_"+str(sn)

        # Check if node is typed
        if type(source) is frozenset:
            for s in source: 
                add_triple_pattern(s_var,s)

        # Check if node is typed
        if type(target) is frozenset:
            for t in target:
                add_triple_pattern(t_var,t)

        # connect s_var to t_var with predicate
        add_conn_pattern(s_var,predicate,t_var)

    # string to be filled with sparql triple patterns
    triplepatterns = ""

    # concatenate rdf type triple patterns
    for sparql_var in added_patterns:
        rdftypes = added_patterns[sparql_var]
        for rdftype in rdftypes:
            triplepatterns += sparql_var + " a <" + rdftype + "> . "

    # concatenate subject to object connection triple patterns
    for (sparlq_var_1,sparql_var_2) in added_conn_patterns:
        predicates = added_conn_patterns[(sparlq_var_1,sparql_var_2)]
        for predicate in predicates:
            triplepatterns +=  sparlq_var_1 + " <" + predicate + "> " + sparql_var_2 + " . "

    select_query = "".join([
                "SELECT DISTINCT ?a ?b WHERE { ",
                triplepatterns,
                "}"])

    ask_query = "".join([
                "ASK { ",
                triplepatterns,
                "}"])
    return ask_query,select_query
