import datetime,subprocess,shlex,pexpect
from ksp import *
from isqlconverter import *

def execute_incremental_query(q_source,q_predicate,q_target,q_limit,isqlpath='/usr/local/virtuoso-opensource/bin/isql'):
    '''Queries db and returns results.
    '''
    delim="_______________________________________________________________________________\r\n\r\n"

    # Start isql process
    child = pexpect.spawn(isqlpath)
    child.expect('SQL> ')
    search_time = datetime.datetime.now()

    # End-nodes' RDF types (SPARQL).
    types_query=""
    for i in q_source:
        types_query+="?a a <"+i+">."
    for i in q_target:
        types_query+="?b a <"+i+">."

    # Readout
    info_list=[]

    # Finishing condition.
    remainingresults = q_limit

    # holds all results
    resultset=set()

    ask_time_total=0.0 # measures total ask time
    select_time_total=0.0 # measures total ask time
    cur_path_length=1
    print "L: path length, R: results of this *length*, T:time"
    print "L R T"
    while remainingresults > 0:

        #print "Path Length=",cur_path_length

        v=cur_path_length-1
        query1="{"+types_query+"?a"
        for i in range(v):
            var="?_"+str(v)+"_"+str(i)
            query1+=" !a "+var+"."+var
        query1+=" <"+q_predicate+"> ?b.}"

        query2="{"+types_query+"?a <"+q_predicate+"> "
        for i in range(v):
            var="?_"+str(v)+"_"+str(i)
            query2+=var+"."+var+" !a "
        query2+="?b.}"

        query="{"+query1+"UNION"+query2+"}"
        ask_query="ASK "+query
        select_query="SELECT DISTINCT ?a ?b WHERE "+query
        #print select_query

        # Execute ASK query.
        child.sendline('SPARQL '+ask_query+';')
        child.expect('SQL> ',timeout=1000000)
        res=child.before
        idelim=res.rfind(delim)
        ask_output=res[idelim+len(delim):]
        time_ask,has_results=process_ask_isql(ask_output)
        ask_time_total+=time_ask

        # Test if query contains any results.
        if has_results:

            # Execute SELECT query.
            child.sendline('SPARQL '+select_query+';')
            child.expect('SQL> ',timeout=1000000)
            res=child.before
            idelim=res.rfind(delim)
            select_output=res[idelim+len(delim):]
            time_select,cur_results=process_select_isql(select_output)
            select_time_total+=time_select

            # Results of this select query (may contain less unique results).
            # add results to list and mark last index for the limit
            limit=0

            # holds results of this query
            cur_useful_results=[]
            for i,r in enumerate(cur_results):

                if r not in resultset:
                    cur_useful_results.append(r)
                    resultset.add(r)
                    limit=i+1
                    remainingresults-=1

                    if remainingresults==0:
                        # add limit to the last select query
                        select_query+='LIMIT '+str(limit)
                        break
            # Store this path only if it contains useful results
            if len(cur_useful_results) > 0:
                print cur_path_length,len(cur_useful_results), (datetime.datetime.now()-search_time).total_seconds()

                info_list.append((cur_path_length,select_query,cur_useful_results,ask_query,time_ask,time_select))

        cur_path_length+=1

    if not info_list:
        print "No connectivity between given RDF types."
        return None,None,"No connectivity between given RDF types."
    else:
        # All results.
        results_sorted=[r[2] for r in info_list]

        # Final query.
        COMBINED_SELECT_SPARQL="SELECT DISTINCT * {"
        for row in info_list:
            COMBINED_SELECT_SPARQL+="{"+row[1]+"}UNION"
        COMBINED_SELECT_SPARQL=COMBINED_SELECT_SPARQL[:-5]+"}"

        # duration of query processing
        search_time = datetime.datetime.now()-search_time
        search_time=search_time.total_seconds()
        te=search_time-ask_time_total-select_time_total
        return (COMBINED_SELECT_SPARQL,results_sorted,info_list),(ask_time_total,select_time_total,te,search_time),None
