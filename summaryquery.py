import datetime,subprocess,shlex,pexpect
from converter import *
from ksp import *
from isqlconverter import *

def execute_query(G,q_source,q_predicate,q_target,q_limit,isqlpath='/usr/local/virtuoso-opensource/bin/isql'):
    '''Queries db and returns results.
    '''
    delim="_______________________________________________________________________________\r\n\r\n"

    # Start isql process
    child = pexpect.spawn(isqlpath)
    child.expect('SQL> ')

    search_time = datetime.datetime.now()

    # Path generator.
    pathgen = topkgen(G,q_source,q_target,q_predicate)

    # Readout
    info_list=[]

    # Finishing condition.
    remainingresults = q_limit

    # holds all results
    resultset=set()

    ask_time_total=0.0 # measures total ask time
    select_time_total=0.0 # measures total ask time
    print "L: path length, RP: results of this *path*, T:time"
    print "L RP T"
    for curspath_cost,curspath in pathgen:
        curlength=curspath_cost[0]

        # integer path length (edge count)
        int_path_length=len(curspath)/2

        # Convert path to an ASK query and a SELECT query.
        ask_query,select_query = get_sparql_from_path(curspath)

        # Execute ASK query.
        child.sendline('SPARQL '+ask_query+';')
        child.expect('SQL> ',timeout=1000000)
        res=child.before
        idelim=res.rfind(delim)
        ask_output=res[idelim+len(delim):]
        time_ask,has_results=process_ask_isql(ask_output)
        ask_time_total+=time_ask
        #print "Has results: ",has_results
        # Test if query contains any results.
        if has_results:

            # Execute SELECT query
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
                print curlength, len(cur_useful_results), (datetime.datetime.now()-search_time).total_seconds()
                info_list.append((curspath,curlength,select_query,cur_useful_results,ask_query,time_ask,time_select))

            # Limit has been reached.
            if remainingresults==0:
                break

    if not info_list:
        print "No connectivity between given RDF types."
        return None,None,"No connectivity between given RDF types."
    else:
        # All results.
        results_sorted=[r[3] for r in info_list]

        # Final query.
        COMBINED_SELECT_SPARQL="SELECT DISTINCT * {"
        for row in info_list:
            COMBINED_SELECT_SPARQL+="{"+row[2]+"}UNION"
        COMBINED_SELECT_SPARQL=COMBINED_SELECT_SPARQL[:-5]+"}"

        # duration of query processing
        search_time = datetime.datetime.now()-search_time
        search_time=search_time.total_seconds()
        te=search_time-ask_time_total-select_time_total
        return (COMBINED_SELECT_SPARQL,results_sorted,info_list),(ask_time_total,select_time_total,te,search_time),None
