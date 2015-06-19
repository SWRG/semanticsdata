import networkx as nx
import heapq,itertools
from collections import defaultdict, deque

def topkgen(G, source, target, predicate):
    """
    Produces paths from source to target that have the predicate in place of the first or last edge.
    """

    def get_path(child):
        path=deque()
        path.appendleft(child[1])
        cur=child
        path_triples=0
        path_length=0
        edge_count_of={}
        while cur != (0,source):
            # Each node in T only has exactly one incoming edge!
            edge=T.in_edges([cur], data=True)[0]
            cur=edge[0]

            # Translate tree node to graph node, eg: (parent,edge_label,child)
            gedge=(edge[0][1],edge[2][3],edge[1][1])
            if gedge not in edge_count_of:
                edge_count_of[gedge]=0
            edge_count_of[gedge]+=1

            if edge_count_of[gedge]>edge[2][2]:
                return None,None
            path_triples+=edge[2][2]
            path_length+=1


            path.appendleft(gedge)
            path.appendleft(gedge[0])
        return (path_length,-path_triples),list(path)

    i = itertools.count()
    depth = 0
    lastparentdepth=0
    T = nx.DiGraph()
    paths=[]
    #Each node in the Tree is named as (serial_number,node_name_in_G)
    T_source=(next(i),source)
    T.add_node(T_source)
    queue = deque([(depth,T_source)])

    while queue:
        parentdepth,(parent_i,parent) = queue.popleft()

        if lastparentdepth < parentdepth:
            # Yield all paths of previous depth.
            while paths:
                yield heapq.heappop(paths)

        lastparentdepth=parentdepth

        # Iterate over it's outgoing edges.
        for edge in G.out_edges_iter([parent],data=True):
            _,child,attrdict=edge
            t_child=(next(i),edge[1])
            T.add_edge((parent_i,parent), t_child,attrdict)

            # Find reverse path from target (child) to source
            curpath_score,curpath=get_path(t_child)
            if curpath:
                queue.append((parentdepth+1,t_child))
                if child == target:
                    if len(curpath)>=3 and (curpath[1][1]==predicate or curpath[-2][1]==predicate):
                        heapq.heappush(paths, (curpath_score,curpath))
            else:
                # Mark t_child as finished.
                T.remove_edge((parent_i,parent), t_child)