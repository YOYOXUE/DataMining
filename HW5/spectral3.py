import networkx as nx
import numpy as np
from collections import defaultdict
from numpy import linalg as LA
import sys
def cluster(newnodes,newedges):
    d = defaultdict(int)
    a={}
    b={}
    ind=0
    #print(d)defaultdict(<class 'int'>, {1: 287, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, ...})
    A=np.zeros((len(newnodes),len(newnodes)))
    D=np.zeros((len(newnodes),len(newnodes)))
    for node in newnodes:
        a[node]=ind
        b[ind]=node
        ind+=1
    for edge in newedges:
        i=int(edge[0])
        j=int(edge[1])
        d[i]+=1
        d[j]+=1
        A[a[i]][a[j]]=1
        A[a[j]][a[i]]=1

    for key in d.keys():
        D[a[key]][a[key]]=d[key]

    L=D-A
    value,vector = LA.eig(L)
    sorted_indices=np.argsort(value)
    # print(value,'\n',sorted_indices)
    # [ 770.00209864+0.j  711.00001998+0.j  482.00013495+0.j ...,
    #     1.00000000+0.j    1.00000000+0.j    1.00000000+0.j]
    #  [18 22 31 ...,  2  1  0]

    #2nd smallest vector
    vector2=vector[:,sorted_indices[1]]
    tmp1=[]
    tmp2=[]
    for index in range(len(vector2)):
        if vector2[index]>0:
            tmp1.append(b[index])#b:a dict key=userid index,value=real userid(node number)
        else:
            tmp2.append(b[index])

    if len(tmp1)>len(tmp2):
        #find larger cluster
        tmp=tmp1#larger one to split
        rem=tmp2#smaller one to remove
    else:
        tmp=tmp2
        rem=tmp1
    return rem,tmp


def it(G):
    newnode=G.nodes
    newedge=G.edges
    rmv,tmp=cluster(newnode,newedge)
    s01=G.subgraph(tmp).copy()
    s02=G.subgraph(rmv).copy()
    clusters.append(s01)
    clusters.append(s02)
    #find the largest cluster
    nc=[i.nodes for i in clusters]#nodes in each clusters
    inde=nc.index(max(nc,key=len))
    G=clusters[inde]
    Largest=G
    del clusters[inde]
    return Largest
if __name__=="__main__":
    dataset=sys.argv[1]
    output=sys.argv[3]
    k=int(sys.argv[2])
    edges=[]
    nodes=[]
    with open(dataset,'r') as o:
        for line in o:
            line=[int(i) for i in line.split()]
            edges.append((int(line[0]),int(line[1])))
            nodes=nodes+line
    nodes=list(set(nodes))
    nodes.sort()
    # print(nodes)[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, ..]
    # print(edges)
    # [(1, 2), (1, 3), (1, 4),..]
    G=nx.Graph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)

    #first iteration split the original data into to two lists
    clusters=[]
    # remvs,temp=cluster(nodes,edges)
    # s1=G.subgraph(temp).copy()
    # s2=G.subgraph(remvs).copy()
    # if len(s1.nodes)>len(s2.nodes):
    #     G=s1
    #     clusters.append(s2)
    # else:
    #     G=s2
    #     clusters.append(s1)
    while k-1>0:
        G=it(G)
        k-=1
    clusters.append(G)

    with open(output,'w') as t:
        for j in clusters:
            i=list(j.nodes)
            i.sort()
            line=str(i)
            line=line[1:-1]
            line=line.replace(' ','')
            t.write(line)
            t.write('\n')
