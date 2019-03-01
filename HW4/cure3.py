from heapq import *
import numpy as np
import sys

def dist(newpointslist,point):
    mind=float("inf")
    for i in newpointslist:
        for j in point:
            dis=np.sum((i-j)**2)
            if dis<mind:
                mind=dis
    return mind

def findre(sorted_data,n):
    re=[]
    for nn in range(n):
        if nn==0:
            re.append(sorted_data[0])
            sorted_data=sorted_data[1:]
        elif nn>0:
            temmin=[]
            c=0
            for j in sorted_data:
                tem=[]
                for i in re:
                    di=np.sum((i-j)**2)
                    tem.append(list([di,j,c]))
                c+=1
                temmin.append(min(tem))
            temp=max(temmin)
            re.append(temp[1])
            sorted_data=np.delete(sorted_data,temp[2],0)
    return re

def assign(point,represent_mov):
    c=0
    clusterset=[]
    for cluster in represent_mov:
        each_c_dis=float("inf")
        for re in cluster:
            d=np.sum((point-re)**2)
            if each_c_dis>d:
                each_c_dis=d
        clusterset.append([each_c_dis,c])
        c+=1
    closest=min(clusterset)
    return closest[1]
if __name__=="__main__":
    sample=sys.argv[1]#'sample_data.txt'
    fulldata=sys.argv[2]#'full_data.txt'
    k=int(sys.argv[3])#3
    n=int(sys.argv[4])#4
    p=float(sys.argv[5])#.2
    output=sys.argv[6]
    #---read sample_data---
    with open(sample,'r') as s:
        points=[]
        for l in s:
            points.append(np.array([float(x) for x in l.split(',')]))
    lenth=len(points)
    h=[]
    for i in range(lenth-1):
        for j in range(i+1,lenth):
            d=np.sum((points[i]-points[j])**2)
            h.append([d,[i,j]])
    heapify(h)
    #initiallly clusterAndSummary
    clusterAndSummary = {} # {clusterID:[numPoints, [[p1x p1y]]], ...}ID:[1, [array([ 0.777057, -0.415491])]]
    for i in range(len(points)):
        clusterAndSummary[i] = [1, list([points[i]])]
    # print(clusterAndSummary[99]) from 0 to 99,in total 100 points
    #[1, [array([ 0.92142 , -0.333052])]]

    #
    clusterID=len(clusterAndSummary)
    while len(clusterAndSummary) > k:
        # pop the top of heap and get two nearest clusters
        pair =heappop(h) # [dist, [id1,id2]]
        id1 = pair[1][0]
        id2 = pair[1][1]
        if not(id1 in clusterAndSummary) or not(id2 in clusterAndSummary): continue

        # merge two clusters and generate new summary
        # clusterAndSummary: {clusterID:[numPoints,[[p1x p1y],[p2x p2y]]], ...}
        summary_cluster_1 = clusterAndSummary[id1]
        summary_cluster_2 = clusterAndSummary[id2]

        del clusterAndSummary[id1]
        del clusterAndSummary[id2]

        new_numPoints = summary_cluster_1[0] + summary_cluster_2[0]
        new_points = summary_cluster_1[1] + summary_cluster_2[1]

        # update pairwise distance heap

        for i in clusterAndSummary: # i is cluster ID
            dd=dist(new_points,clusterAndSummary[i][1])
            heappush(h, [dd, [i, clusterID]])
        clusterAndSummary[clusterID] = list([new_numPoints,new_points])
        clusterID += 1

    sampleClusters=[]
    centroids=[]
    for key in clusterAndSummary.keys():
        s=np.array(clusterAndSummary[key][1])
        idex=np.lexsort([s[:,1], s[:,0]])
        sorted_data = s[idex, :]
        #     print(type(sorted_data)) np.array
        num=len(sorted_data)
        colsum=np.sum(sorted_data, axis=0)
        centroid=colsum/num
        centroids.append(centroid)
        sampleClusters.append(sorted_data)

    #pick up representative points
    represents=[]
    for x in sampleClusters:
        r=findre(x,n)
        represents.append(r)

    #move representative points toward centroids
    t=0
    represent_mov=[]
    for repre in represents:
    #     print(t)
        eachcluster=[]
        for rep in repre:
            rep=rep+(centroids[t]-rep)*p
            eachcluster.append(rep)
        represent_mov.append(eachcluster)
        t+=1

    #print representative points before move
    for re in represents:
        relist=[]
        for i in re:
            ii=i.tolist()
            relist.append(ii)
        print(relist)

    #final:collect full_data
    final=[]
    with open(fulldata,'r') as f:
        for line in f:
            point=np.array([float(x) for x in line.split(',')])
            c_num=assign(point,represent_mov)
            final.append(point.tolist()+list([c_num]))

    with open(output,'w') as o:
        for line in final:
            o.write(str(line[0])+','+str(line[1])+','+str(line[2]))
            o.write('\n')
