from itertools import combinations
from pyspark import SparkContext
import sys
#all singletons before filt
def singletons(baskets):
    count={}
    for i in baskets:
        for j in i:
            count[j] = count.get(j,0) + 1
    k=list(count.keys())
    k.sort()
    return k
# singleton=singletons(baskets)#all singletons
# print(singleton)
#[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]


def count_singletons(candidates,baskets):
    itemset_size = 1
    result = []
    if candidates:
        itemset_size = len(candidates[0])

    if not candidates:
        count = {}
        for basket in baskets:
            for item in basket:
                count[item] = count.get(item,0) + 1
        for (item, count) in count.items():
            result.append([item, count])
    else:
        result = candidates
    result.sort()
    return result
# singletons_count=count_singletons([],baskets)
# print(singletons_count)
# [[1, 114520], [2, 103104], [3, 91616], [4, 79953], [5, 68692],
# [6, 57107], [7, 45765], [8, 34211], [9, 22927], [10, 11505], [11, 11487],
# [12, 17112], [13, 28511], [14, 97213], [15, 74200], [16, 51478], [17, 40375],
# [18, 28527], [19, 17047], [20, 5780]]

def filt_singletons(itemsets,s):
    single = []
    for items in itemsets:
        if items[1]>=s:
            single.append(items[0])
    single.sort()
    return single
# singletons_fil=filt_singletons(singletons_count,entire_s)
# print(singletons_fil)
#[1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18]

# p=combinations(singletons_fil, 2)
def count_tuple(tuples,baskets):
    result={}
    for i in tuples:
        result[i]=0
    for basket in baskets:
        for t in result.keys():
            if set(t).issubset(set(basket)):
                result[t]=result[t]+1
    return result
# tuples_count=count_tuple(p,baskets)
# {(1, 2): 103104, ...}

def fil_multi(singletons_fil,s,baskets):
    result=[]
    itemset_size = 2
    freq_singles=singletons_fil
    while 1:
        # print(itemset_size)
        # print(freq_singles)
        #freq_singles type:list
        candidates=combinations(freq_singles,itemset_size)
        tuples_count=count_tuple(candidates,baskets)
#         print(tuples_count)
        f=[]
        d={}
        for k,v in tuples_count.items():
            if v>=s:
                d[k]=v
        if len(d)==0:
            break
        for k in d.keys():
            l=list(k)
            f=f+l
        for t in list(d.keys()):
            result.append((t,1))
        result.sort()
        freq_singles=list(set(f))
        itemset_size=itemset_size+1
    return result

def passone(baskets):
    baskets=list(baskets)
    num_bar=len(baskets)
    # print(num_bar)
    result=[]
    s=num_bar*ratio
    #collect local fre singletons
#     singleton=singletons(baskets)#all singletons
    singletons_count=count_singletons([],baskets)
    singletons_fil=filt_singletons(singletons_count,s)
    for i in singletons_fil:
        result.append((i,1))
    #collect local fre multi tuples
    multi_fil=fil_multi(singletons_fil,s,baskets)
    result=result+multi_fil
    return result

def passtwo(baskets):
    baskets=list(baskets)
    counts = {}
    for i in candidate_sets1:
        counts[i[0]] = 0
    for basket in baskets:
        for k in counts.keys():
            if type(k)==int:
                if k in basket:
                    counts[k]+=1
            else:
                if set(k).issubset(set(basket)):
                    counts[k]+=1
    return counts.items()

def sort(i):
    l1=[]
    try:
        e=int(i)
        l1.append(e)
    except:
        pass
    l1.sort()
    return l1

if __name__ == "__main__":
    sc = SparkContext()
    lines=sc.textFile(sys.argv[1]).collect()
    lines=sc.parallelize(lines)
    lines_to_list=lines.map(lambda l: l.split(','))\
    .map(lambda l: [int(i) for i in l])
    b=sc.parallelize(lines_to_list.collect(),2)
    baskets=b.collect()
    ratio=float(sys.argv[2])
    filename=sys.argv[3]
    singleton=singletons(baskets)
    singletons_count=count_singletons([],baskets)
    entire_s=int(len(baskets)*ratio)
    # print(entire_s)22904
    # print(len(baskets))114520
    singletons_fil=filt_singletons(singletons_count,entire_s)
    #passone mapPartitions
    candidate_sets1 = b.mapPartitions(passone).reduceByKey(lambda x,y: 1).collect()
    # print(len(candidate_sets1)) 351
    #passtwo mapPartitions
    result2=b.mapPartitions(passtwo).reduceByKey(lambda x,y: x + y)\
    .filter(lambda x: x[1] >= entire_s).map(lambda x: x[0])
    result2_list=result2.collect()
    # print(len(result2_list)) 345
    l1=[]
    l2=[]
    for i in result2_list:
        if type(i)==int:
            l1.append(i)
        else:
            i=list(i)
            l2.append(i)
        l1.sort()
        l2.sort()
    l=l1+l2
    output=sc.parallelize(l)
    outsingle=output.map(sort).filter(lambda x:len(x)>0)
    single=outsingle.collect()
    # print(single)[[1], [2], [3], [4], [5], [6], [7], [8], [9], [13], [14], [15], [16], [17], [18]]
    allresult=single+l2
    final=sc.parallelize(allresult)
    finalresult=final.sortBy(lambda x:len(x)).collect()
    with open(sys.argv[3],'w') as f:
        for i in finalresult:
            if len(i)==1:
                f.write(str(i[0])+'\n')
            else:
                s= ','.join(str(e) for e in i)
                f.write('('+s+')'+'\n')
