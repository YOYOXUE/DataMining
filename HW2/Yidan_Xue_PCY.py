from pyspark import SparkContext
import sys
import os
#generate all singletons before filt fre
def singletons(baskets):
    count={}
    for i in baskets:
        for j in i:
            count[j] = count.get(j,0) + 1
    k=list(count.keys())
    k.sort()
    return k
#singleton=singletons(baskets)
# print(singletons(baskets))   [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

#count all singletons
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
#[[1, 114520], [2, 103104], [3, 91616], [4, 79953], [5, 68692], [6, 57107], [7, 45765],
#[8, 34211], [9, 22927], [10, 11505], [11, 11487], [12, 17112], [13, 28511], [14, 97213],
#[15, 74200], [16, 51478], [17, 40375], [18, 28527], [19, 17047], [20, 5780]]

def has(pairs,a,b,bucket_size):
    buckets={}
    for p in pairs:
        i=p[0]
        j=p[1]
        bucket_no=(a*i +b*j) % bucket_size
        buckets.setdefault(bucket_no, [])
        buckets[bucket_no].append(p)
    return buckets
#buckets=has(pairs,11,29,150)
#print(buckets){69: [[1, 2], [5, 16], [16, 17]], 98: [[1, 3], [5, 17], [16, 18]], ...}

#generate all pairs
def generate_pair(items):
    items1=items[1:]
    pair=[]
    for i in items[:-1]:
        for j in items1:
            pair.append([i,j])
        items1=items1[1:]
    return pair
# pairs=generate_pair(singleton)
# buckets=has(pairs,11,29,150)
# print(buckets){69: [[1, 2], [5, 16], [16, 17]], 98: [[1, 3], [5, 17], [16, 18]], ...}
def count_pairs(buckets, baskets):
    # add count to each pair in-place
    itemset_size = len(pairs[0])
    pair_count=[]
    for k in buckets.keys():
        count = 0
        for v in buckets[k]:
            for basket in baskets:
                if set(v).issubset(set(basket)):
                    count+=1
        pair_count.append([k,count])
    return pair_count
# pairs_num=count_pairs(buckets,baskets)
#[[69, 151933], [98, 128544], ...
def bitmap(pairs_num,s):
    l=[]
    infre_bitlist=[]
    for i in pairs_num:
        if i[1]>=s:
            l.append([i[0],1])
        else:
            infre_bitlist.append([i[0],0])
    return l,infre_bitlist
# bitlist=bitmap(pairs_num,2500)
# print(len(bitlist)) 130
#[[69, 1], [98, 1], [127, 1], [6, 1], [35, 1], ...]

def filt_pairs(bitlist,s,baskets):
    p=[]
    cand=[]
    for i in bitlist:
        for value in buckets[i[0]]:
            c=0
            for basket in baskets:
                if set(value).issubset(set(basket)):
                    c+=1
            if c>=s:
                p.append(value)
            else:
                cand.append(value)
    p.sort()
    cand.sort()
    return p,cand
# pairs_f,cand=filt_pairs(bitlist,s,baskets)
'''print(pairs_f)
    [[1, 2], [1, 3], [1, 4], [1, 5], [1, 6], [1, 7], [1, 8], [1, 9],
    [1, 10], [1, 11], [1, 12], [1, 13], [1, 14], [1, 15],...]
   print(len(pairs_f))
    173 fre pairs
   print(cand)
    [[7, 20], [8, 20], [9, 20], [12, 20], [13, 20], [17, 20], [18, 20], [19, 20]]'''

def filt_singletons(itemsets,s):
    single = []
    for items in itemsets:
        if items[1]>=s:
            single.append(items[0])
    single.sort()
    return single
# s1=filt_singletons(singletons_count,2500)
# print(s1)
def candidate(infre_bitlist,s,baskets,buckets,single_f):
    cand=[]
    for i in infre_bitlist:
        for value in buckets[i[0]]:
            if set(value).issubset(set(single_f)):
                cand.append(value)
    cand.sort()
    return cand
#print(candidates=candidate(infre_bitlist,s,baskets,buckets))
# [[9, 10], [9, 11], [10, 11], [10, 12], [10, 19], [10, 20], [11, 12], [11, 19], [11, 20]]
if __name__ == "__main__":
    sc = SparkContext()
    lines=sc.textFile(sys.argv[1]).collect()
    lines=sc.parallelize(lines)
    lines_to_list=lines.map(lambda l: l.split(','))\
    .map(lambda l: [int(i) for i in l])
    baskets=lines_to_list.collect()
    a=int(sys.argv[2])
    b=int(sys.argv[3])
    bucket_size=int(sys.argv[4])
    s=float(sys.argv[5])
    newdir=sys.argv[6]
    #get all singletons in baskets before filt
    singleton=singletons(baskets)
    singletons_count=count_singletons([],baskets)#count each singleton
    pairs=generate_pair(singleton)
    buckets=has(pairs,a,b,bucket_size)
    pairs_num=count_pairs(buckets,baskets)
    bitlist,infre_bitlist=bitmap(pairs_num,s)
    pairs_f,cand=filt_pairs(bitlist,s,baskets)
    single_f=filt_singletons(singletons_count,s)
    candidates=candidate(infre_bitlist,s,baskets,buckets,single_f)
    result=single_f+pairs_f
    # result.sort()
    print 'False Positive Rate:',"%.3f" % (len(bitlist)/float(bucket_size))
    if not os.path.exists(newdir):
        os.makedirs(newdir)

    with open(newdir+'/'+'frequentset.txt','w') as f:
        for i in result :
            if type(i)==int:
                f.write(str(i)+"\n")
            else:
                s= ','.join(str(e) for e in i)
                f.write('('+s+')'+'\n')

    with open(newdir+'/'+'candidates.txt','w') as ca:
        for i in candidates:
            s= ','.join(str(e) for e in i)
            ca.write('('+s+')'+'\n')
