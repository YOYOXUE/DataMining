import re,sys,os
from operator import add
# from pyspark.sql import SparkSession
import pyspark
def parse(line):
    parts=re.split(r'\s+',line)
    return parts[0],parts[1]

def parseh(line):
    parts=re.split(r'\s+',line)
    return parts[1],parts[0]

def compute(link,rank):
    for l in link:
        yield (l,rank)


if __name__=="__main__":
    dataset=sys.argv[1]
    itr=int(sys.argv[2])
    output=sys.argv[3]
    sc=pyspark.SparkContext()
    lines=sc.textFile(dataset)
    links=lines.map(lambda line:parse(line)).distinct().groupByKey().cache()
    #('4', <pyspark.resultiterable.ResultIterable at 0x10ddfda90>)
    hubs=links.map(lambda link:(link[0],1.0))
    #ranks.take(5)
    #[('4', 1), ('8', 1), ('9', 1), ('10', 1), ('12', 1)]
    links_h=lines.map(lambda line:parseh(line)).distinct().groupByKey().cache()
    # hubs=links_h.map(lambda link:(link[0],1))
    # ranks_h.take(5)
    # [('8', 1), ('10', 1), ('56', 1), ('130', 1), ('140', 1)]
    for it in range(itr):
    #update auths
        auths=links.join(hubs).flatMap(lambda link_rank:compute(link_rank[1][0],link_rank[1][1])).reduceByKey(add)
        #links.join(ranks).collect()('4', (<pyspark.resultiterable.ResultIterable at 0x10de2cda0>, 1))
    #     ranks=LT.reduceByKey(add)
        maxvalue=auths.map(lambda x:x[1]).reduce(lambda x,y:max(x,y))
        auths=auths.map(lambda x:(x[0],x[1]/maxvalue))
        auths.cache()
        #update hubs
        hubs=links_h.join(auths).flatMap(lambda link_rank:compute(link_rank[1][0],link_rank[1][1])).reduceByKey(add)
        #links_h.join(ranks_h).take(5)('10', (<pyspark.resultiterable.ResultIterable at 0x10dcd72e8>, 1))
    #     ranks=L.reduceByKey(add)
        maxvalue_h=hubs.map(lambda x:x[1]).reduce(lambda x,y:max(x,y))
        hubs=hubs.map(lambda x:(x[0],(x[1]/maxvalue_h)))
        hubs.cache()
    a=auths.map(lambda l:(int(l[0]),"%.5f" % l[1])).sortByKey().collect()
    h=hubs.map(lambda l:(int(l[0]),"%.5f" % l[1])).sortByKey().collect()
    if not os.path.exists(output):
        os.makedirs(output)
    with open(output+'/hub.txt','w') as f:
        for l in h:
            f.write(str(l[0])+','+str(l[1]))
            f.write('\n')
    with open(output+'/authority.txt','w') as fa:
        for l in a:
            fa.write(str(l[0])+','+str(l[1]))
            fa.write('\n')
    spark.stop()
