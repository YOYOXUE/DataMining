from pyspark import SparkContext
from operator import add
import sys
import re
sc = SparkContext()
lines=sc.textFile(sys.argv[1]).collect()[1:]
lines=sc.parallelize(lines)
# lines=lines.map(lambda x:x.split(','))
# print(lines.take(1))
lines=lines.map(lambda x:x.split(','))
# print(lines.take(2))
#remove spaces,punctations
l1=lines.map(lambda l:list([l[3].lower(),int(l[18])]))
f=l1.filter(lambda line:line[0]!='')
l2=f.map(lambda l:(l[0].replace("'",'').replace("-",''),l[1]))
l3=l2.map(lambda l:(re.sub('[^\w\s]',' ',l[0]),l[1])).map(lambda l:(l[0].strip(),l[1])).map(lambda l:(' '.join(l[0].split()),l[1]))
fil=l3.filter(lambda line:line[0]!='')
# print(l3.take(30))
c=fil.aggregateByKey((0,0),lambda U,v:(U[0]+1,U[1]+v),lambda U1,U2:(U1[0]+U2[0],U1[1]+U2[1]))
#print(c.take(5))
avg=c.map(lambda x:(x[0],(x[1][0],'%.3f' % (float(x[1][1])/x[1][0])))).sortByKey(True)
a=avg.map(lambda y:y[0]+'\t'+str(y[1][0])+'\t'+y[1][1])
a.saveAsTextFile(sys.argv[2])
