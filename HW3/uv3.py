import numpy as np
import math
import csv,sys

def Mat(lines,movie_index):
    M=np.zeros(shape=(n,m))
    for l in lines:
        M[l[0]-1][movie_index[l[1]]-1]=l[2]
    return M


def learn(M,U,V):
    for i in range(it):
        # print('iterate:',i+1)
            # training U
        for r in range(n): # r is row index of U
            for s in range(f): # s is col index of U

                tmpsum = 0
                tmpd = 0
                for j in range(m):
                    tmpsum2 = 0
                    if (M[r][j] != 0):
                        tmpsum2=np.dot(U[r,:],V[:,j])-U[r][s] * V[s][j]

#                         for k in range(f):
#                             if k != s:
#                                 tmpsum2 += U[r][k] * V[k][j]
                        tmpsum += V[s][j] * (M[r][j] - tmpsum2)
                        tmpd += V[s][j] * V[s][j]
                U[r][s] = float(tmpsum) / tmpd
    #     print("end U")

        for s in range(m):
            for r in range(f):
                if V[r][s]!=0:
                    tmpsum = 0
                    tmpd = 0
                    for i in range(n):
                        tmpsum2 = 0
                        if (M[i][s] != 0):
#                             for k in range(f):
#                                 if k != r:
#                                     tmpsum2 += U[i][k] * V[k][s]
                            tmpsum2=np.dot(U[i,:],V[:,s])-U[i][r] * V[r][s]
                            tmpsum += U[i][r] * (M[i][s] - tmpsum2)
                            tmpd += U[i][r] * U[i][r]
                V[r][s] = float(tmpsum) / tmpd
    #     print('end V')
        MM=np.dot(U,V)
        error=0
        c=0
        for i in range(n):
            for j in range(m):
                if M[i][j]>0:
                    error+=(M[i][j]-MM[i][j])*(M[i][j]-MM[i][j])
                    c+=1
        error/=float(c)
        error = math.sqrt(error)
        print("%.4f"%error)
if __name__=="__main__":
    movieid=[]
    n=int(sys.argv[2])
    m=int(sys.argv[3])
    f=int(sys.argv[4])
    it=int(sys.argv[5])
    with open(sys.argv[1],'r') as d:
        lines=csv.reader(d)
        lines=list(lines)[1:]
        for l in lines:
            l.pop()
            l[0]=int(l[0])
            l[1]=int(l[1])
            movieid.append(l[1])
            l[2]=float(l[2])
    lines.sort()
    movieid.sort()
    movie_id = list({}.fromkeys(movieid).keys())
    movie_id.sort()
    movie_index={}
    i=1
    for q in movie_id:
        movie_index[q]=i
        i+=1

    M=Mat(lines,movie_index)
    U=np.ones(shape=(n,f))
    V=np.ones(shape=(f,m))
    R=learn(M,U,V)
