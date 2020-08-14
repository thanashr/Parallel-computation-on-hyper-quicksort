import numpy as np
from mpi4py import MPI
import time
import random
import math
def Rand(begin, end, num):
    res = []
    for j in range(num):
        res.append(random.randint(begin, end))
    return res
start = time.time()
num = 102400
num_p = 4
begin =0
end = 7000
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
rank_arr = np.arange(num_p)
if rank == 0:
        data =[]
        arr = Rand(begin,end,num)
        arr = np.array(arr)
        print("----Initial array at node 0 is:",arr)
        data  = np.split(arr , num_p)
        arr=data[0]
        print("data at 0",arr)
        for i in range(1,len(rank_arr)):
                comm.send(data[i], dest = i , tag =1)
if (rank!=0):
   arr = comm.recv(source = 0 , tag =1)
   print("rank at",rank,"data",arr)
def sorting(arr):
   return (sorted(arr))
def splitting (sorted_list, median):
    low=[]
    up = []
    for i in sorted_list:
        if(i<median):
           low.append(i)
        else:
           up.append(i)
    return low,up
def assigning_in_hash (rankarray):
    start=0
    hashmap = {}
    next= int(len(rankarray)/2)
    for i in range(start,next):
        hashmap[rankarray[i]]= rankarray[i+next]
    return hashmap
def repitition(arr,ranklist):                
   #sorted_list = sorting(arr)
   start_node = ranklist[0]
   if rank ==start_node:
          #med1 = sorted_list[int((len(sorted_list))/2)]
          med1 = arr[0]
          for j in range (1,len(ranklist)):
              comm.send(med1 , dest = ranklist[j] , tag = 2)  
   else:  
      med1 = comm.recv(source = start_node , tag =2)
   low , up = splitting(arr,med1)
   hash = assigning_in_hash(rank_arr)
   hashkeys = list(hash.keys())
   hashvalues = list(hash.values())
   if rank in hashkeys:
      old = low
      comm.send(up,dest = hash[rank],tag =3)
      new=comm.recv(source= hash[rank], tag = 4)
   else:
      old =up
      comm.send(low, dest =hashkeys[hashvalues.index(rank)], tag = 4)
      new = comm.recv (source = hashkeys[hashvalues.index(rank)] , tag=3)
   arr=old+new
   return arr
arr=repitition(arr,rank_arr)
count=1
iteration = int(math.log(len(rank_arr),2))
while(count< iteration) :
   rank_arr_new  = np.split(rank_arr , 2)
   for item in rank_arr_new:
        if rank in item:
            rank_arr = item
   print("in while at rank",rank,"is:",len(arr),rank_arr)
   arr = repitition (arr , rank_arr)
   print("after fn at rank",rank,"is:",len(arr))
   count = count+1
arr = sorting(arr)
if (rank!=0):
   comm.send(arr , dest=0,tag=5)
else:
   for i in range(1,num_p):
      arr+=(comm.recv(source=i, tag=5))
   print("-----Final Array at node 0 is:",arr)
   end=time.time()
   print("-------Execution Time is", end-start)
