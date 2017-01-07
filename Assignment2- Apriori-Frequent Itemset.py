#Name:Amit Kumar Gupta
#ID 110900982
from pyspark import SparkContext
from itertools import combinations
from operator import add


def combineThree(lst,user):
	newtemp=[]
	temp=[tuple(set(a[0]+a[1]+a[2])) for a in combinations(lst,3)]
	for item in temp:
		if len(item) ==3:
			newtemp.append((item,user))
	return newtemp		

def combineFour(lst,user):
	newlist=[]
	newtemp=[]
	for (item,count) in lst:
		newlist.append(item)
	temp=[tuple(set(a[0]+a[1]+a[2]+a[3])) for a in combinations(newlist,4)]
	for item in temp:
		if len(item) ==4:
			newtemp.append ((item,user))
	return newtemp		
#Refining k4 list which has items only with confidence >=0.05
def calculateSupport(k3list,k4list):
	flag=0
	templst=k4list
	index=0
	for(k4item,value4) in k4list:
		index+=1
		for (k3item,value3) in k3list:
			if len(set(k4item)-set(k3item))==1:
				if(value4/value3)< 0.05:
					flag=1;
					newitem=k4item
					break #if any of the set doesn't satisfy the confidence criteria
		if flag==1:
			k4list.remove(k4list[index])
			flag=0		
	return 	k4list			

if __name__ == "__main__":
	#hdfs:/ratings/ratings_Video_Games.csv.gz,ratings_Amazon_Instant_Video.csv
	file="ratings_Amazon_Instant_Video.csv"
	sc = SparkContext(appName="Apriori")
	
	#((item)(user1,user2,user3,....))
	data=sc.textFile(file).map(lambda line:line.split(",")).map(lambda s:(s[1],s[0])).distinct().groupByKey().mapValues(lambda x : list(x)).filter(lambda x : len(x[1]) >= 10)

	#(user,[itme1,item2,....])
	finaldata=data.flatMap(lambda s:[(user,s[0]) for user in s[1]]).groupByKey().mapValues(lambda x:list(x)).filter(lambda s:len(s[1])>=5)

	#Pairing for k=2 frequent items ,generate the pair ((item1,item2),user1,user2,....) in the end ti count with min support 10
	k2set=finaldata.flatMap(lambda tup:[(item,tup[0]) for item in combinations(tup[1],2)]).groupByKey().mapValues(lambda x :list(x)).filter(lambda x : len(x[1]) >= 10) 
	print(k2set.count())

	#Bringing back the items in the basket,pairing them as (user,[(item1,item2)....])
	k2basket=k2set.flatMap(lambda s: [(user,s[0]) for user in s[1]]).groupByKey().mapValues(lambda x:list(x)).filter(lambda x: len(x[1])>=3) #1000
	
	#((item1,item2,item3),(user1,user2,....)) and filter with support >10	
	k3set=k2basket.map(lambda s:combineThree(s[1],s[0])).filter(lambda x: len(x)>0).flatMap(lambda s:s).groupByKey().mapValues(lambda x: list(x)).filter(lambda x: len(x[1])>=10).map(lambda s:(s[0],s[1],len(s[1]))) 
	print(k3set.count())
	k3list=k3set.map(lambda x: (x[0],x[2])).collect()

	#pairing for k=3 (user,[(item1,item2,item3),])
	k3basket=k3set.flatMap(lambda s:[(user,(s[0],s[2])) for user in s[1]]).groupByKey().mapValues(lambda x:list(x)) 

	#k4set
	k4set=k3basket.map(lambda s:combineFour(s[1],s[0])).filter(lambda x:len(x)>0).flatMap(lambda s:s).groupByKey().mapValues(lambda x:list(x)).map(lambda s:(s[0],s[1],len(s[1]))) 
	print(k4set.count())
	k4list=k4set.map(lambda x:(x[0],x[2])).collect()
	#print(k4list)
	output=calculateSupport(k3list,k4list)
	#k4basket	
	#k4basket=k4set.flatMap(lambda s:[(user,(s[0],s[2])) for user in s[1]]).groupByKey().mapValues(lambda x:list(x))#420
	#output=k4set.collect()
	#({tuple_of_item_ids_for_I}, item_id_for_j}
	for (item,value) in output:
		print((item[0],item[1],item[2]),{item[3]})
		