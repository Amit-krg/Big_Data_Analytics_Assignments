#Name:Amit Gupta
#ID:110900982
from pyspark import SparkContext
from pyspark.conf import SparkConf
import sys
import numpy

from pprint import pprint


#user item support
def minItemSupport(item_rated):
    if len(item_rated[1]) >= 25:
        return list(item_rated[1])
    return list()

#item user support
def minUser(userItems):
    if len(userItems[1]) >= 10:
        return list(userItems[1])
    return list()

#user ratings
def ratings_user(userRatings):
    d = dict()
    #d=set()     
    for item in userRatings[1]:
        if item[1] in d:
            if item[3] > d[item[1]][0]:
                d[item[1]] = (item[3],float(item[2]))
        else:
            d[item[1]] = (item[3],float(item[2]))

    rating_list = list()
    for item in d:
        rating_list.append([userRatings[0], item, d[item][1]])
    return rating_list


#Similarity calculation
def similarity(data, item_target):
    item_user = data[1]
    user_target = item_target[1]
    if len(user_target)==0:
        return (data[0],data[1],0.0)

    i=0
    j=0
    num = 0
    sum_sq = 0; target_sq = 0
    while i<len(item_user) and j<len(user_target):
        if item_user[i][0]==user_target[j][0]:
            num += item_user[i][2]*user_target[j][2]
            i +=1;j+=1
        elif item_user[i]>user_target[j]:
            j+=1
        else:
            i+=1


    for i in item_user:
        sum_sq += i[2]**2
    for j in user_target:
        target_sq += j[2]**2

    denom = (sum_sq**0.5) * (target_sq**0.5)

    sim = num/denom
    return (data[0],data[1],sim)


#Removing elements with no deviation from the mean
def mean_ratings(item_rating):

    if numpy.std(list(e[2] for e in item_rating[1]))==0.0:
        return None

    sum_rating = sum(e[2] for e in item_rating[1])
    avg = sum_rating/len(item_rating[1])

    users_sorted = sorted(item_rating[1], key=lambda  x:x[0])
    user_ratelist = list()
    for e in users_sorted:
        ratings_norm = round((e[2] - avg),2)
        user_ratelist.append((e[0],e[2],ratings_norm))
    return (item_rating[0], user_ratelist)

def calculateRating(res,final_user):
    rating_list=[]
    for user in final_user:
        count = 0
        num = 0
        denom = 0
        for it in res:
            item_rated = it[1]
            rep_user = [i for i in item_rated if i[0]==user]
            if len(rep_user)>0 and it[2]>0.00:
                num += (it[2] * rep_user[0][1])
                denom +=it[2]
                count += 1
            if count >= 50:
                break
        if count>1:
            rating = num/denom
            rating = round(rating,2)
            rating_list.append((user, rating))
    return rating_list


if __name__=='__main__':
    #hdfs:/ratings/ratings_Video_Games.csv.gz
    file = "ratings_Amazon_Instant_Video.csv"
    sc = SparkContext(appName="recommendation")

    data = sc.textFile(file).map(lambda x: x.split(",")).groupBy(lambda x: x[0]).map(ratings_user).flatMap(lambda x:x)

    itembasket = data.groupBy(lambda x:x[1]).map(minItemSupport).filter(lambda x:len(x)>0).flatMap(lambda x:x)

    min_user_item = itembasket.groupBy(lambda x:x[0]).map(minUser).filter(lambda x:len(x)>0).flatMap(lambda x:x)

    utility = min_user_item.groupBy(lambda x:x[1]).map(mean_ratings).filter(lambda x:x!=None)
    
    #for video games
    item_target = utility.filter(lambda x:x[0]=="B000035Y4P").collect()[0]
    #item_target = utility.filter(lambda x:x[0]=="B000035Y4P").collect()
    print(item_target)
    similarity = utility.map(lambda x: similarity(x,item_target)).sortBy(lambda x:x[2],False)
    res = similarity.collect()

    final_user = min_user_item.map(lambda x:x[0]).distinct().sortBy(lambda x:x).collect()
    rating_list= list()
    rating_list=calculateRating(res,finaluser)
   

    out = sc.parallelize(rating_list).map(lambda x:x).map(lambda x: (str(x[0]),x[1])).collect()
    pprint(out)
