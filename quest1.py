from elasticsearch import Elasticsearch
import pandas as pd

#metric
def metric(x):
    return x[0]*0.5 + x[1]*0.5

def search():
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    books= pd.DataFrame() #create an empty dataframe
    
    while books.empty:
        user_title=input("Enter Title: ")
    
        resp= es.search(index='books',body={"query":{ "match": {"book_title": user_title  } },"sort": [ {"_score" : { "order":"desc" }} ]  }    , size=10000)

        books = pd.json_normalize(resp['hits']['hits']) #to take results into flat tables

    scores = books["_score"]
    isbn = books["_source.isbn"]
    books = books.set_index("_source.isbn")  #to kanoyme euretirio grammhs

    results= pd.DataFrame()

    #-----User Rating-----
    search_user_rating = [0]*len(scores)
    while results.empty:
        userId=int(input("Enter Id: "))
        res= es.search(index='ratings',body={"query":{ "match": {"uid": userId  } }})
        results = pd.json_normalize(res['hits']['hits'])

    for i in range(len(isbn)):
        for j in range(len(results["_source.isbn"])):
            if isbn[i] == results["_source.isbn"][j]:
                search_user_rating[i] = results["_source.rating"][j]
    
    for i in range(len(books)):
        books["_score"][i] = metric([scores[i], search_user_rating[i]])

    n = 10
    books = books.sort_values(by=['_score'], ascending=False)
    print("Best Match with user with ID:" ,userId)
    dfFreq = pd.DataFrame(books[["_score", "_source.book_title"]])
    print(dfFreq.head(int(len(dfFreq)*(n/100))))
    #print(dfFreq)


if __name__ == "__main__":
    search()