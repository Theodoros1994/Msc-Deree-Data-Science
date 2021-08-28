# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 19:54:16 2020

@author: 30694
"""



# ==== Apache Spark Queries ===
from datetime import datetime    
from pyspark.sql import SparkSession
import sys
from pyspark import SparkContext,SparkConf
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import pandas as pd


df = pd.read_csv("C:\\Users\\30694\\Downloads\\Churn_Modelling.csv")

def kill_current_spark_context():
    SparkContext.getOrCreate().stop()

kill_current_spark_context()
sc = SparkContext('local[*]', 'FinalProject')

spark=SparkSession.builder.appName("spark sql test2").getOrCreate()

spark_df = spark.createDataFrame(df)
   
spark_df.registerTempTable("Table")


# Query No# 1
start=datetime.now()
query_result=spark.sql("SELECT COUNT(CustomerID) AS  ExitedCustomers FROM Table WHERE Exited=1 ").show()

# query_result.show()

print ('The time for Query No#1 is %s' % (datetime.now()-start))

# Query No# 2
start=datetime.now()
query_result=spark.sql("SELECT AVG(EstimatedSalary) As Average_Estimated_Customer_Salary,MIN(EstimatedSalary) As Minimum_Estimated_Customer_Salary ,MAX(EstimatedSalary) As Maximum_Estimated_Customer_Salary  FROM Table")


print ('The time for Query No#2 is %s' % (datetime.now()-start))

# Query No# 3
start=datetime.now()
query_result=spark.sql("SELECT COUNT(CustomerID) FROM Table WHERE Exited=1 AND IsActiveMember=1 ")

print ('The time for Query No#3 is %s' % (datetime.now()-start))


# Query No# 4
start=datetime.now()
query_result=spark.sql("SELECT  (COUNT(CustomerID)*100/(SELECT COUNT(*) FROM Table ))  AS Total_Male_Percentage FROM Table WHERE Gender='Male'")

print ('The time for Query No#4 is %s' % (datetime.now()-start))

# Query No# 5
start=datetime.now()
query_result=spark.sql("SELECT COUNT (CustomerId) AS COUNTERSUM FROM Table GROUP BY Age,CustomerId HAVING Age BETWEEN 20 AND 40 ")

print ('The time for Query No#5 is %s' % (datetime.now()-start))

# Query No# 6
start=datetime.now()
query_result=spark.sql("SELECT COUNT(CustomerId), Geography  FROM Table  WHERE Geography='Spain' or Geography='France' GROUP BY Geography ")

print ('The time for Query No#6 is %s' % (datetime.now()-start))

# Query No# 7
start=datetime.now()
query_result=spark.sql("SELECT CustomerId, Surname from Table  WHERE NOT ( HasCrCard=0 AND IsActiveMember=0) AND (Age<40)GROUP BY Surname,CustomerId ORDER BY Surname DESC ")

print ('The time for Query No#7 is %s' % (datetime.now()-start))



# Query No# 8
start=datetime.now()
query_result=spark.sql("SELECT CustomerId,Surname, Geography FROM Table WHERE Surname LIKE  'A%' AND (Geography IN('Germany','France'))GROUP BY CustomerId,Surname,Geography ORDER BY CustomerId, Surname DESC,Geography DESC")

print ('The time for Query No#8 is %s' % (datetime.now()-start))

# Query No# 9
start=datetime.now()
query_result=spark.sql("SELECT CustomerId,Surname FROM Table WHERE Tenure IN ('2' ,'4' ,'6' ,'8') AND Geography IN('Spain' ,'Germany') AND AGE IN ( '20', '30','40') GROUP BY CustomerId,Surname,Tenure,Geography,Age ORDER BY CustomerId,Surname,Tenure,Geography,Age ")

print ('The time for Query No#9 is %s' % (datetime.now()-start))



# Query No# 10
start=datetime.now()
query_result=spark.sql("SELECT COUNT(CustomerId) FROM Table  WHERE (Gender='Male' AND Geography='Spain') OR(CreditScore>500)")

print ('The time for Query No#10 is %s' % (datetime.now()-start))

# Query No# 11
start=datetime.now()
query_result=spark.sql("SELECT CustomerId,NumOfProducts FROM Table  WHERE (Balance>150000 AND Age>30) OR ( NumOfProducts  IN( '3' ,'4')) GROUP BY CustomerId,NumOfProducts ")

print ('The time for Query No#11 is %s' % (datetime.now()-start))



# Query No# 12
start=datetime.now()
query_result=spark.sql("SELECT Age,CreditScore FROM Table  WHERE CreditScore>500 GROUP BY Age,CreditScore  HAVING Age>(Select AVG(Age) FROM Table)")

print ('The time for Query No#12 is %s' % (datetime.now()-start))


# Query No# 13
start=datetime.now()
query_result=spark.sql(""" SELECT MIN (Balance) AS Minimum_Balance,MAX (Balance) AS Maximum_Balance,   AVG (Balance) AS Average_Balance,
 
  MIN (Age) AS Minimum_Age,MAX (Age) AS Maximum_Age,   AVG (Age) AS Average_Age  ,
 
  MIN (CreditScore) AS Minimum_CreditScore,MAX (CreditScore) AS Maximum_CreditScore,   AVG (CreditScore) AS Average_CreditScore  ,
 
  MIN (Tenure) AS Minimum_Tenure,MAX (Tenure) AS Maximum_Tenure,   AVG (Tenure) AS Average_Tenure                
    FROM Table GROUP BY Balance,Age,CreditScore,Tenure""")
print ('The time for Query No#13 is %s' % (datetime.now()-start))

# Query No# 14
start=datetime.now()
query_result=spark.sql("SELECT Geography,Balance,sum(Balance ) OVER(PARTITION BY Geography ) AS TotalBalPerRegion FROM Table GROUP BY Geography, Balance")

print ('The time for Query No#14 is %s' % (datetime.now()-start))






# Query No# 15
start=datetime.now()
query_result=spark.sql("SELECT CustomerId,EstimatedSalary FROM Table GROUP BY CustomerId,EstimatedSalary  HAVING EstimatedSalary >(SELECT  AVG(EstimatedSalary ) FROM Table )")

print ('The time for Query No#15 is %s' % (datetime.now()-start))




# Query No# 16
start=datetime.now()
query_result=spark.sql("SELECT CustomerId, Surname FROM Table GROUP BY CustomerId,Surname,Balance,CreditScore HAVING Balance >(SELECT  AVG(Balance )FROM Table ) AND CreditScore >(SELECT  AVG(CreditScore )FROM Table )")

print ('The time for Query No#16 is %s' % (datetime.now()-start))


# Query No# 17
start=datetime.now()
query_result=spark.sql("SELECT CustomerId, Tenure, count(*) FROM Table GROUP BY GROUPING SETS (  ( CustomerId),  (Tenure))")

print ('The time for Query No#17 is %s' % (datetime.now()-start))





# Query No# 18
start=datetime.now()
query_result=spark.sql("""SELECT CustomerId, 
        SUM(Balance+EstimatedSalary) as TotalMoney FROM Table
     
GROUP BY CustomerId
  HAVING     SUM(Balance+EstimatedSalary)  > 100000
ORDER BY   TotalMoney DESC""")

print ('The time for Query No#18 is %s' % (datetime.now()-start))

# Query No# 19
start=datetime.now()
query_result=spark.sql("""select Tenure, count(*) as total_tenure,
        count(*) * 1.0 / sum(count(*)) over () as ratio
from Table
group by Tenure """)

print ('The time for Query No#19 is %s' % (datetime.now()-start))


# Query No# 20
start=datetime.now()
query_result=spark.sql("SELECT CustomerId FROM Table WHERE CreditScore> (SELECT AVG(CreditScore)  FROM Table  )")

# print(query_result)
# for row in query_result:
#     print(row)

print ('The time for Query No#20 is %s' % (datetime.now()-start))