# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 17:49:47 2020

@author: 30694
"""

from datetime import datetime
import mysql.connector

mydb = mysql.connector.connect(user='root', password='Bnft!123',
                              host='127.0.0.1', database='itc6107a1',
                              auth_plugin='mysql_native_password')


mycursor = mydb.cursor()

# Query No# 1
start=datetime.now()
mycursor.execute("SELECT COUNT(CustomerID) AS Number_of_Exited_Customers  FROM churn_modelling WHERE Exited=1 ")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#1 is %s' % (datetime.now()-start))
print("------------------------------------")  



# Query No# 2
start=datetime.now()
mycursor.execute("SELECT AVG(EstimatedSalary) As Average_Estimated_Customer_Salary,MIN(EstimatedSalary) As Minimum_Estimated_Customer_Salary ,MAX(EstimatedSalary) As Maximum_Estimated_Customer_Salary  FROM churn_modelling")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#2 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 3
start=datetime.now()
mycursor.execute("SELECT COUNT(CustomerID) FROM churn_modelling WHERE Exited=1 AND IsActiveMember=1 ")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#3 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 4
start=datetime.now()
mycursor.execute("SELECT  (COUNT(CustomerID)*100/(SELECT COUNT(*) FROM churn_modelling ))  AS Total_Male_Percentage FROM churn_modelling WHERE Gender='Male'")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#4 is %s' % (datetime.now()-start))
print("------------------------------------")  



# Query No# 5
start=datetime.now()
mycursor.execute("SELECT COUNT(CustomerId) AS COUNTERSUM FROM churn_modelling GROUP BY Age,CustomerId HAVING Age BETWEEN 20 AND 40 ")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#5 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 6
start=datetime.now()
mycursor.execute("SELECT COUNT(CustomerId), Geography  FROM churn_modelling  WHERE Geography='Spain' or Geography='France' GROUP BY Geography")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#6 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 7
start=datetime.now()
mycursor.execute("SELECT CustomerId, Surname from churn_modelling  WHERE NOT ( HasCrCard=0 AND IsActiveMember=0) AND (Age<40) GROUP BY Surname,CustomerId ORDER BY Surname DESC")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#7 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 8
start=datetime.now()
mycursor.execute("SELECT CustomerId,Surname, Geography FROM churn_modelling WHERE Surname LIKE  'A%' AND (Geography IN('Germany','France')) GROUP BY CustomerId,Surname,Geography ORDER BY CustomerId, Surname DESC,Geography DESC")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#8 is %s' % (datetime.now()-start))
print("------------------------------------")  




# Query No# 9
start=datetime.now()
mycursor.execute("SELECT CustomerId,Surname FROM churn_modelling WHERE Tenure IN ('2' ,'4' ,'6' ,'8') AND Geography IN('Spain' ,'Germany') AND AGE IN ( '20', '30','40') GROUP BY CustomerId,Surname,Tenure,Geography,Age ORDER BY CustomerId,Surname,Tenure,Geography,Age")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#9 is %s' % (datetime.now()-start))
print("------------------------------------")  

# Query No# 10
start=datetime.now()
mycursor.execute("SELECT COUNT(CustomerId) FROM churn_modelling  WHERE (Gender='Male' AND Geography='Spain') OR(CreditScore>500)")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#10 is %s' % (datetime.now()-start))
print("------------------------------------")  



# Query No# 11
start=datetime.now()
mycursor.execute("SELECT CustomerId,NumOfProducts FROM churn_modelling  WHERE (Balance>150000 AND Age>30) OR ( NumOfProducts  IN( '3' ,'4')) GROUP BY CustomerId,NumOfProducts ")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#11 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 12
start=datetime.now()
mycursor.execute("SELECT Age,CreditScore FROM churn_modelling  WHERE CreditScore>500 GROUP BY Age, CreditScore  HAVING Age>(Select AVG(Age) FROM churn_modelling )")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#12 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 13
start=datetime.now()
mycursor.execute("  SELECT MIN(Balance) AS Minimum_Balance,MAX(Balance) AS Maximum_Balance,   AVG(Balance) AS Average_Balance, MIN(Age) AS Minimum_Age,MAX(Age) AS Maximum_Age,   AVG(Age) AS Average_Age  , MIN(CreditScore) AS Minimum_CreditScore,MAX(CreditScore) AS Maximum_CreditScore,   AVG(CreditScore) AS Average_CreditScore  , MIN(Tenure) AS Minimum_Tenure,MAX(Tenure) AS Maximum_Tenure,   AVG(Tenure) AS Average_Tenure   FROM churn_modelling GROUP BY Balance,Age,CreditScore,Tenure")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#13 is %s' % (datetime.now()-start))
print("------------------------------------")  

# Query No# 14
start=datetime.now()
mycursor.execute("SELECT Geography,Balance,sum(Balance ) OVER(PARTITION BY Geography ) AS TotalBalPerRegion FROM churn_modelling GROUP BY Geography, Balance")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#14 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 15
start=datetime.now()
mycursor.execute("SELECT CustomerId,EstimatedSalary FROM churn_modelling GROUP BY CustomerId,EstimatedSalary  HAVING EstimatedSalary >(SELECT  AVG(EstimatedSalary ) FROM churn_modelling )")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#15 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 16
start=datetime.now()
mycursor.execute("SELECT CustomerId, Surname FROM churn_modelling GROUP BY CustomerId,Surname,Balance,CreditScore HAVING Balance >(SELECT  AVG(Balance )FROM churn_modelling ) AND CreditScore >(SELECT  AVG(CreditScore )FROM churn_modelling )")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#16 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 17
start=datetime.now()
mycursor.execute("SELECT CustomerId, Tenure, count(*) FROM churn_modelling GROUP BY  CustomerId, Tenure WITH ROLLUP")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#17 is %s' % (datetime.now()-start))
print("------------------------------------")  



# Query No# 18
start=datetime.now()
mycursor.execute("SELECT COUNT(CustomerID) AS Number_of_Exited_Customers  FROM churn_modelling WHERE Exited=1 GROUP BY CustomerId")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#18 is %s' % (datetime.now()-start))
print("------------------------------------")  


# Query No# 19
start=datetime.now()
mycursor.execute("SELECT CustomerId,  SUM(Balance+EstimatedSalary) as TotalMoney FROM churn_modelling GROUP BY CustomerId HAVING SUM(Balance+EstimatedSalary)>100000 ORDER BY TotalMoney DESC ")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#19 is %s' % (datetime.now()-start))
print("------------------------------------")  











# Query No# 20
start=datetime.now()
mycursor.execute("SELECT CustomerId FROM churn_modelling WHERE CreditScore> (SELECT AVG(CreditScore)  FROM churn_modelling  )")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
  
print("------------------------------------")  
print ('The time for Query No#20 is %s' % (datetime.now()-start))
print("------------------------------------")  



