# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 16:12:21 2020

@author: 30694
"""
from datetime import datetime    
import pyodbc 
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-4QCKGR44\MSSQLSERVER2012;'
                      'Database=ITC6107a1;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()







#Query No#1
start=datetime.now()
cursor.execute('SELECT COUNT(CustomerId) AS [Number of Exited Customers]  FROM ITC6107A1.dbo.Churn_Modelling$ WHERE Exited=1 ')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#1 is %s' % (datetime.now()-start))
print("------------------------------------")  



#Query No#2
start=datetime.now()
cursor.execute('SELECT AVG(EstimatedSalary) As [Average Estimated Customer Salary],MIN(EstimatedSalary) As [Minimum Estimated Customer Salary] ,MAX(EstimatedSalary) As [Maximum Estimated Customer Salary]  FROM ITC6107A1.dbo.Churn_Modelling$')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#2 is %s' % (datetime.now()-start))
print("------------------------------------")  






#Query No#3
start=datetime.now()
cursor.execute('SELECT COUNT(CustomerID) FROM ITC6107A1.dbo.Churn_Modelling$ WHERE Exited=1 AND IsActiveMember=1 ')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#3 is %s' % (datetime.now()-start))
print("------------------------------------")  





#Query No#4
start=datetime.now()
cursor.execute(""" SELECT  (COUNT(CustomerID)*100/(SELECT COUNT(*) FROM ITC6107A1.dbo.Churn_Modelling$ ))  AS [Total Male Percentage] FROM ITC6107A1.dbo.Churn_Modelling$ WHERE Gender='Male'""")

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#4 is %s' % (datetime.now()-start))
print("------------------------------------")  




#Query No#5
start=datetime.now()
cursor.execute('SELECT COUNT (CustomerId) AS [COUNTERSUM] FROM ITC6107A1.dbo.Churn_Modelling$ GROUP BY Age,CustomerId HAVING Age BETWEEN 20 AND 40 ')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#5 is %s' % (datetime.now()-start))
print("------------------------------------")  







#Query No#6
start=datetime.now()
cursor.execute("""SELECT COUNT(CustomerId), Geography  FROM ITC6107A1.dbo.Churn_Modelling$  WHERE Geography='Spain' or Geography='France' GROUP BY Geography""")

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#6 is %s' % (datetime.now()-start))
print("------------------------------------")  




#Query No#7
start=datetime.now()
cursor.execute('SELECT CustomerId, Surname from ITC6107A1.dbo.Churn_Modelling$  WHERE NOT ( HasCrCard=0 AND IsActiveMember=0) AND (Age<40) GROUP BY Surname,CustomerId ORDER BY Surname DESC')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#7 is %s' % (datetime.now()-start))
print("------------------------------------")  




#Query No#8
start=datetime.now()
cursor.execute("""SELECT CustomerId,Surname, Geography FROM ITC6107A1.dbo.Churn_Modelling$ WHERE Surname LIKE  'A%' AND (Geography IN('Germany','France')) GROUP BY CustomerId,Surname,Geography ORDER BY CustomerId, Surname DESC,Geography DESC""")

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#8 is %s' % (datetime.now()-start))
print("------------------------------------")  






#Query No#9
start=datetime.now()
cursor.execute(""" SELECT CustomerId,Surname FROM ITC6107A1.dbo.Churn_Modelling$ WHERE Tenure IN ('2' ,'4' ,'6' ,'8') AND Geography IN('Spain' ,'Germany') AND AGE IN ( '20', '30','40') GROUP BY CustomerId,Surname,Tenure,Geography,Age ORDER BY CustomerId,Surname,Tenure,Geography,Age""")
               

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#9 is %s' % (datetime.now()-start))
print("------------------------------------")  




#Query No#10
start=datetime.now()
cursor.execute("""SELECT COUNT(CustomerId) FROM ITC6107A1.dbo.Churn_Modelling$  WHERE (Gender='Male' AND Geography='Spain') OR(CreditScore>500)""")

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#10 is %s' % (datetime.now()-start))
print("------------------------------------")  






#Query No#11
start=datetime.now()
cursor.execute(""" SELECT CustomerId,NumOfProducts FROM ITC6107A1.dbo.Churn_Modelling$  WHERE (Balance>150000 AND Age>30) OR ( NumOfProducts  IN( '3' ,'4')) GROUP BY CustomerId,NumOfProducts  """)

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#11 is %s' % (datetime.now()-start))
print("------------------------------------")  



#Query No#12
start=datetime.now()
cursor.execute('SELECT Age,CreditScore FROM ITC6107A1.dbo.Churn_Modelling$  WHERE CreditScore>500 GROUP BY Age, CreditScore  HAVING Age>(Select AVG(Age) FROM ITC6107A1.dbo.Churn_Modelling$ )')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#12 is %s' % (datetime.now()-start))
print("------------------------------------")  







#Query No#13
start=datetime.now()
cursor.execute('SELECT MIN (Balance) AS [Minimum Balance],MAX (Balance) AS [Maximum Balance],   AVG (Balance) AS [Average Balance], MIN (Age) AS [Minimum Age],MAX (Age) AS [Maximum Age],   AVG (Age) AS [Average Age]  ,MIN (CreditScore) AS [Minimum CreditScore],MAX (CreditScore) AS [Maximum CreditScore],   AVG (CreditScore) AS [Average CreditScore]  , MIN (Tenure) AS [Minimum Tenure],MAX (Tenure) AS [Maximum Tenure],   AVG (Tenure) AS [Average Tenure]  FROM ITC6107A1.dbo.Churn_Modelling$ GROUP BY Balance,Age,CreditScore,Tenure')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#13 is %s' % (datetime.now()-start))
print("------------------------------------")  




#Query No#14
start=datetime.now()
cursor.execute('SELECT Geography,Balance,sum(Balance ) OVER(PARTITION BY Geography ) AS TotalBalPerRegion FROM ITC6107A1.dbo.Churn_Modelling$ GROUP BY Geography, Balance')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#14 is %s' % (datetime.now()-start))
print("------------------------------------")  









#Query No#15
start=datetime.now()
cursor.execute('SELECT CustomerId,EstimatedSalary FROM ITC6107A1.dbo.Churn_Modelling$ GROUP BY CustomerId,EstimatedSalary  HAVING EstimatedSalary >(SELECT  AVG(EstimatedSalary ) FROM ITC6107A1.dbo.Churn_Modelling$ )')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#15 is %s' % (datetime.now()-start))
print("------------------------------------")  







#Query No#16
start=datetime.now()
cursor.execute('SELECT CustomerId, Surname FROM ITC6107A1.dbo.Churn_Modelling$ GROUP BY CustomerId,Surname,Balance,CreditScore HAVING Balance >(SELECT  AVG(Balance )FROM ITC6107A1.dbo.Churn_Modelling$ ) AND CreditScore >(SELECT  AVG(CreditScore )FROM ITC6107A1.dbo.Churn_Modelling$ )')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#16 is %s' % (datetime.now()-start))
print("------------------------------------")  





#Query No#17
start=datetime.now()
cursor.execute('SELECT CustomerId, Tenure, count(*) FROM ITC6107A1.dbo.Churn_Modelling$ GROUP BY GROUPING SETS (   ( CustomerId),  (Tenure))')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#17 is %s' % (datetime.now()-start))
print("------------------------------------")  





#Query No#18
start=datetime.now()
cursor.execute('SELECT CustomerId,  SUM(Balance+EstimatedSalary) as [TotalMoney] FROM ITC6107A1.dbo.Churn_Modelling$ GROUP BY CustomerId HAVING SUM(Balance+EstimatedSalary)> 100000 ORDER BY TotalMoney DESC;')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#18 is %s' % (datetime.now()-start))
print("------------------------------------")  





#Query No#19
start=datetime.now()
cursor.execute('select [Tenure], count(*) as total_tenure,  count(*) * 1.0 / sum(count(*)) over () as ratio from ITC6107A1.dbo.Churn_Modelling$ group by [Tenure] ;')

for row in cursor:
    print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#19 is %s' % (datetime.now()-start))
print("------------------------------------")  







#Query No#20
start=datetime.now()
cursor.execute('SELECT CustomerId FROM ITC6107A1.dbo.Churn_Modelling$ WHERE CreditScore> (SELECT AVG(CreditScore)  FROM ITC6107A1.dbo.Churn_Modelling$  )')

# for row in cursor:
#     print(row)
          
    
print("------------------------------------")  
print ('The time for Query No#20 is %s' % (datetime.now()-start))
print("------------------------------------")  














    