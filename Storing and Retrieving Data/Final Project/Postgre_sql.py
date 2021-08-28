# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 16:13:20 2020

@author: 30694
"""

import psycopg2
from datetime import datetime  
connection = psycopg2.connect(database="ITC6107A1", user="sysadmin", password="Bnft!123", host="127.0.0.1", port=5432)




#Query No#1
start=datetime.now()

c = connection.cursor()
c.execute('SELECT COUNT(customerid) AS Number_of_exited_Customers  FROM churn_modeling WHERE exited=1 ') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#1 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()





#Query No#2
start=datetime.now()

c = connection.cursor()
c.execute('SELECT AVG(estimatedsalary) As Average_Estimated_Customer_Salary,MIN(estimatedsalary) As Minimum_Estimated_Customer_Salary ,MAX(estimatedsalary) As Maximum_Estimated_Customer_Salary  FROM churn_modeling') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#2 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()







#Query No#3
start=datetime.now()

c = connection.cursor()
c.execute('SELECT COUNT(customerid) FROM churn_modeling WHERE exited=1 AND isactivemember=1 ') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#3 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()






#Query No#4
start=datetime.now()

c = connection.cursor()
c.execute("SELECT  (COUNT(customerid)*100/(SELECT COUNT(*) FROM churn_modeling ))  AS Total_Male_Percentage FROM churn_modeling WHERE gender='Male' ") # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#4 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()





#Query No#5
start=datetime.now()

c = connection.cursor()
c.execute('SELECT COUNT (customerid) AS COUNTERSUM FROM churn_modeling GROUP BY age,customerid HAVING age BETWEEN 20 AND 40 ') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#5 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()





#Query No#6
start=datetime.now()

c = connection.cursor()
c.execute("SELECT COUNT(customerid), geography  FROM churn_modeling  WHERE geography='Spain' or geography='France' GROUP BY geography ") # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#6 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()




#Query No#7
start=datetime.now()

c = connection.cursor()
c.execute('SELECT customerid, surname from churn_modeling  WHERE NOT ( hascrcard=0 AND isactivemember=0) AND (age<40) GROUP BY surname,customerid ORDER BY surname DESC') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#7 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()




#Query No#8
start=datetime.now()

c = connection.cursor()
c.execute("SELECT customerid,surname, geography FROM churn_modeling WHERE surname LIKE  'A%' AND (geography IN('Germany','France')) GROUP BY customerid,surname,geography ORDER BY customerid, surname DESC,geography DESC") # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#8 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()




#Query No#9
start=datetime.now()

c = connection.cursor()
c.execute("SELECT customerid,surname FROM churn_modeling WHERE tenure IN ('2' ,'4' ,'6' ,'8') AND geography IN('Spain' ,'Germany') AND age IN ( '20', '30','40') GROUP BY customerid,surname,tenure,geography,age ORDER BY customerid,surname,tenure,geography,age") # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#9 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()




#Query No#10
start=datetime.now()

c = connection.cursor()
c.execute("SELECT COUNT(customerid) FROM churn_modeling  WHERE (gender='Male' AND geography='Spain') OR(creditscore>500)") # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#10 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



#Query No#11
start=datetime.now()

c = connection.cursor()
c.execute("SELECT customerid,numofproducts FROM churn_modeling  WHERE (Balance>150000 AND age>30) OR ( numofproducts  IN( '3' ,'4')) GROUP BY customerid,numofproducts ") # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#11 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



#Query No#12
start=datetime.now()

c = connection.cursor()
c.execute('SELECT age,creditscore FROM churn_modeling  WHERE creditscore>500 GROUP BY age, creditscore  HAVING age>(Select AVG(age) FROM churn_modeling )') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#12 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



#Query No#13
start=datetime.now()

c = connection.cursor()
c.execute('SELECT MIN (Balance) AS Minimum_Balance,MAX (Balance) AS Maximum_Balance,   AVG (Balance) AS Average_Balance, MIN (age) AS Minimum_age,MAX (age) AS Maximum_age,   AVG (age) AS Average_age  , MIN (creditscore) AS Minimum_creditscore,MAX (creditscore) AS Maximum_creditscore,   AVG (creditscore) AS Average_creditscore  , MIN (tenure) AS Minimum_tenure,MAX (tenure) AS Maximum_tenure,   AVG (tenure) AS Average_tenure   FROM churn_modeling GROUP BY Balance,age,creditscore,tenure') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#13 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



#Query No#14
start=datetime.now()

c = connection.cursor()
c.execute('SELECT geography,Balance,sum(Balance ) OVER(PARTITION BY geography ) AS TotalBalPerRegion FROM churn_modeling GROUP BY geography, Balance') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#14 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



#Query No#15
start=datetime.now()

c = connection.cursor()
c.execute('SELECT customerid,estimatedsalary FROM churn_modeling GROUP BY customerid,estimatedsalary  HAVING estimatedsalary >(SELECT  AVG(estimatedsalary ) FROM churn_modeling )') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#15 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()




#Query No#16
start=datetime.now()

c = connection.cursor()
c.execute('SELECT customerid, surname FROM churn_modeling GROUP BY customerid,surname,Balance,creditscore HAVING Balance >(SELECT  AVG(Balance )FROM churn_modeling ) AND creditscore >(SELECT  AVG(creditscore )FROM churn_modeling )') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#16 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()




#Query No#17
start=datetime.now()

c = connection.cursor()
c.execute('SELECT customerid, tenure, count(*) FROM churn_modeling GROUP BY GROUPING SETS (  ( customerid),  (tenure))') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#17 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



#Query No#18
start=datetime.now()

c = connection.cursor()
c.execute('SELECT customerid,  SUM(Balance+estimatedsalary) as TotalMoney FROM churn_modeling GROUP BY customerid HAVING SUM(Balance+estimatedsalary)> 100000 ORDER BY TotalMoney DESC;') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#18 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()




#Query No#19
start=datetime.now()

c = connection.cursor()
c.execute('select tenure, count(*) as total_tenure,   count(*) * 1.0 / sum(count(*)) over () as ratio from churn_modeling group by tenure') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#19 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



#Query No#20
start=datetime.now()

c = connection.cursor()
c.execute('SELECT customerid FROM churn_modeling WHERE creditscore> (SELECT AVG(creditscore)  FROM churn_modeling  )') # use triple quotes if you want to spread your query across multiple lines
for result in c:
    print (result) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.

    
print("------------------------------------")  
print ('The time for Query No#20 is %s' % (datetime.now()-start))
print("------------------------------------")  

c.close()



