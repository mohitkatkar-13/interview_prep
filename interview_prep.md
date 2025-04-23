# Deloitte

1. **Briefly introduce yourself?** 
-

***
2. **Difference between head() and take()?**
- No difference, both return row objects in list type, to ispect rows without using the complete collect method.
- df.head(2) or df.take(2)
- Output -> [Row(movie_id = '1',title='Avengers'),Row(movie_id = '2',title='Frozen')]

***
3. **How to convert array column into list of columns?**
- df.select('*',explode('establishment_types').alias('establishment_type'))

***
4. **How to drop columns if it has null value in it?**
```python 
df1 = df.agg(*[count(c).alias(c) for c in df.columns])

nonNull_cols = [c for c in df.columns if df1[[c]].first()[c]>0]

df = df.select(*nonNull_cols)
```

***
5. **Error in dynamic partition pruning ?**
-

***
6. **What are the read and write mode in spark?**
- Read Mode -> Failfast,DropMalformed, Permissive
- Write Mode -> Append, Overwrite, Ignore, ErrorIfExists

***
7. **How will you keep one column always on top?**
- Create a new Column by giving 1 as US and 2 as rest. 
-  Now use orderBy newly created _column.  
```SQL
select *,
case when country='US' then 1 else 2 end as new_column
from table;

select * from table order by new_column;
```

***
8. **No of occurance in column?**
```sql
select country,count(*) from tbl group by country;
```

***
9. **Age breacket in SQL?**
```SQL
select *,
case 
    when age < 18 then 'child'
    when age >= 18 and age <= 50 then 'Adult'
    else 'Old'
    end as Age_bracket
from tbl;
```

***
10. **How AQE works?**
- Spark Adaptive Query Execution offers following features
    1. Dynamically coalescing shuffle partitions
    2. Dynamically switching join strategies
    3. Dynamically optimizing skew joins

***
11. **Difference between MR and Spark programming?**
- [MR-vs-Spark](https://www.geeksforgeeks.org/difference-between-mapreduce-and-apache-spark/)

- [MR-vs_Spark_Quora](https://www.quora.com/What-are-the-differences-between-MapReduce-and-Spark-When-would-you-choose-to-use-MapReduce-over-Spark)

***
12. **What is check pointiing in Spark?**
- Checkpointing is the act of saving an RDD to disk so that future refrences to this RDD point to those intermidiate partitions on disk rather than recomputing the RDD from its original source. This is similar to caching except that it's not stored in memory, only disk. 
- Once the Dataframe is checkpointed the subsequent reads would be from the Checkpoint location.
- checkpoint saves the RDD to an HDFS file and actually forgets the lineage completely. This allows long lineages to be truncated and the data to be saved reliably in HDFS, which is naturally fault tolerant by replication.

``` 
df.checkpoint()
```

***
13. **What is serializer in Spark?**
- Serialization referes to converting objects into stream of bytes and vice-versa(de-serialization) in an optimal way to transfer it over nodes of network or store it in a file/memory buffer.

- Spark provides two serialization libraries and modes are supported and configured through spark.serializer.property
    - Java Serialization (default)
    - Kyro Serialization (recommended by Spark)

- Spark can also use the Kryo library (version 4) to serialize objects more quickly. Kryo is significantly faster and more compact than Java serialization (often as much as 10x), but does not support all Serializable types and requires you to register the classes you’ll use in the program in advance for best performance.

- You can switch to using Kryo by initializing your job with a SparkConf & calling conf.set(“spark.serializer”,“org.apache.spark.serializer.KryoSerializer”). This setting configures the serializer used for not only shuffling data between worker nodes but also when serializing RDDs to disk. 

***
14. **How to convert 3 row result in one column**
```sql
select id, name,
group_concat(strength) as 'strengths'
from tbl group by 1,2;
```

***
15. **Given two string check whether anagram?**
- Solution - 1
```python
if str1.sort() == str2.sort():
    return True
else:
    return False
```
- Solution 2
```python
dict1 = {}
dict2 = {}

if len(str1) != len(Str2):
    return False

for i in range(len(str1)):
    dict1[str1[i]] = 1 + dict1.get(str1[i],0)

for i in range(len(str2)):
    dict2[str2[i]] = 1 + dict2.get(str2[i],0)

if dict1 == dict2:
    return True

else:
    return False
```

***
16. **Two Sum Problem**
```python
nums=[2,7,11,15] , target = 9

hashMap = {}
for i in range(len(nums)):
    j = k - nums[i]
    if j in hashMap.keys():
        return i,hashMap[j]
    else:
        hashMap[nums[i]] = i
return False
```
***
# Impetus

1. **Breif Introduction**
***

2. **Given a string find out occurance of each character?**
```python
str1 = 'Hello World'
hmap = {}

str1 = str1.lower()

for char in str1:
    if char in hmap:
        hmap[char] += 1
    else:
        hmap[char] = 1

for k,v in hmap.items():
    print(k," ",v) 
```

***
3. **Sort the array and merge it**
```python
arr1 = [7,5,1,10,9]
arr2 = [3,8,2,6,4]

for i in range(len(arr1)):
    for j in range(0,len(arr1)-1):
        if arr1[j] > arr[j+1]:
            arr1[j],arr1[j+1] = arr1[j+1],arr1[j]

for i in range(len(arr2)):
    for j in range(0,len(arr2)-1):
        if arr2[j] > arr2[j+1]:
            arr2[j],arr2[j+1] = arr2[j+1],arr2[j]

lst = [0] * (len(arr1)+len(arr2))
left = 0
right = 0 
index = 0

while (left < len(arr1)) and (right < len(arr2)):
    if arr1[left] < arr2[right]:
        lst[index] = arr1[left]
        left += 1
        index += 1
    else:
        lst[index] = arr2[right]
        right += 1
        index += 1
while left < len(arr1):
    lst[index:] = arr1[left:]
    break

while right < len(arr2):
    lst[index] = arr2[right:]
    break

```

***
4. **How to create a table partititoned on few columns?**
```SQL
CREATE TABLE DB_NAME.TBL_NAME
(
    ID int, NAME string, City string
)
PARTITIONED BY(state string, Month string)
ROW FORMAT DELIMITED FIELDS
TERMINATED BY '|';
```
***
5. **How will you put data into table from hdfs and local into the table.**
- HDFS
    - hdfs dfs -put -f \<table hdfs location\>
    - hadoop fs -put -f \<table hdfs location\>
- File at local
    - LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE
    tablename [PARTITION (partcol1=val1,partcol2=val2..)]
    [INPUTFORMAT 'inputformat' SERDE 'serde']

***
6. **Given a table with one column find the summ of all positive and all negative integers.**
```sql
select sum(id) from tbl where id < 0
union all
select sum(id) from tbl where id > 0;
```

***
7. **Given two tables find the records whicha re not common in both**
```sql
select t1.id from tbl1 t1 left join tbl2 t2
on t1.id = t2.id
where t2.id == NULL
```
```sql
select * from tbl1 where id not in (select id from tbl2)
union all
select * from tbl2 where id not in (Select id from tbl1)
```

***
8. **Read a csv file, inferschema, filter the record where id>100 and write it into a table.**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('test').getOrCreate()

my_schema = structType(
    [structField('id',IntegerType(),True)],
    [structField('name',StringType(),True)],
    [structField('date',TIMESTAMP(),True)]
)

df = spark.read.format('csv')\
            .option('header','true')\
            .option('schema',my_schema)\
            .option('mode','FAILFAST')\
            .load('/home/mohitkatkar/Documents/mohit.txt')

new_df = df.filter(col('id')>100)

new_df.write.format('parquet').saveAsTable('DB_Name.Table_Name')
```

***
9. **PySPark partition by and Bucket by**
```python
#partitionBy() multiple columns
df.write.option('header',True)\
    .partitionBy("State","City")\
    .mode('overwrite')\
    .csv('path')
```

```python
#BucketBy()
df.write.format('parquet')\
    .bucketBy(100,'year','month')\
    .mode('overwrite')\
    .saveAsTable('bucketed_table')
```

****
# Walmart

1. **Briefly Introduce**

***
2. **Do you know API?**
- API is Application programming interface, it is a set of rules and protocols that allows different software applications to communicate with each other. 
- Eg. A weather app uses API to fetch weather data from remote server. 

***
3. **How do you ingest data from source system?**
- Data ingestion is the process of transporting data from one or more sources to a target site for further processing and analysis.
- SFTP : Secure file transfer protocol 
    - It is a way to securelytransfer files between a client and remote server. It uses to SSH to create a secure connection between the client and server to tranfer file. 
    - It is like FTP just secure.
- hdfs -get : Command to retrieve the data from hdfs and copy it to local file system of edge node.  
- Managed FIle Transfer (MFT) : Connect:Direct bridge, facilitates file transfers between a mainframe and edge node.

***
4. **SQL Rank and Desnse Rank**

***
5. **Parenthesis check problem live coding**
```python
def checkParenthesis(exper:str):
    stack = []
    for char in exper:
        if char in ['(','{','[']:
            stack.append(char)
        else:
            if not stack:
                return False
            current_char = stack.pop()
            if current_char == '(':
                if char != ')':
                    return False
            if current_char == '[':
                if char != ']':
                    return False
            if current_char == '{':
                if char != '}':
                    return False
    if stack:
        return False
    return True
```

```python
def checkParenthesis(expr:str):
    stack = []
    hashM = {'}':'{',')':'(',']':'['}

    for char in expr:
        if char in ['(','{','[']:
            stack.append(char)
        
        else:
            if not stack:
                return False
            current_char = stack.pop()

            if hashM[char] != current_char:
                return False
            
    if stack:
        return False
    return True
```

***
6. **Do you know Scala?**
-

***
7. **How to submit Spark Job?**
```shell
spark-submit --master yarn --deploy-mode cluster\
            --num-executors 10 --executors-cores 2\
            --driver-memory 5g --executor-memory 10g\
            --conf "spark.dynamicAllocation.enabled=true"\
            --py-files //edge_node/scipt.py\
            --files //edge_node/files.xyz\
            Your_Python_Script.py argument1 argument2
```

***
8. **What is AQE?**
- AQE stands for Adaptiuve query exceution
    - It is one of the optimiztion techniques that is used to run the process efficiently.
    - AQE is enabled by default in Spark3.0
- How no. of partitions are changed at runtime or how join condition are changed at runtime. 
    - Dynamic Coalescing of Shuffle Partitions: If Spark detects that shuffle data is smaller than expected, it reduces the number of partitions.

    - Repartitioning for Skew Mitigation: If data skew is detected (e.g., some partitions are much larger than others), Spark automatically splits the large partitions.

    - AQE can dynamically optimize join strategies based on the size of data during execution. The commonly used join types in Spark are:

        - Broadcast Join: Best for small datasets. The smaller dataset is broadcasted to all nodes.

***
9. **How do you optimize spark jobs?**
- Broadcast join
- Caching 
- Shuffle partition
- Filter before shuffling

***
10. **Given sorted arr=[3,6,8,12,15] insert 7 at correct index.**
```python
def instert_num(arr,n):
    index = len(arr)
    for i in range(len(arr)):
        if arr[i] > n:
            index = i
            break
    
    if index == len(arr):
        arr = arr[:index] + [n]
    else:
        arr = arr[:index] + [n] + arr[index:]
    
    return arr
```

***
11. **Movie theatre booking system**

- ![Movie_system](/home/mohitkatkar/Pictures/Screenshots/movie.png)

***
12. **Most challenging project and what were the challenges?**
- Data Volume
- Update Path
- Technical challenge
- Business Challenge

***
# Amazon

1. Brief Introduction

***
2. **Find all employee whose salary is more than their manager.**
```sql
select 
t1.EmpName, t1.empId
from tbl t1 inner join tbl t2
on t1.managerId = t2.empId
where t1.salary > t2.salary
```

***
3. **All employees whose salary is more than average salary of dept**
```sql
with cte as (
    select *,
    avg(salary) over(partition by department) as avg_salary
    from geeksforgeeks
)

select * from cte
where salary > avg_salary;
```

***
4. **Find total no of people in US and India (horizonbtal format)**
```sql
with cte as (
select countryId,sum(population) as total_population
from people
group by 1
having countryId = 1,2
)

with cte2 as (
select c.*, ct.total_population from country c 
inner join cte ct
on c.coubntryId = ct.countryId
)

with cte3 as (
select 
case when country_name = 'USA' then total_population else 0 end as USA,
case when country_name = 'INDIA' then total_population else 0 end as INDIA
from cte3
)
select *, USA+INDIA as Total from 
(select sum(USA) as USA, sum(INDIA) as INDIA from cte3) population_df;
```

***
5. **Write a query to return the designation with highest salary in each city**
```sql
with cte (
select empId, EmpName,EmpCity, 
max(salary) over(partiton by EmpCity) as max_salary 
from employee_df
where max_salary = salary
)

select * from 
cte c inner join designation d 
on c.empId = d.empId
```

***
6. **what all file format you have worked on**
- CSV
- ORC
- Parquet
- Avro

***
7. **Why Parquet**
- Define Parquet

***
8. **What makes parquet faster than csv**
- Parquet being columanar based file formart sercing becomes faster as it doesnt have to go row by row to get details of particular column. 

***
9. **Bucketing vs Partitioning**
- Hive Paritioning - is a way to organise large tables into smaller logical tables based on values ofcolumns; 
one logical table partition for each distinct value. In Hive tables are created as directory on HDFS.

- Hive Bucketing - aka clustering , is a technique to split the data into more manageable files, (by specifying the number of buckets to create). The value of the bucketing column will be hashed by a user-defined number into buckets. 

***
10. **Spark optimization techniques**
- Broadcast Join
- Caching
- Shuffle Partition
- Filter before shuffling

***
11. **How broadcasting works in Spark**
- [Broadcasting in Spark](https://www.sparkcodehub.com/broadcasting-dataframes-in-pyspark)

***
12. **What are actions and transformation?**
- Actions in PySpark are operations that start transformation calculation, write data to an external storage system, and return results to the driver programme.

- Narrow Dependency Transformation
    - A transformation that can be performed on a single poartition sand still produce an overall valid result is a narrow dependency. 
    - where() clause is narrow dependancy transformation

- Wide Dependenacy Transformation
    - A transformation that requires data from other partitions to produce correct results. 
    - groupBy(), orderBy, Join, distinct etc are wide dependency transformations.

***

# EPAM

1. Brief Introduction

***
2. **How to create external table from internal/managed table?**
```sql
-- HIVE
ALTER TABLE <table> SET TBLPROPERTIES('EXTERNAL'='TRUE')
```

***
3. **what is vectorization in Hive?**
- Vectorization allows Hive to process a batch of rows together instead of processing one row at a time.

- Vectorized query execution streamlines operations by processing a block of 1024 rows at a time.

- to enable vectorization set this config parameter
    - hive.vectorized.execution.enabled=true

- The current implementation supports only single table read-only queries.
DD queries or DML queries are not supported.

- Supported operators are slection, filter and group by.

- Partitioned tables are supported. 

***
4. **Write a word count program in Spark**
```python
import sys
from pyspark import SparkContext, SparkConf
if __name__ == '__main__':
    # create spark context with necessary config 
    sc = SparkContext('local','PySpark word count example')

    # read data from txt file and split each line into words
    words = sc.textFile("path/file.txt").flatMap(lambda line: line.split(" "))

    # count the occurance of each word
    wordCounts = words.map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)

    # save the count to output
    wordCounts.saveAsTextFile("/path/")
```

***
5. **Explain Anti Join**
- Left_Anti_Join -> This join returns the values from left table which are not common or not mathcing from the joined table / right table.

***
6. **Whta is Projection Pruning and Predicate Pushdown?**
- Projecting pruning is a technique where only the necessary columns are read froma a dataframe/dataset this in return aloows the minization of data tranfer between the filesystem and spark engine. Primarily useful when the data contains too many columns.
    - column filtering - only required column is read.

- Predicate Pushdown is optimization technique that imporves query performnce by applying filter conditions as early as possible in the query execution process. 
    - filtering row wise - eg. in partition by only the required partition will be read.

***
7. **Cache and Persist?**
- Without passing any argument persist() and cahce() are the same , with default settings:
    - when RDD: MEMORY_ONLY
    - when DataSet: MEMORY_AND_DISK

- Difference: Unlike cache(), perist() allows you to pass arguments, inorder to specify the storage level.
    - persist(MEMORY_ONLY)
    - persist(MEMORY_ONLY_SER)
    - persist(MEMORY_AND_DISK)
    - persist(MEMORY_AND_DISK_SER)
    - persist(DISK_ONLY)

***
8. **AQE?**
- Dynamically coalescing shuffle partitions
- Dynamically switiching join strategies
- Dynamically optimizing skew joins

***
9. **What is unresolved relation?**
-

***
10. **How query gets created, list all 4 stages.**
- The first phase is Analysis. In this Phase, the spal SQL engine will read the code and generate an abstract Syntax tree for SQL or DataFrame queries. In this phase code is analyszed and the column names, table or views names or SQL functions are resolved. You might get a runtime error shown as analysis error at this stage if the names don't resolve. 

- Second phase is logical optimization. In this phase, the SQL engine will apply rule based optimization and construct a set of multiple execution plans. Then the catalyst optimizer will use cost-based optimization to assign a cost to each plan. The logical optimization includes standard SQL optimization techniques such as predicate pushdown, projection pruning, Boolean expresion simplification and constant folding. 

- Third phase is physical planning, the SQL engine engine picks the mosty effective logical plan and generates aphysical plan. The physical plan is nbothing but a set of RDD operations, which determines how the plan is going to execute on the Spark cluster. 

- Fourth phase is whole code generation. This phase involves generting efficient Java bytecode to run on each machine. 


***
11. **Pivot Programming**
```python
pivotDf = df.groupBy("product").pivot("Country").sum("Amount")
pivotDf.printSchema()
pivotDf.show(truncate=False)
```

***
12. **Pull the data for may month and find total sales. (To check approach)** 
- There was refund table too, orders table is big while the refund table is small (it is a hint for brodcast join)

- Further questions on Cache for optimization and Hive metastore and internals of it.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, sum as _sum

# Initialize Spark session
spark = SparkSession.builder.appName("MayMonthSales").getOrCreate()

# Example HDFS path
hdfs_path = "hdfs://namenode:9000/user/data/sales_data.csv"

# Load data from HDFS (assuming CSV format)
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Filter for May month (assuming 'date' column in yyyy-MM-dd format)
may_data = df.filter(month(col('date')) == 5)

# Calculate total sales
total_sales = may_data.agg(_sum("sales").alias("Total_May_Sales"))

# Show the result
total_sales.show()
```

```python
# From Hive 
from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("ReadHiveTable") \
    .enableHiveSupport() \
    .getOrCreate()

# Specify full table name (database.table)
full_table_name = "your_database.your_table"

# Read Hive table directly
df = spark.read.table(full_table_name)

# Show the data
df.show()
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, sum as _sum

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("MayMonthSalesFromHive") \
    .enableHiveSupport() \
    .getOrCreate()

# Specify the Hive database and table
database_name = "your_database"
table_name = "your_table"

# Load data from Hive table
df = spark.sql(f"SELECT * FROM {database_name}.{table_name}")

# Filter for May month (assuming 'date' column in yyyy-MM-dd format)
may_data = df.filter(month(col('date')) == 5)

# Calculate total sales
total_sales = may_data.agg(_sum("sales").alias("Total_May_Sales"))

# Show the result
total_sales.show()
```

```python
# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("MayMonthSalesWithRefunds") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the orders and refunds tables
orders_df = spark.sql("SELECT * FROM your_database.orders")
refunds_df = spark.sql("SELECT * FROM your_database.refunds")

# Filter orders for May month (assuming 'order_date' column in yyyy-MM-dd format)
may_orders_df = orders_df.filter(month(col('order_date')) == 5)

# Perform a broadcast join using refunds (since it is small)
final_df = may_orders_df.join(
    broadcast(refunds_df), 
    may_orders_df['order_id'] == refunds_df['order_id'], 
    how='left'
).fillna(0, ['refund_amount'])

# Calculate net sales (Total Sales - Refunds)
net_sales_df = final_df.withColumn('net_sales', col('sales_amount') - col('refund_amount'))

# Find total net sales for May
total_net_sales = net_sales_df.agg(_sum("net_sales").alias("Total_May_Net_Sales"))

# Show the result
total_net_sales.show()

```

***
13. **Two Sum**
- Refer above

***
# HashedIn By Deloitte

1. Brief Introduction

***
2. **Print repestive manager name of employee for respective employee(new column with manager name)**

```sql
select 
t1.*, t2.name as manager_name
from tbl t1 left join tbl t2
on t1.manager_id = t2.id
```

***
3. **Who is manager who is not**

```sql
with cte as (
select distinct t1.id, t2.manager_id
from emp t1 left join emp t2
on t1.id = t2.manager_id
)

select id,
case when manager_id is null then 'No' else 'Yes' end as is_manager
from cte;
```

***
4. **Add new column to the table with average salary for employees by department**

```sql
select *,
avg(salary) over(partition by dept) as avg_salary
from tbl;
```

***
5. **Add new column with highest salary per dept**

```sql
select *,
max(salary) over(partition by dept) as avg_salary
from tbl;
```

***
6. **Check whether palindrome**
- TNT, ABBA, racecar is palindrome

```python
def is_palindrome(strn:str):
    left = 0 
    right = len(strn) - 1

    while left < right:
        if strn[left] == strn[right]:
            left += 1
            right -= 1
        else:
            return False
    
    return True
```

***
7. **"123a!" , it should return "112233aa!!"**

```python
string = "123a!"

def create_duplicate(s:str):
    new_char = ""
    for char in s:
        new_char = new_char + (char * 2)
    return new_char

create_duplicate(string)
```

***
8. **How do you check given no. is prime.**

```python
def is_prime(n:int):
    for i in range(2, n//2):
        if n % i == 0:
            return False
    else:
        return True
```

***
9. **How to process 1Tb of data in Spark**
- Refer Video

***
10. **Designing a datawarehouse problem statement**
- Refer Video

***

# Tiger Analytics

1. Brief Introduction

***
2. **SQL Print only the newest record for each name.**
```sql
with cte as (
select *,
rank() over(partition by id order by date desc) as rnk
from tbl
)

select * from cte where rnk = 1;
```

***
3. **SQL query to club column together group by id**
```sql
select id, name,
group_concat(course seperator ',') 
from tbl 
group by id,name;
```
```python
spark.sql("""
select id, name, 
array_join(collect_list(course),',') as courses
from table group by id, name
""")
```

***
4. **Vice Versa above question**
```sql
select id, name, cs.Value
from table
cross apply STRING_SPLIT(courses, ',') cs
```
```python
spark.sql("""
select id, name, explode(split(courses,",")) as course from table
""")
```

***
5. **Find out average salary of each manager where each manager can have multiple employee working under them**
```python
spark.sql("""
with cte as(
select distinct mngr_id, avg(sal) over(partition by mngr_id) as avg_sal from manager_tbl
)
select m.name as manager_name, c.mngr_id as manager_id, c.avg_sal
from cte c inner join manager_tbl m
on c.mngr_id = m.id
""").show()
```

```python
spark.sql("""
with cte as(
select mngr_id, avg(sal) as avg_sal from manager_tbl
group by mngr_id
)
select m.name as manager_name, c.mngr_id as manager_id, c.avg_sal
from cte c inner join manager_tbl m
on c.mngr_id = m.id
""").show()
```

***
6. **What will be the count of records on ineer join and left join?**

```
tbl1    tbll2 |     
1       1     | inner join : 6
1       1     | left join : 7
2       2     |
0       2     |
```

***
7. **Basic command of Spark**
- df = spark.read.csv("path")
- df.createOrReplaceTempView("table")
- spark.sql("select count(*) from table)
- df.printSchema()

***
8. **Write a word count program in spark**
```python
from pyspark.sql import SparkContext, SparkConf

sc = SparkContext('local','word count program')

file = sc.textFile('path/file.txt')

flat_file = file.flatMap(lambda word, word.split(' '))

flat_file_tuple = flat_file.map(lambda word,(word, 1))

filnal_word_count = flat_file_tuple.reduceByKey(lambda x,y : x+y)

count = filnal_word_count.collect()

count
```

***
# HCL Technologies

1. **Brief Introduction**

***
2. **Explain your project architecture**
- 

***
3. **Difference between below code**
- list1 = [1,2,3,4,5]
- list2 = list1
- list3 =list1.copy()

- Here the list1=list1 is a direct refrence to the list if nay changes are made in list1 same would be reflected in list2 and vice versa. 

- list.copy() create a new copy of the list at a new location so changes made in list1 wont be affecting list3 and vice versa. 

***
4. **Sort dictionary based on values**
```python
my_dict = {'a':17, 'b':18, 'c':16, 'd':2}

sorted_dict = {}

list_of_sorted_keys = sorted(my_dict, key=my_dict.get)

for i in list_of_sorted_keys:
    sorted_dict[i] = my_dict[i]

print(sorted_dict)
```

***
5. **Pyspark code to extract data from csv and create a table on top of it**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csv_to_table").getOrCreate()

df = spark.read.csv("/path/file.csv")

df.createOrReplaceTempView('table')

query = spark.sql('select * from table')

query.show()

df.write.saveAsTable('table')

df.write.format('parquet').mode('overwrite').partitionBy("column_name").saveAsTable('my_table')
```

***
6. **Transformation vs Action**
- Refer Above

***
7. **How to monitor spark jobs?**
- Spark UI

***
8. **Explain project architecture**
-

***
9. **What volumne of data have you worked on?**
- Depends

***
10. **What is the cluster configuration for your project?**
- We had 2TB cluster decicated for development. 
- There were multiple ques in there

***
11. **What is fact table and star schema in Data warehouse?**
- Measurement -> fact table
- Context -> dimension table
- star schema dirrect joins with dimesnions
- snowflake may require multiple joins with dimension

***
12. **SQL query to find house from student table whose avg(score) > 70**

```sql
with cte as 
(select id, avg(score) as avg_score from student group by id)

select c.id, a.Name, a.address 
from cte c inner join address_tbl a
on c.id = a.id
where avg_score > 70;
```

***
13. **Difference between list and tuple**
- list is mutable
- tupe is not mutable. 

***
14. **What is map reduce architecture**
- Hadoop vs Spark lec - lec 3

***
15. **How did you handle production deployment in your project?**
- CI/CD pipeline
- BitBucket as as version control 
- Jfrog - Jenkins to createartifactory and move the code to production

***
16. **Spark Submit Command**
```

```

***
17. **Partitioning vs Bucketing in Spark**
-

***
18. **What is comparison between spark sql and hive in terms performance?**
- Spark is in-memory processing
- Query optimization 
- Data Format  
- ORC is better suited for Hive while parquet is best suited for PySpark
- Though PySpark is suited with other file formnats. 

***
# PWC

1. **Brief Introduction**

***
2. **How many tables will be there after joining two tables, left and inner join**
```
tbl1        tbl2    |  left join -> 10
id          id      |  
1           1       |  inner join -> 8
1           1       |
1           0       |  right record -> 9
0           null    |  
0                   |  full outer join -> 11
null                |
null                |
```

***
3. **Difference between rank, dense rank and row number?**

-  Rank assigns a number / rank to a agiven sequence in order if the sequesnce no is repeadted the rank is same for the repeadted sequence no.s and  assigned number is skipped exactly equal to the repeadted no. of sequence 

- Dense rank is same as rank it just that the assigned number is repeated same times the sequence is repeated but the next number assigned is not skipped. 

- Row_number just assisngs a numerical no.s is order like 1,2,3 ...

***
4. **Read the dataframe, define schema, filter out employee earning less than 20k add column bonus 10% for each employee and calculate the total salary after bonus. Save the final df as parquet.**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").getOrCreate()

file_path = '/home/mohitkatkar/Documents/file.csv'

my_schema =  StructType([
    StructField("Name",StringType(),True),
    StructField("Department",StringType(),True),
    StructField("Salary",IntegerType(),True)
])

df = spark.read.format('csv')\
        .option('header','true')\
        .option('sep','|')\
        .schema(my_schema)\
        .load(file_path)

df2 = df.filter(col("Salary")<20000)

df3 = df2.withColumn("Bonus",col('Salary')*.1)

df4 = df3.withColumn("Total_Salary",col("Salary")+col("Bonus"))

df5 = df4.withColumnRenamed('Salary',"Employee_Salary")

df5.write.format('parquet').save("file_path")
```

***
5. **How many points Mohit will get on below given point system?**
```python
point_dict = {}

for chars, points in point_system:
    for char in chars:
        point_dict[char] = points

print(point_dict)

value = 0
name = 'mohit'

for key in name:
    value = point_dict[key] + value

print(value)
```

***
6. **In the below given data replace '!$#@' character with empty space.**

```python
df.withColumn('lastName',when(col('lastName').contains("!"),"").otherwise('lastName'))\
    .withColumn('lastName',when(col('lastName').contains("$"),"").otherwise('lastName'))\
    .withColumn('lastName',when(col('lastName').contains("@"),"").otherwise('lastName'))\
    .withColumn('lastName',when(col('lastName').contains("#"),"").otherwise('lastName'))

df.withColumn('lastName',regexp_replace(col('lastName'),"[!@#$]",""))
```

***
# Nagarro

1. **Brief Introduction**

***
2. **Can you explain about your project architecture?**
- 

***
3. **How to upsert your data on daily basis using sppppark?**
- Find the column that will be unique
- Split the data into new_records_df and update_records_df
- Update the existing records using join
- Union the new_records_df

***
4. **How to perform scd2 using spark?**
- 

***
5. **What is shuffle and how to handle this?**
- Refer Lec-6 and Lec-11 from Spark Series.

***
6. **What is broadcasr join and why it is required?**
- Refrer Lec-14

***
7. **What is predicate pushdown and AQE, show with realtime example.**
- Row based filtering
```sql 
select * from tbl where sal > 50000;
```
- Here due to predicate pushdown the query will fetch only the records more the 50000 sal rnage on database level itself and wont be scanning other records, saving resouce.  For eg the Parquet file format stores highest and lowest range if there are no records greater than 50000, the query o/p wont scan anything. 

***
8. **PySpark code to perform broadcast join and conditional agregation based on location max(avg(salary))**
- 

***
9. **From a student table based on student id get the best 3 marks of student and get the avg of that.**

```SQL
with cte as (
select *,
row_number() over(partition by student_id ordder by marks desc) as rn
from students
)

select student_id, avg(marks) from cte 
group by student_id
where rn <= 3;
```

```python
window = Window.partitionBy('student_id').orderBy(desc('marks'))

rankedDf = df.withColumn('rn',row_number().over(window))

best_three_df = rankedDf.filter(col('rn') <= 3)

resultDf = best_three_df.groupBy('student_id').agg(avg('marks'))
```

***
10. **How to handle null in Spark**
- Filtering null values
- Replacing null vlaues
- Dropping rows with null values
- Coalesce function

***
11. **How did you handle scd in your project and etl implementation?**
-

***
12. **How to convert scd0 to scd3?**
- 

***
13. **Fact table and Dimensional table properties**
- Fact Table 
    - Gives measurement
    - Transcational Data
    - Foreign Keys
    - Data Volume (Mostly High)

- Dimension Table
    - Gives context
    - Descriptive Data
    - Primary Keys
    - Frequent updates (For Eg. If any new product is launced we store its details in product dimension table)
    - Smaller data volume

***
14. **How did you handle tbs of data during ingestion in a data pieline where trigger can be invoked along with scheduling**
- 

***
15. **What are the challenges you faced implementing Spark jobs and how did u resolve it?**
- Data duplication after join
- Upsert not supported
- Business complex use case
- OOM
- Spark optimization for TBs of data.

***
16. **What is data shuffling? Why it is occured and what are the techniques to resolve this?**
- 

***
17. **Difference between narrow and wide transformations?**
- 

***
18. **What is the difference between groupByKey and reduceByKey?**
- 

***
19. **What is the data volumne in your day to day pipeline and how to resolve scalability challenges?**
- 

***
20. **How to validate and monitor jobs stages in Spark in production and what is the orchestration tool you use for automation of ypour jobs?**
- Spark Ui though in production we did not have access, used to check logs.
- CA Autosys

***
21. **What is NoSQL datbase and what are its features?**
- A No-SQL database (short for Not Only SQL) is a non-relational database that provides Flexible, Scalable and Schema-Less approach to data storage. 

- No SQL databases are designed to handle large volumne of unstructured or Semi-structured data. 

- NoSQL databases are schema-less, meaning there is no predefined table structure.

- NoSQL databases are designed for horizontal scaling using sharding and replication. Instead of scaling up (adding more resources to a single machine), they scale out by distributing data across multiple servers.

- They offer low-latency reads and writes, optimized for high-performance applications. Support in-memory storage for real-time data access.

- Caching and In-Memory storage. 

***
# Jio

1. Brief Introduction

***
2. **lst=[12,3,27,5,4,9,4] find 3 numbers whose multiplication gives 180.**
```python
def find_mul(lst,k):
    for i in range(len(lst)):
        for j in range(len(lst)-1):
            x = lst[i]*lst[j]
            if (k/x) in lst[i+2:]:
                return lst[i],lst[j],(k//x)
    return False
```
```python
def find_mul_hm(lst,k):
    hashMap = {}

    for i in range(len(lst)):
        for j in range(i,len(lst)-1):
            x = lst[i] * lst[j]
            if k/x in hashMap:
                return i,j,hashMap[k//x]
            else:
                hashMap[lst[i]] = i
                hashMap[lst[j]] = j
    return False
```


***
3. **Write a python script to insert and delete the element from a list without using insert and pop function.**

```python
ls = [18,5,7,43,67,43,6]

length = len(lst)

inp=9
index=3

if index == length:
    ls = ls + [inp]
else:
    ls = ls[:index] + [inp] + [index:]

rm_index = 3

if rm_index == len(lst):
    ls = ls[:rm_index]
else:
    ls = ls[:rm_index] + ls[rm_index+1:]
```

***
4. **Check if two strings are anagram**

```python
def check_anagram(str1,str2):
    hashMap1 = {}
    hashMap2 = {}

    if len(str1) !=  len(str2):
        return False

    for i in range(len(str1)):
        hashMap1[str1[i]] = 1 + hashMap1.get(str1[i],0)

    for i in range(len(str2)):
        hashMap2[str2[i]] = 1 + hashMap2.get(str2[i],0)
    
    if hashMap1 == hashMap2:
        return True
    
    return False
```

***
5. **Lead Lag question**

***
6. **What optimization technique you have used other than inbuilt repartitioning, caching, salting?**
- Filtering - Caching - Repartition - Broadcast Join

***
7. **Write a query to list mataches of every team from single table Teams.**
```sql
Select *
from match m1 inner join match m1
on m1.team <> m2.team
```
```sql
select * 
from Teams t1 cross join Teams t2 
where t1.TeamName <> t2.TeamName;
```

***
8. **What are your aspiration and why are you looking for a change?**
- 

***
9. **What are the major challenges you faced in the project and how did you resolve it?**
- Business
- Technical 


# Maveric

1. **Explain end to end arhcitecture of projects you worked on**

***
2. **Client vs Cluster mode in Spark**

***
3. **what is Dag?**

***
4. **What is Lazy Evaluation?**


***
5. **3rd Highest Salary in dept**

```sql
sel dept, 
dense_rank() over(sal partition by dept order by sal desc) as rnk,
sal,
where rnk = 3;
```
***

6. **How to read data from Hive table in PySpark?**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadHiveTable") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.table("sales.employee")

df = spark.sql("SELECT * FROM sales.employee")
```

***
7. **How to load data in table using PySpark?**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.csv("hdfs:///user/data/employee.csv", header=True, inferSchema=True)

df.write.saveAsTable("default.employee")

df.write.mode("overwrite").saveAsTable("default.employee")
```

***
8. **Load data in Hive table from file**
```SQL
CREATE EXTERNAL TABLE employee (
    id INT,
    name STRING,
    salary FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/external/employee';

LOAD DATA INPATH '/path/in/hdfs/employee.csv' INTO TABLE employee;
```

***
9. **How to copy file from hdfs to local?**
- hadoop fs -copyToLocal /hdfs/path /localPath

***
10. **Make the given data of marks in diffrent lines**

```SQL
Student_name marks
Ashok 87,92,76,89
```

```python

```
