# PySpark APIs - Comprehensive Guide

A comprehensive notebook demonstrating various PySpark APIs and transformations for data processing. This guide covers data reading, transformations, window functions, joins, and data writing operations.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Data Files](#data-files)
- [Data Reading](#data-reading)
- [Transformations](#transformations)
- [Advanced Operations](#advanced-operations)
- [Data Writing](#data-writing)
- [Spark SQL](#spark-sql)

## Overview

This notebook provides hands-on examples of PySpark APIs for:
- Reading data from various formats (CSV, JSON)
- Data transformations and manipulations
- Schema definitions and type casting
- String and date operations
- Aggregations and window functions
- Joins and data merging
- User-defined functions (UDFs)
- Writing data to various formats

## Prerequisites

- Apache Spark (PySpark)
- Python 3.x
- Databricks Runtime (for `dbutils` and `display()` functions)
- Or Apache Spark standalone with equivalent functions

## Data Files

The notebook uses the following sample datasets:
- **BigMart Sales.csv**: Contains sales data with columns like Item_Identifier, Item_Weight, Item_Fat_Content, Item_Visibility, Item_Type, Item_MRP, Outlet_Identifier, Outlet_Establishment_Year, Outlet_Size, Outlet_Location_Type, Outlet_Type, and Item_Outlet_Sales
- **drivers.json**: JSON file containing driver information

## Data Reading

### Reading JSON Files

```python
df_json = spark.read.format('json').option('inferSchema',True)\
                    .option('header',True)\
                    .option('multiLine',False)\
                    .load('/FileStore/tables/drivers.json')
```

### Reading CSV Files

```python
df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
```

### Schema Definitions

PySpark supports three methods for defining schemas:

#### 1. DDL Schema (String-based)

```python
my_ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight STRING,
    Item_Fat_Content STRING, 
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size STRING,
    Outlet_Location_Type STRING, 
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE 
'''

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv')
```

#### 2. StructType Schema (Programmatic)

```python
from pyspark.sql.types import *

my_strct_schema = StructType([
    StructField('Item_Identifier',StringType(),True),
    StructField('Item_Weight',StringType(),True),
    StructField('Item_Fat_Content',StringType(),True),
    StructField('Item_Visibility',StringType(),True),
    StructField('Item_MRP',StringType(),True),
    StructField('Outlet_Identifier',StringType(),True),
    StructField('Outlet_Establishment_Year',StringType(),True),
    StructField('Outlet_Size',StringType(),True),
    StructField('Outlet_Location_Type',StringType(),True),
    StructField('Outlet_Type',StringType(),True),
    StructField('Item_Outlet_Sales',StringType(),True)
])

df = spark.read.format('csv')\
            .schema(my_strct_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv')
```

#### 3. Infer Schema (Automatic)

```python
df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
df.printSchema()  # View the inferred schema
```

## Transformations

### SELECT

Select specific columns from a DataFrame:

```python
from pyspark.sql.functions import col

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()
```

### ALIAS

Rename columns using aliases:

```python
df.select(col('Item_Identifier').alias('Item_ID')).display()
```

### FILTER

Filter rows based on conditions:

```python
# Single condition
df.filter(col('Item_Fat_Content')=='Regular').display()

# Multiple conditions with AND
df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()

# Null checks and IN clause
df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()
```

### Renaming Columns

```python
df.withColumnRenamed('Item_Weight','Item_Wt').display()
```

### Adding/Modifying Columns

#### Add a Constant Column

```python
from pyspark.sql.functions import lit

df = df.withColumn('flag', lit("new"))
```

#### Add a Calculated Column

```python
df.withColumn('multiply', col('Item_Weight')*col('Item_MRP')).display()
```

#### Modify Existing Column with Regex

```python
from pyspark.sql.functions import regexp_replace

df = df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Low Fat","Lf"))
```

### Type Casting

Convert data types:

```python
df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))
df.printSchema()  # Verify the change
```

### Sorting

Sort data by columns:

```python
# Descending order
df.sort(col('Item_Weight').desc()).display()

# Ascending order
df.sort(col('Item_Visibility').asc()).display()

# Multiple columns with different sort orders
df.sort(['Item_Weight','Item_Visibility'], ascending=[0,0]).display()  # Both descending
df.sort(['Item_Weight','Item_Visibility'], ascending=[0,1]).display()  # First desc, second asc
```

### Limit

Limit the number of rows returned:

```python
df.limit(10).display()
```

### Drop Columns

Remove columns from DataFrame:

```python
# Single column
df.drop('Item_Visibility').display()

# Multiple columns
df.drop('Item_Visibility','Item_Type').display()
```

### Remove Duplicates

```python
# Remove all duplicate rows
df.dropDuplicates().display()

# Remove duplicates based on specific columns
df.drop_duplicates(subset=['Item_Type']).display()

# Alternative method
df.distinct().display()
```

### Union Operations

#### Union (Position-based)

```python
# Creates DataFrame with same schema
data1 = [('1','kad'), ('2','sid')]
schema1 = 'id STRING, name STRING' 
df1 = spark.createDataFrame(data1, schema1)

data2 = [('3','rahul'), ('4','jas')]
schema2 = 'id STRING, name STRING' 
df2 = spark.createDataFrame(data2, schema2)

df1.union(df2).display()
```

**Note:** Union matches columns by position, not by name.

#### Union by Name (Column-based)

```python
# Matches columns by name, regardless of order
df1.unionByName(df2).display()
```

### String Functions

```python
from pyspark.sql.functions import upper

df.select(upper('Item_Type').alias('upper_Item_Type')).display()
```

### Date Functions

#### Current Date

```python
from pyspark.sql.functions import current_date

df = df.withColumn('curr_date', current_date())
```

#### Date Addition

```python
from pyspark.sql.functions import date_add

df = df.withColumn('week_after', date_add('curr_date', 7))
```

#### Date Subtraction

```python
from pyspark.sql.functions import date_sub

df.withColumn('week_before', date_sub('curr_date', 7)).display()

# Alternative using date_add with negative value
df = df.withColumn('week_before', date_add('curr_date', -7))
```

#### Date Difference

```python
from pyspark.sql.functions import datediff

df = df.withColumn('datediff', datediff('week_after','curr_date'))
```

#### Date Formatting

```python
from pyspark.sql.functions import date_format

df = df.withColumn('week_before', date_format('week_before','dd-MM-yyyy'))
```

### Handling Null Values

#### Dropping Nulls

```python
# Drop rows where all values are null
df.dropna('all').display()

# Drop rows where any value is null
df.dropna('any').display()

# Drop rows where specific column(s) are null
df.dropna(subset=['Outlet_Size']).display()
```

#### Filling Nulls

```python
# Fill all nulls with a value
df.fillna('NotAvailable').display()

# Fill nulls in specific column(s)
df.fillna('NotAvailable', subset=['Outlet_Size']).display()
```

### Split and Indexing

#### Split String into Array

```python
from pyspark.sql.functions import split

df.withColumn('Outlet_Type', split('Outlet_Type',' ')).display()
```

#### Extract Element from Array

```python
df.withColumn('Outlet_Type', split('Outlet_Type',' ')[1]).display()
```

### Explode Array Columns

```python
from pyspark.sql.functions import explode

df_exp = df.withColumn('Outlet_Type', split('Outlet_Type',' '))
df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()
```

### Array Functions

```python
from pyspark.sql.functions import array_contains

df_exp.withColumn('Type1_flag', array_contains('Outlet_Type','Type1')).display()
```

## Advanced Operations

### GroupBy and Aggregations

#### Basic Aggregations

```python
from pyspark.sql.functions import sum, avg

# Sum aggregation
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# Average aggregation
df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# Multiple aggregations
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'), avg('Item_MRP')).display()
```

#### Collect List (Array Aggregation)

```python
from pyspark.sql.functions import collect_list

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]
schema = 'user string, book string'
df_book = spark.createDataFrame(data, schema)

df_book.groupBy('user').agg(collect_list('book')).display()
```

### Pivot Tables

```python
df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()
```

### Conditional Logic (When-Otherwise)

```python
from pyspark.sql.functions import when

# Simple when-otherwise
df = df.withColumn('veg_flag', 
    when(col('Item_Type')=='Meat','Non-Veg')
    .otherwise('Veg'))

# Multiple conditions
df.withColumn('veg_exp_flag',
    when(((col('veg_flag')=='Veg') & (col('Item_MRP')<100)), 'Veg_Inexpensive')
    .when((col('veg_flag')=='Veg') & (col('Item_MRP')>100), 'Veg_Expensive')
    .otherwise('Non_Veg')).display()
```

### Joins

#### Prepare Sample Data

```python
dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 
schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 
df1 = spark.createDataFrame(dataj1, schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]
schemaj2 = 'dept_id STRING, department STRING'
df2 = spark.createDataFrame(dataj2, schemaj2)
```

#### Inner Join

Returns only matching records:

```python
df1.join(df2, df1['dept_id']==df2['dept_id'], 'inner').display()
```

#### Left Join

Returns all records from left DataFrame and matching records from right:

```python
df1.join(df2, df1['dept_id']==df2['dept_id'], 'left').display()
```

#### Right Join

Returns all records from right DataFrame and matching records from left:

```python
df1.join(df2, df1['dept_id']==df2['dept_id'], 'right').display()
```

#### Anti Join

Returns records from left DataFrame that don't have matches in right DataFrame:

```python
df1.join(df2, df1['dept_id']==df2['dept_id'], 'anti').display()
```

### Window Functions

#### Row Number

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df.withColumn('rowCol', 
    row_number().over(Window.orderBy('Item_Identifier'))).display()
```

#### Rank vs Dense Rank

```python
from pyspark.sql.functions import rank, dense_rank

df.withColumn('rank', rank().over(Window.orderBy(col('Item_Identifier').desc())))\
  .withColumn('denseRank', dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()
```

#### Cumulative Sum

```python
from pyspark.sql.functions import sum

# Cumulative sum over window
df.withColumn('cumsum', 
    sum('Item_MRP').over(
        Window.orderBy('Item_Type')
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )).display()

# Total sum over entire partition
df.withColumn('totalsum', 
    sum('Item_MRP').over(
        Window.orderBy('Item_Type')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )).display()
```

### User Defined Functions (UDF)

#### Define Python Function

```python
def my_func(x):
    return x*x
```

#### Register as UDF

```python
from pyspark.sql.functions import udf

my_udf = udf(my_func)
```

#### Use UDF in DataFrame

```python
df.withColumn('mynewcol', my_udf('Item_MRP')).display()
```

## Data Writing

### Writing to CSV

```python
df.write.format('csv').save('/FileStore/tables/CSV/data.csv')
```

### Write Modes

#### Append Mode

Adds data to existing files:

```python
df.write.format('csv')\
        .mode('append')\
        .save('/FileStore/tables/CSV/data.csv')
```

#### Overwrite Mode

Replaces existing data:

```python
df.write.format('csv')\
        .mode('overwrite')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()
```

#### Error Mode

Throws an error if the path already exists:

```python
df.write.format('csv')\
        .mode('error')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()
```

#### Ignore Mode

Does nothing if the path already exists:

```python
df.write.format('csv')\
        .mode('ignore')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()
```

### Writing to Parquet

```python
df.write.format('parquet')\
        .mode('overwrite')\
        .save('/FileStore/tables/data.parquet')
```

### Saving as Table

```python
df.write.format('parquet')\
        .mode('overwrite')\
        .saveAsTable('my_table')
```

## Spark SQL

### Create Temporary View

```python
df.createTempView('my_view')
```

### Query Using SQL

```sql
%sql
SELECT * FROM my_view WHERE Item_Fat_Content = 'Lf'
```

### Query Using Spark SQL API

```python
df_sql = spark.sql("SELECT * FROM my_view WHERE Item_Fat_Content = 'Lf'")
df_sql.display()
```

## Key Takeaways

1. **Schema Definition**: Choose between DDL strings, StructType, or schema inference based on your needs
2. **Union vs UnionByName**: Use `unionByName()` when column orders differ between DataFrames
3. **Window Functions**: Powerful for ranking, cumulative calculations, and row-level operations
4. **Join Types**: Choose the appropriate join type (inner, left, right, anti) based on your data requirements
5. **Null Handling**: Use `dropna()` and `fillna()` strategically to clean your data
6. **Write Modes**: Understand the different write modes to avoid data loss or errors
7. **UDF Performance**: Use UDFs sparingly as they can impact performance; prefer built-in functions when possible

## Best Practices

- Always define schemas explicitly when reading data for better performance and type safety
- Use `unionByName()` instead of `union()` when column orders might differ
- Leverage window functions for efficient analytical operations
- Choose appropriate join types to avoid unintended data loss
- Use appropriate write modes to prevent accidental data overwrites
- Consider partitioning when writing large datasets

## Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Documentation](https://docs.databricks.com/)

---

**Note**: This notebook is designed for Databricks environment. For standalone Spark, replace `display()` with `.show()` and adjust file paths accordingly.

