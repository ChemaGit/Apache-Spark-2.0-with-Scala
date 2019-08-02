/** Question 14
  * Problem Scenario 79 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.products
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of products table : (product_id | product categoryid | product_name | product_description | product_prtce | product_image )
  * Please accomplish following activities.
  * 1. Copy "retaildb.products" table to hdfs in a directory p93_products
  * 2. Filter out all the empty prices
  * 3. Sort all the products based on price in both ascending as well as descending order.
  * 4. Sort all the products based on price as well as product_id in descending order.
  * 5. Use the below functions to do data ordering or ranking and fetch top 10 elements top() takeOrdered() sortByKey()
  */

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table products \
--as-textfile \
--delete-target-dir \
--target-dir /user/cloudera/exercise_10/products \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

hdfs dfs -ls /user/cloudera/exercise_10/products
*/