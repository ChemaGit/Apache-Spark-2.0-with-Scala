/** Question 21
  * Problem Scenario 46 : You have been given belwo list in scala (name,sex,cost) for each work done.
  * List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
  * Now write a Spark program to load this list as an RDD and do the sum of cost for combination of name and sex (as key)
  */

val emp = sc.parallelize(List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)))
val empKey = emp.map({case(name,sex,cost) => ( (name,sex),cost)})
val cost = empKey.reduceByKey( (v, v1) => v + v1)
cost.collect.foreach(println)
/*
((Deepak,female),2000)
((Neeta,female),2000)
((Deeapak,male),4000)
((Deepak,male),3000)
((Deepika,female),2000)
 */