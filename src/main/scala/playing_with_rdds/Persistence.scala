package playing_with_rdds

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

object Persistence {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Persistence").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val mydata = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/purplecow.txt")
		val myrdd = mydata.map(line => line.toUpperCase).filter(line => line.startsWith("I"))
		// Each	action	re-executes	the	lineage transformations	starting	with	the base
		println(s"Count: ${myrdd.count()}")

		// Each	action	re-executes	the	lineage transformations	starting	with	the base
		println(s"Count: ${myrdd.count()}")

		// Persisting	an	RDD	saves	the	data	(in memory,	by	default)
		val myrdd1 = mydata.map(line => line.toUpperCase)
		myrdd1.persist() // by Default MEMORY_ONLY
		// myrdd1.persist(StorageLevel.MEMORY_ONLY)
		// myrdd1.persist(StorageLevel.DISK_ONLY)
		// myrdd1.persist(StorageLevel.MEMORY_AND_DISK)
		val myrdd2 = myrdd1.filter(line => line.startsWith("I"))

		println(s"Count: ${myrdd2.count()}")
		// Subsequent	operations	use	saved data
		println(s"Count: ${myrdd2.count()}")

		/**
			* In-memory	persistence	is	a	suggestion to	Spark
			* – If	not	enough	memory	is	available,	persisted	partitions	will	be	cleared from	memory
			* – Least	recently	used	partitions	cleared	first
			* – Transformations	will	be	re-executed	using	the	lineage	when	needed
			*
			* The	persist method	offers	other	options	called	storage	levels
			* – Storage	location	(memory	or	disk)
			* – Format	in	memory
			* – Partition	replication
			*
			* Storage	location—where	is	the	data	stored?
			* – MEMORY_ONLY:	Store	data	in	memory	if	it	fits
			* – MEMORY_AND_DISK:	Store	partitions	on	disk	if	they	do	not	fit	in memory (Called	spilling)
			* – DISK_ONLY:	Store	all	partitions	on	disk
			*
			* Serialization—you	can	choose	to	serialize	the	data	in	memory
			* – MEMORY_ONLY_SER and	MEMORY_AND_DISK_SER
			* – Much	more	space	efficient
			* – Less	time	efficient
			* – If	using	Java	or	Scala,	choose	a	fast	serialization	library	such	as	Kryo
			*
			* Replication—store	partitions	on	two	nodes
			* – DISK_ONLY_2
			* – MEMORY_AND_DISK_2
			* – MEMORY_ONLY_2
			* – MEMORY_AND_DISK_SER_2
			* – MEMORY_ONLY_SER_2
			*
			* cache() is	a	synonym	for	persist() with	no	storage	level	specified
			*
			* Persistence	replication	makes	recomputation	less	likely	to	be	necessary
			*
			* When	should	you	persist	a	dataset?
			* – When	a	dataset	is	likely	to	be	re-used
			* – Such	as	in	iterative	algorithms	and	machine	learning
			* How	to	choose	a	persistence	level
			* – Memory	only—choose	when	possible,	best	performance
			* – Save	space	by	saving	as	serialized	objects	in	memory	if	necessary
			* – Disk—choose	when	recomputation	is	more	expensive	than	disk	read
			* – Such	as	with	expensive	functions	or	filtering	large	datasets
			* – Replication—choose	when	recomputation	is	more	expensive	than memory
			*
			* To	stop	persisting	and	remove	from	memory	and	disk
			* – rdd.unpersist()
			* To	change	an	RDD	to	a	different	persistence	level
			* – Unpersist	first
			*/
		myrdd2.unpersist()

		/**
			* Essential	Points
			*
			* Spark	keeps	track	of	each	RDD’s	lineage
			* – Provides	fault	tolerance
			* By	default,	every	RDD	operation	executes	the	entire	lineage
			* If	an	RDD	will	be	used	multiple	times,	persist	it	to	avoid	re-computation
			* Persistence	options
			* – Location—memory	only,	memory	and	disk,	disk	only
			* – Format—in-memory	data	can	be	serialized	to	save	memory	(but	at	the cost	of	performance)
			* – Replication—saves	data	on	multiple	locations	in	case	a	node	goes	down, for	job	recovery	without	recomputation
			*/

		sc.stop()
		spark.stop()
	}
}
