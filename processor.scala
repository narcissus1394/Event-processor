import org.apache.spark.sql.SQLContext

 object processor {

      def main(args: Array[String]): Unit = {        
	
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	val events = sqlContext.jsonFile("obfuscated_data")
	events.registerTempTable("events") 
	
	question1(sqlContext)		

	question2(sqlContext)
	
	question3(sqlContext)
	
	question4(sqlContext)

	question7(sqlContext)
      }


	def question1(sqlContext:SQLContext)
	{
		println("Question1: How many time a product has been launched?")
		println("Please enter the name of the product:")
		val product_name = Console.readLine()
		println("The name of product is:" + product_name)
		val query1= sqlContext.sql("SELECT COUNT(source) FROM events WHERE source='" + product_name + "' and type = 'launch' ")
		query1.collect.foreach(println)

	}
	
	def question2(sqlContext: SQLContext)
	{
		val query2= sqlContext.sql("SELECT event_id, COUNT(event_id) FROM events GROUP BY event_id HAVING count(event_id)>1")
		query2.collect.foreach(println)	
	}

	def question3(sqlContext: SQLContext)
	{
		val query3= sqlContext.sql("SELECT type,source FROM events WHERE time.create_timestamp>sender_info.received_timestamp" )
		query3.collect.foreach(println)
	}
	
	def question4(sqlContext: SQLContext)
	{	
		 val query4= sqlContext.sql("SELECT sender_info.geo.country,COUNT(*) FROM events WHERE source='product-a' GROUP BY sender_info.geo.country ")
		query4.collect.foreach(println)

		val query42= sqlContext.sql("SELECT device.operating_system.kind,COUNT(*) FROM events GROUP BY device.operating_system.kind") 
		query42.collect.foreach(println)	
	}

	def question7(sqlContext: SQLContext)
	{
		val query7= sqlContext.sql("SELECT r.dev_id,r.activity_time FROM (SELECT device.device_id as dev_id,MAX(time.create_timestamp)-MIN(time.create_timestamp) as activity_time FROM events GROUP BY device.device_id) r ORDER BY r.activity_time DESC limit 1")
		query7.collect.foreach(println)
	}

}


