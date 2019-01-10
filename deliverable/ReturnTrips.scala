import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._


// Für die Distanzberechnung
val trips4 = trips.withColumn("lon1_rad", toRadians($"pickup_longitude"))
.withColumn("lon2_rad", toRadians($"dropoff_longitude"))
.withColumn("lat1_rad", toRadians($"pickup_latitude"))
.withColumn("lat2_rad", toRadians($"dropoff_latitude"))

val trips5 = trips4.select("lon1_rad","lon2_rad","lat1_rad","lat2_rad","tpep_pickup_datetime","tpep_dropoff_datetime")


val distanceBuckets = trips5.withColumn("distanceBucket",floor((
atan2(
		sqrt(
			sin(($"lat2_rad"-$"lat1_rad")/2) * sin(($"lat2_rad"-$"lat1_rad")/2)
				+ cos($"lat2_rad") * cos($"lat1_rad")
				* sin(($"lon2_rad"-$"lon1_rad")/2) * sin(($"lon2_rad"-$"lon1_rad")/2)
		),
		sqrt(($"lat2_rad"/$"lat2_rad") - (sin(($"lat2_rad"-$"lat1_rad")/2) * sin(($"lat2_rad"-$"lat1_rad")/2)
				+ cos($"lat2_rad") * cos($"lat1_rad")
				* sin(($"lon2_rad"-$"lon1_rad")/2) * sin(($"lon2_rad"-$"lon1_rad")/2))
		)
	) * 6371e3 *2

) / (2 * dist))).cache() 
//val distanceBuckets = trips5.withColumn("distanceBucket",floor((acos(cos($"lat1_rad")*cos($"lat2_rad")*cos($"lon2_rad" - $"lon1_rad") + sin($"lat1_rad") * sin($"lat2_rad")) *6378137) / (2*dist))).cache()

val distanceNeighbours = distanceBuckets.withColumn("distanceBucket",explode(array($"distanceBucket" - 1,$"distanceBucket",$"distanceBucket" + 1))).cache()
val pickupBuckets = distanceBuckets.withColumn("pickupBucket",floor((unix_timestamp($"tpep_pickup_datetime") - unix_timestamp(lit("2016-01-01 00:00:00"))) //distanceBuckets
	/ (8*3600))).cache()
val dropoffBuckets = distanceNeighbours.withColumn("dropoffBucket",floor((unix_timestamp($"tpep_dropoff_datetime") - unix_timestamp(lit("2016-01-01 00:00:00"))) //distanceNeighbours
 / (8*3600))).cache()
val pickupNeighbours = pickupBuckets.withColumn("pickupBucket", explode(array($"pickupBucket", $"pickupBucket" + 1))).cache()
val timeJoin = dropoffBuckets.as("a").join(pickupNeighbours.as("b"),($"a.distanceBucket" === $"b.distanceBucket")&&($"a.dropoffBucket" === $"b.pickupBucket"))
val timeSelect = timeJoin.select($"b.tpep_pickup_datetime",$"a.tpep_dropoff_datetime",$"a.lat2_rad",$"a.lon2_rad",$"a.lat1_rad",$"a.lon1_rad",$"b.lon1_rad",$"b.lat1_rad",$"b.lat2_rad",$"b.lon2_rad")
val timeFilter = timeSelect.filter(unix_timestamp($"b.tpep_pickup_datetime") > unix_timestamp($"a.tpep_dropoff_datetime"))
val timeFilter2 = timeFilter.filter(unix_timestamp($"a.tpep_dropoff_datetime") + 8*3600 > unix_timestamp($"b.tpep_pickup_datetime"))
val distanceSelect = timeFilter2.select($"a.lon1_rad",$"a.lon2_rad",$"a.lat1_rad",$"a.lat2_rad",$"b.lon1_rad",$"b.lat1_rad",$"b.lat2_rad",$"b.lon2_rad")
val distanceFilter = distanceSelect.filter(
	atan2(
		sqrt(
			sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2)
		),
		sqrt(($"a.lat2_rad"/$"a.lat2_rad") - (sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2))
		)
	) * 6371e3 * 2 < dist
)
val distanceFilter2 = distanceFilter.filter(
	atan2(
		sqrt(
			sin(($"b.lat2_rad"-$"a.lat1_rad")/2) * sin(($"b.lat2_rad"-$"a.lat1_rad")/2)
				+ cos($"b.lat2_rad") * cos($"a.lat1_rad")
				* sin(($"b.lon2_rad"-$"a.lon1_rad")/2) * sin(($"b.lon2_rad"-$"a.lon1_rad")/2)
		),
		sqrt(($"a.lat2_rad"/$"a.lat2_rad") - (sin(($"b.lat2_rad"-$"a.lat1_rad")/2) * sin(($"b.lat2_rad"-$"a.lat1_rad")/2)
				+ cos($"b.lat2_rad") * cos($"a.lat1_rad")
				* sin(($"b.lon2_rad"-$"a.lon1_rad")/2) * sin(($"b.lon2_rad"-$"a.lon1_rad")/2))
		)
	) * 6371e3 * 2 < dist
)
distanceFilter2
}}

/*





acos(cos($"lat1_rad")*cos($"lat2_rad")*cos($"lon2_rad" - $"lon1_rad") + sin($"lat1_rad") * sin($"lat2_rad")) *6378137







atan2(
		sqrt(
			sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2)
		),
		sqrt(($"a.lat2_rad"/$"a.lat2_rad") - (sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2))
		)
	) * 6371e3 * 2 < dist
*/


/*
    && (unix_timestamp($"b.tpep_pickup_datetime") > unix_timestamp($"a.tpep_dropoff_datetime"))
    && (unix_timestamp($"a.tpep_dropoff_datetime") + 8*3600 > unix_timestamp($"b.tpep_pickup_datetime"))
    && (atan2(
		sqrt(
			sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2)
		),
		sqrt(($"a.lat2_rad"/$"a.lat2_rad") - (sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2))
		)
	) * 6371e3 * 2 < dist)
	&& 
	(atan2(
		sqrt(
			sin(($"b.lat2_rad"-$"a.lat1_rad")/2) * sin(($"b.lat2_rad"-$"a.lat1_rad")/2)
				+ cos($"b.lat2_rad") * cos($"a.lat1_rad")
				* sin(($"b.lon2_rad"-$"a.lon1_rad")/2) * sin(($"b.lon2_rad"-$"a.lon1_rad")/2)
		),
		sqrt(($"a.lat2_rad"/$"a.lat2_rad") - (sin(($"b.lat2_rad"-$"a.lat1_rad")/2) * sin(($"b.lat2_rad"-$"a.lat1_rad")/2)
				+ cos($"b.lat2_rad") * cos($"a.lat1_rad")
				* sin(($"b.lon2_rad"-$"a.lon1_rad")/2) * sin(($"b.lon2_rad"-$"a.lon1_rad")/2))
		)
	) * 6371e3 * 2 < dist)
)

timeJoin

}
} 
*/

