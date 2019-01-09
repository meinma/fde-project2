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
val trips2 = trips.withColumn("lon1_rad", toRadians($"pickup_longitude"))
val trips3 = trips2.withColumn("lon2_rad", toRadians($"dropoff_longitude"))
val trips4 = trips3.withColumn("lat1_rad", toRadians($"pickup_latitude"))
val trips5 = trips4.withColumn("lat2_rad", toRadians($"dropoff_latitude"))

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
	) * 6371e3 * 2

) / (2*dist))).cache()

val distanceNeighbours = distanceBuckets.withColumn("distanceBucket",explode(array($"distanceBucket" - 1,$"distanceBucket",$"distanceBucket" + 1))).cache()

val pickupBuckets = distanceNeighbours.withColumn("pickupBucket",floor((unix_timestamp($"tpep_pickup_datetime") - unix_timestamp(lit("2016-01-01 00:00:00"))) 
	/ (8*3600))).cache()
val dropoffBuckets = distanceBuckets.withColumn("dropoffBucket",floor((unix_timestamp($"tpep_dropoff_datetime") - unix_timestamp(lit("2016-01-01 00:00:00")))
 / (8*3600))).cache()
val pickupNeighbours = pickupBuckets.withColumn("pickupBucket", explode(array($"pickupBucket", $"pickupBucket" + 1))).cache()

val timeJoin = dropoffBuckets.as("a").join(pickupNeighbours.as("b"), ($"a.dropoffBucket" === $"b.pickupBucket")
    && (unix_timestamp($"b.tpep_pickup_datetime") > unix_timestamp($"a.tpep_dropoff_datetime"))
    && (unix_timestamp($"a.tpep_dropoff_datetime") + 8*3600 > unix_timestamp($"b.tpep_pickup_datetime"))
    && ($"a.distanceBucket" === $"b.distanceBucket")
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


