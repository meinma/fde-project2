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


// Add Columns for longitude and latitude of pickup and dropoff in radians instead of degrees
val trips4 = trips.withColumn("lon1_rad", toRadians($"pickup_longitude"))
.withColumn("lon2_rad", toRadians($"dropoff_longitude"))
.withColumn("lat1_rad", toRadians($"pickup_latitude"))
.withColumn("lat2_rad", toRadians($"dropoff_latitude"))

// Select the necessary columns
val trips5 = trips4.select("lon1_rad","lon2_rad","lat1_rad","lat2_rad","tpep_pickup_datetime","tpep_dropoff_datetime","pickup_latitude","pickup_longitude","dropoff_latitude","dropoff_longitude")




// Create a bucket for the pickup and dropoff positions, map positions within dist meters to same buckets
val pickupLatitudeBucket = trips5.withColumn("pickupLat",floor($"lat1_rad" * (6371e3  / dist))).withColumn("pickupLon",floor($"lon1_rad" * (cos(lit("61")) * 6371e3 /  dist)))
val pickupLatitudeNeighbours = pickupLatitudeBucket.withColumn("pickupLat",explode(array($"pickupLat" - 1,$"pickupLat",$"pickupLat" + 1))).withColumn("pickupLon",explode(array($"pickupLon" - 1,$"pickupLon",$"pickupLon" + 1)))
val dropoffLatitude = trips5.withColumn("dropoffLat",floor($"lat2_rad" * (6371e3 /  dist))).withColumn("dropoffLon",floor($"lon2_rad" * (cos(lit("61")) *6371e3 / dist)))
// Create buckets for trips within 8 hours for pickup and dropoff time
val pickupBuckets = pickupLatitudeNeighbours.withColumn("pickupBucket",floor(unix_timestamp($"tpep_pickup_datetime") / (8*3600)))
val dropoffBuckets = dropoffLatitude.withColumn("dropoffBucket",floor(unix_timestamp($"tpep_dropoff_datetime") / (8*3600)))
val pickupNeighbours = pickupBuckets.withColumn("pickupBucket", explode(array($"pickupBucket", $"pickupBucket" - 1)))
// Check the bucket conditions at first so that as less trips as possible are left
val timeJoin = dropoffBuckets.as("a").join(pickupNeighbours.as("b"),($"a.dropoffLat" === $"b.pickupLat")
	&& ($"a.dropoffLon" === $"b.pickupLon")
	&&($"a.dropoffBucket" === $"b.pickupBucket")
	// Check if trip a has ended earlier than trip b has started
	&& (unix_timestamp($"a.tpep_dropoff_datetime") < unix_timestamp($"b.tpep_pickup_datetime"))
	// Check if trip b has started maximum eight hours after trip a ended
    && (unix_timestamp($"a.tpep_dropoff_datetime") + 8*3600 > unix_timestamp($"b.tpep_pickup_datetime"))
	// Computing the distance between two points given in radians and checking of the condition of dist is fulfilled
    // Check if a.dropoff position is within dist meters of b's pickup location
	&& (asin(
		sqrt(
			sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2)
		)) * 2 * 6371e3 < dist)
	// Check if b.dropoff location is within dist meters of a's pickup location
	&& 
	(asin(
		sqrt(
			sin(($"b.lat2_rad"-$"a.lat1_rad")/2) * sin(($"b.lat2_rad"-$"a.lat1_rad")/2)
				+ cos($"b.lat2_rad") * cos($"a.lat1_rad")
				* sin(($"b.lon2_rad"-$"a.lon1_rad")/2) * sin(($"b.lon2_rad"-$"a.lon1_rad")/2)
		)
	) * 2 * 6371e3 < dist)
)

timeJoin
}}






