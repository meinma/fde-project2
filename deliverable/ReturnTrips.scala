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



val trips4 = trips.withColumn("lon1_rad", toRadians($"pickup_longitude"))
.withColumn("lon2_rad", toRadians($"dropoff_longitude"))
.withColumn("lat1_rad", toRadians($"pickup_latitude"))
.withColumn("lat2_rad", toRadians($"dropoff_latitude"))


//val trips5 = trips4.select("lon1_rad","lon2_rad","lat1_rad","lat2_rad","tpep_pickup_datetime","tpep_dropoff_datetime","pickup_latitude","pickup_longitude","dropoff_latitude","dropoff_longitude")
val pickupLatitudeBucket = trips4.withColumn("pickupLat",floor(ceil($"pickup_latitude" * 111111 ) / (2 * dist))).withColumn("pickupLon",floor(ceil($"pickup_longitude"*111111) / (2 * dist)))
val pickupLatitudeNeighbours = pickupLatitudeBucket.withColumn("pickupLat",explode(array($"pickupLat" - 1,$"pickupLat",$"pickupLat" + 1))).withColumn("pickupLon",explode(array($"pickupLon" - 1,$"pickupLon",$"pickupLon" + 1)))
val dropoffLatitude = trips4.withColumn("dropoffLat",floor(ceil($"dropoff_latitude" * 111111 ) / (2 * dist))).withColumn("dropoffLon",floor(ceil($"dropoff_longitude" * 111111)/(2 * dist)))
val pickupBuckets = pickupLatitudeNeighbours.withColumn("pickupBucket",floor((unix_timestamp($"tpep_pickup_datetime") - unix_timestamp(lit("2016-01-01 00:00:00"))) //distanceBuckets
	/ (8*3600)))
val dropoffBuckets = dropoffLatitude.withColumn("dropoffBucket",floor((unix_timestamp($"tpep_dropoff_datetime") - unix_timestamp(lit("2016-01-01 00:00:00"))) //distanceNeighbours
 / (8*3600)))
val pickupNeighbours = pickupBuckets.withColumn("pickupBucket", explode(array($"pickupBucket", $"pickupBucket" + 1)))
//Join funktioniert und die Spaltenwerte liegen nciht am Join
val timeJoin = dropoffBuckets.as("a").join(pickupNeighbours.as("b"),($"a.dropoffBucket" === $"b.pickupBucket")
	&& ($"a.dropoffLat" === $"b.pickupLat")
	&& ($"a.dropoffLon" === $"b.pickupLon")
	//Das hier muss richtig sein
	&& (unix_timestamp($"a.tpep_dropoff_datetime") < unix_timestamp($"b.tpep_pickup_datetime"))
    && (unix_timestamp($"a.tpep_dropoff_datetime") + 8*3600 > unix_timestamp($"b.tpep_pickup_datetime"))
    //Hier kann nix falsch sein
    && (lit("2") * (atan2(
		sqrt(
			sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2)
		),
		sqrt(lit("1") - (sin(($"a.lat2_rad"-$"b.lat1_rad")/2) * sin(($"a.lat2_rad"-$"b.lat1_rad")/2)
				+ cos($"a.lat2_rad") * cos($"b.lat1_rad")
				* sin(($"a.lon2_rad"-$"b.lon1_rad")/2) * sin(($"a.lon2_rad"-$"b.lon1_rad")/2))
		)
	) * 6371000) < dist)
	&& 
	(lit("2") * (atan2(
		sqrt(
			sin(($"b.lat2_rad"-$"a.lat1_rad")/2) * sin(($"b.lat2_rad"-$"a.lat1_rad")/2)
				+ cos($"b.lat2_rad") * cos($"a.lat1_rad")
				* sin(($"b.lon2_rad"-$"a.lon1_rad")/2) * sin(($"b.lon2_rad"-$"a.lon1_rad")/2)
		),
		sqrt(lit("1") - (sin(($"b.lat2_rad"-$"a.lat1_rad")/2) * sin(($"b.lat2_rad"-$"a.lat1_rad")/2)
				+ cos($"b.lat2_rad") * cos($"a.lat1_rad")
				* sin(($"b.lon2_rad"-$"a.lon1_rad")/2) * sin(($"b.lon2_rad"-$"a.lon1_rad")/2))
		)
	) * 6371000) < dist)
)

timeJoin
}}






