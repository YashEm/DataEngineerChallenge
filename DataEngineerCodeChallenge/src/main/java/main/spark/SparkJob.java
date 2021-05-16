package main.spark;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebAnalytics
 * A new approach is taken for access logs analysis since leaving time of user is not present in the data
 * Fixed time frame is taken in consideration instead of dynamic session window according to each users start time
 */
public class SparkJob {
	
	private static Logger logger = LoggerFactory.getLogger(SparkJob.class);
	
    public static void main(String[] args) throws AnalysisException {
           	
    	SparkSession spark = SparkSession
    			.builder()
    			.master("local[*]")
    			.appName("DataEngineerChallenge")
    			.getOrCreate();
    	
    	/**
    	 * DATA PREPARATION:
    	 * Parding access log file into dataframe
    	 */
    	Dataset<AccessLog> accessLogData = spark.read().text(
				"data\\2015_07_22_mktplace_shop_web_log_sample.log.gz")
				.mapPartitions(new AccessLogEntryParser(),
						Encoders.bean(AccessLog.class));
    	accessLogData.createOrReplaceTempView("accessLogData");
//		accessLogData.show(false);

    	/**
    	 * Creating Time windows of 15 minutes by dividing "requestTime" field in 15 minutes batches
    	 */    			
    	Dataset<Row> accessLogDataWithSessions = spark.sql("SELECT url, userAgent, clientIPAddress, "
    			+ "to_unix_timestamp(requestStartTime, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'\") AS requestStartTimestamp, "
    			+ "CAST(to_unix_timestamp(requestStartTime, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'\")/(15 * 60) AS INT) AS sessionWindow "
    			+ "FROM accessLogData");
    	accessLogDataWithSessions.createOrReplaceTempView("accessLogDataWithSessions");
//    	accessLogDataWithSessions.show(false);
    	
  	
    	
		/**
		 * TASK 1: TIME WINDOW BASED SESSIONS
		 * Sessionizing access logs data by session window and user
		 * page_hits: total URL visits count
		 * session_start_time: minimum timestamp represents the first time the users visits in that session
		 * sessionWindow: the time window for that session
		 * userIP: unique user
		 */
		Dataset<Row> timeBasedSessionWindow = spark.sql("SELECT count(url) page_hits, min(requestStartTimestamp) AS sessionStartTimestamp, "
				+ "sessionWindow, clientIPAddress,  userAgent FROM accessLogDataWithSessions "
				+ "GROUP BY sessionWindow, clientIPAddress, userAgent ORDER BY sessionWindow ASC");
		timeBasedSessionWindow.createTempView("timeBasedSessionWindow");
		timeBasedSessionWindow = timeBasedSessionWindow.cache();
//		timeBasedSessionWindow.show(false);
		timeBasedSessionWindow = timeBasedSessionWindow.coalesce(1);
		timeBasedSessionWindow.write().option("header", "true").mode(SaveMode.Overwrite).csv("results/time_based_session_windows.csv");

		
		
		/**
		 * TASK 2: AVERAGE SESSION TIME
		 * Calculating session time for each session by taking difference between session start time and session end time
		 * Session end time is assumed as end of time frame since no users website's leaving time is present in data
		 * and average session time by finding out each session time, summing session time for whole data and dividing it by total number of sessions
		 */
		long totalSessions = timeBasedSessionWindow.count();
		logger.info("Total Sessions: {}", totalSessions);
		Dataset<Double> averageSessionTime = spark.sql("SELECT "
				+ "sum((((sessionWindow * (15*60))+ 15*60) - sessionStartTimestamp) /" + totalSessions + ")  / 60  as average_session_time_in_minutes "
				+ "FROM timeBasedSessionWindow").as(Encoders.DOUBLE());
		double averageSessionTimeInMinutes = averageSessionTime.collectAsList().get(0);
		logger.info("Average Session Time: {} Minutes", averageSessionTimeInMinutes);
		averageSessionTime = averageSessionTime.coalesce(1);
		averageSessionTime.write().option("header", "true").mode(SaveMode.Overwrite).csv("results/averageSessionTime.csv");
		
		
		/**
		 * TASK 3: UNIQUE URL VISITS PER SESSIONS
		 * Aggregating unique URLs hit per session
		 */
		Dataset<Row> uniqueURLsPerSession = spark.sql("SELECT count(distinct url) unique_url, "
				+ "min(requestStartTimestamp) AS sessionStartTimestamp "
				+ "FROM accessLogDataWithSessions GROUP BY sessionWindow");
		uniqueURLsPerSession.show(false);
		uniqueURLsPerSession = uniqueURLsPerSession.coalesce(1);
		uniqueURLsPerSession.write().option("header", "true").mode(SaveMode.Overwrite).csv("results/unique_urls_per_session.csv");

		
		
		
		/**
		 * TASK 4: MOST ENGAGED USERS
		 * Calculating session time for each session by taking difference between session start time and session end time
		 * Summing session time of all sessions of each user
		 */
		Dataset<Row> totalSessionTimePerUser = spark.sql("SELECT "
				+ "sum(((sessionWindow * 15*60)+15*60) - sessionStartTimestamp) as total_session_time, clientIPAddress, userAgent "
				+ "FROM timeBasedSessionWindow GROUP BY clientIPAddress, userAgent");
		totalSessionTimePerUser.createTempView("totalSessionTimePerUser");
		/**
		 * Ordering users by their total session time in descending order
		 */
		Dataset<Row> totalSessionTimePerUserOrdered = spark.sql("SELECT (total_session_time / 60 ) AS total_session_time_in_minutes, "
				+ "clientIPAddress, userAgent FROM totalSessionTimePerUser ORDER BY total_session_time_in_minutes DESC");
		totalSessionTimePerUserOrdered.show(false);
		/**
		 * Output most engaged user data
		 */
		totalSessionTimePerUserOrdered = totalSessionTimePerUserOrdered.coalesce(1);
		totalSessionTimePerUserOrdered.write().option("header", "true").mode(SaveMode.Overwrite).csv("results/most_engaged_users_with_session_time.csv");
		
    	spark.close();
    	
    }
}
