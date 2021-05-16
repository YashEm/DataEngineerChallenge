package main.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements function for parsing Access Log records as per the 
 * AWS Specifications provided here: "https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format"
 * @author yash.chawada
 *
 */
public class AccessLogParser implements MapPartitionsFunction<Row, AccessLog> {

	private static final long serialVersionUID = 1L;
	private Logger logger = LoggerFactory.getLogger(AccessLogParser.class);
	
	public AccessLogParser() {
	}

	@Override
	public Iterator<AccessLog> call(Iterator<Row> itr) throws Exception {
		List<AccessLog> list = new ArrayList<AccessLog>();
		while(itr.hasNext()) {
			String record = itr.next().getString(0).trim();
			String[] fields = record.split(" ");
			try {
				AccessLog accessLog = new AccessLog(fields[0].trim(), fields[2].trim(), getUserAgent(record), 
						getURL(fields));
				list.add(accessLog);
			} catch (Exception e) {
				logger.info("Error in parsing Log, record {}, Exception {}",
						record, ExceptionUtils.getStackTrace(e));
			}
		}
 		return list.iterator();
	}
	
	/**
	 * Getting value of "user_agent" from log record
	 * @param value
	 * @return userAgent
	 */
	private String getUserAgent(String record) {
		String[] fields = record.split("\" \"");
		String userAgent = fields[1].split("\" ")[0];
		return userAgent;
	}

	/**
	 * Filter invalid URLs and return correct URL values
	 * @param values
	 * @return request URL
	 */
	private String getURL(String[] values) {
		if(!values[12].equalsIgnoreCase("-")) {
			return values[12];
		}
		return null;
	}
	
	
}
