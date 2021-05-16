package main.spark;

public class AccessLog {

	private String requestStartTime;
	private String clientIPAddress;
	private String userAgent;
	private String url;
	
	public AccessLog(String requestStartTime, String clientIPAddress, String userAgent, String url) {
		super();
		this.requestStartTime = requestStartTime;
		this.clientIPAddress = clientIPAddress;
		this.userAgent = userAgent;
		this.url = url;
	}

	public String getRequestStartTime() {
		return requestStartTime;
	}

	public void setRequestStartTime(String requestStartTime) {
		this.requestStartTime = requestStartTime;
	}

	public String getClientIPAddress() {
		return clientIPAddress;
	}

	public void setClientIPAddress(String clientIPAddress) {
		this.clientIPAddress = clientIPAddress;
	}
	
	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
}
