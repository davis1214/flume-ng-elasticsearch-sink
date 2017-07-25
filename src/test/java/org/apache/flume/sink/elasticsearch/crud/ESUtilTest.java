package org.apache.flume.sink.elasticsearch.crud;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESUtilTest {

	private static Client testClient = null;
	private static Client publicClient = null;
	private static Client payClient = null;
	private static final int DEFAULT_PORT = 9300;
	private static final Logger logger = Logger.getLogger(ESUtilTest.class);



	public final static String ES_PAY_HOSTS = "10.1.23.191,10.1.23.192,10.1.23.193,10.1.23.194,10.1.23.195";
	public final static String ES_PAY_CLUSTER_NAME = "elasticsearch_hd";


	static {
		init();
	}

	public static void main(String[] args) {
		Client client = ESUtilTest.getPayClient();
		if (client == payClient) {
			System.out.println("ddd");
		}

	}

	/**
	 * 初始化操作
	 */
	private static void init() {
		try {

			String[] payHost = StringUtils.deleteWhitespace(ES_PAY_HOSTS).split(",");
			payClient = openClient(payHost, ES_PAY_CLUSTER_NAME);

			logger.info("");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("***init es client error " + e.getMessage());
		}
	}

	/**
	 * 获取Client对象
	 * 
	 * @return
	 */
	public synchronized static Client getTestClient() {
		if (testClient == null) {
			init();
		}
		return testClient;
	}

	/**
	 * 获取Client对象
	 * 
	 * @return
	 */
	public synchronized static Client getPublicClient() {
		if (publicClient == null) {
			init();
		}
		return publicClient;
	}


	/**
	 * 获取Client对象
	 * 
	 * @return
	 */
	public synchronized static Client getPayClient() {
		if (payClient == null) {
			init();
		}
		return payClient;
	}

	/**
	 * 获取Client对象
	 * 
	 * @return
	 * @param client
	 */
	public synchronized static Client resetPayClient(Client client) {
		if (client != payClient && payClient != null) {
			return payClient;
		}
		if (payClient != null) {
			payClient.close();
		}
		String[] payHost = StringUtils.deleteWhitespace(ES_PAY_HOSTS).split(",");
		payClient = openClient(payHost, ES_PAY_CLUSTER_NAME);
		return payClient;
	}

	public static TransportClient openClient(String[] hostNames, String clusterName) {
		Settings settings = Settings.builder().put("cluster.name", clusterName)// 指定集群名称
				.put("client.transport.sniff", true)// 探测集群中机器状态
				.build();
		TransportClient client = new PreBuiltTransportClient(settings);

		hostNames = ES_PAY_HOSTS.split(",");
		
		try {
			InetSocketTransportAddress[] addresses = new InetSocketTransportAddress[hostNames.length];
			for (int i = 0; i < hostNames.length; i++) {
				addresses[i] = new InetSocketTransportAddress(InetAddress.getByName(hostNames[i]), 9300);
			}

			client.addTransportAddresses(addresses);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return client;
	}

}
