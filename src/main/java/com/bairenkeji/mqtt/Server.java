package com.bairenkeji.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * 
 * Title:Server Description: �����������ͻ����������⣬����ͬ�ͻ��˿��������������ͬ����
 * 
 * @author chenrl 2016��1��6������3:29:28
 */
public class Server {

	public static final String HOST = "tcp://123.56.233.121:1883";
	public static final String TOPIC = "mqtt/test";
	public static final String TOPIC125 = "toclient/2";
	private static final String clientid = "server";

	private MqttClient client;
	private static MqttTopic topic;
	private static MqttTopic topic125;
	private String userName = "";
	private String passWord = "";

	public Server() throws MqttException {
		// MemoryPersistence����clientid�ı�����ʽ��Ĭ��Ϊ���ڴ汣��
		client = new MqttClient(HOST, clientid, new MemoryPersistence());
		connect();
	}

	private void connect() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCleanSession(false);
		// options.setUserName(userName);
		// options.setPassword(passWord.toCharArray());
		// ���ó�ʱʱ��
		options.setConnectionTimeout(10);
		// ���ûỰ����ʱ��
		options.setKeepAliveInterval(20);
		try {
			client.setCallback(new PushCallback());
			client.connect(options);
			topic = client.getTopic(TOPIC);
			topic125 = client.getTopic(TOPIC125);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void publish(MqttTopic topic, MqttMessage message) throws MqttPersistenceException, MqttException {
		MqttDeliveryToken token = topic.publish(message);
		token.waitForCompletion();
		System.out.println("message is published completely! " + token.isComplete());
	}

	public static void batchPublish(MqttTopic topic) throws MqttPersistenceException, MqttException {

		for (int i = 0; i < 200000; i++) {

			MqttMessage message = new MqttMessage();
			message.setQos(2);
			message.setRetained(true);
			message.setPayload(("���ͻ���1���͵���Ϣ[" + i + "]").getBytes());
			MqttDeliveryToken token = topic.publish(message);
			token.waitForCompletion();
			System.out.println("message [" + i + "] is published completely! " + token.isComplete());
			System.out.println(message.isRetained() + "------ratained״̬");
		}

	}

	public static void main(String[] args) throws MqttException {

		new Server();
		batchPublish(topic);

	}
}