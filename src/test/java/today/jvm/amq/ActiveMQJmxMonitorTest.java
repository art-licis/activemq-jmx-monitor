package today.jvm.amq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import javax.management.ObjectInstance;

import static org.junit.Assert.*;

public class ActiveMQJmxMonitorTest {
	private static final int CONSUMER_PREFETCH_SIZE = 2;

	private BrokerService brokerService;
	private ActiveMQJmxMonitor activeMQJmxMonitor;

	private static class CountingJmxMonitorListener implements ActiveMQJmxMonitor.JmxMonitorListener {
		protected int slowConsumers;
		protected int blockedProducers;

		public int getSlowConsumers() {
			return slowConsumers;
		}

		public int getBlockedProducers() {
			return blockedProducers;
		}

		@Override
		public void onEvent(ActiveMQJmxMonitor.NotificationType notificationType, ActiveMQJmxMonitor.State state, ObjectInstance objectInstance) {
			if (notificationType == ActiveMQJmxMonitor.NotificationType.SLOW_CONSUMER) {
				if (state == ActiveMQJmxMonitor.State.START) slowConsumers++;
				else if (state == ActiveMQJmxMonitor.State.END) slowConsumers--;
			} else if (notificationType == ActiveMQJmxMonitor.NotificationType.PRODUCER_BLOCKED) {
				if (state == ActiveMQJmxMonitor.State.START) blockedProducers++;
				else if (state == ActiveMQJmxMonitor.State.END) blockedProducers--;
			}
		}
	}

	private static class DelayMessageListener implements MessageListener {
		private long delay;

		public DelayMessageListener(long delay) {
			this.delay = delay;
		}

		public void setDelay(long delay) {
			this.delay = delay;
		}

		@Override
		public void onMessage(Message message) {
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Before
	public void startBroker() throws Exception {
		brokerService = new BrokerService();
		brokerService.setPersistent(false);
		brokerService.setUseJmx(true);
		brokerService.addConnector("tcp://localhost:61616");

		PolicyEntry entry = new PolicyEntry();
		entry.setPendingMessageLimitStrategy(new ConstantPendingMessageLimitStrategy() {
			@Override
			public int getLimit() {
				return 0;
			}
		});
		PolicyMap map = new PolicyMap();
		map.setDefaultEntry(entry);
		brokerService.setDestinationPolicy(map);
		brokerService.start();
		brokerService.waitUntilStarted();

		activeMQJmxMonitor = new ActiveMQJmxMonitor("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi", "localhost", null);
	}

	@After
	public void stopBroker() throws Exception {
		if (brokerService != null) {
			brokerService.stop();
			brokerService.waitUntilStopped();
		}
	}

	@Test
	public void testSlowConsumerNotification() throws Exception {
		CountingJmxMonitorListener countingJmxMonitorListener = new CountingJmxMonitorListener();
		activeMQJmxMonitor.addListener(countingJmxMonitorListener);

		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(" + brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString() + ")");

		// consumer
		Connection consumerConnection = connectionFactory.createConnection();
		consumerConnection.start();
		Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSession.createConsumer(consumerSession.createTopic("TOPIC.EXAMPLE?consumer.prefetchSize=" + CONSUMER_PREFETCH_SIZE));
		DelayMessageListener delayMessageListener = new DelayMessageListener(30);
		consumer.setMessageListener(delayMessageListener);

		// producer
		Connection producerConnection = connectionFactory.createConnection();
		producerConnection.start();
		Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = producerSession.createProducer(producerSession.createTopic("TOPIC.EXAMPLE"));

		activeMQJmxMonitor.checkUpdates();
		assertEquals(0, countingJmxMonitorListener.getSlowConsumers());
		assertEquals(0, countingJmxMonitorListener.getBlockedProducers());

		for (int i = 0; i < 100; i++) {
			producer.send(producerSession.createTextMessage("hello: " +  i));
			Thread.sleep(1);
		}

		activeMQJmxMonitor.checkUpdates();

		assertEquals(1, countingJmxMonitorListener.getSlowConsumers());
		assertEquals(0, countingJmxMonitorListener.getBlockedProducers());

		delayMessageListener.setDelay(0);
		Thread.sleep(500);
		for (int i = 0; i < 100; i++) {
			producer.send(producerSession.createTextMessage("hello: " +  i));
			Thread.sleep(1);
		}
		Thread.sleep(500);

		activeMQJmxMonitor.checkUpdates();

		assertEquals(0, countingJmxMonitorListener.getSlowConsumers());
		assertEquals(0, countingJmxMonitorListener.getBlockedProducers());
	}
}
