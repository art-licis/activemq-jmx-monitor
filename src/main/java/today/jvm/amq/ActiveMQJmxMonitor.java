package today.jvm.amq;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.*;

/**
 * Simple ActiveMQ JMX Monitor. Provides notifications about slow consumers
 * and blocked producers. Unlike advisory messages, it will notify when consumers
 * stop being slow, or producers stop being blocked; and the actual state
 * will be built with the first call.
 *
 * This class will hold state and notify listeners whenever state change happens,
 * based on results of JMX queries.
 *
 * Current version will query all topics and queues, including Advisory topics.
 *
 * This can be wrapped into a separate thread to perform periodic checks for updates
 * (i.e., invoke {@link #checkUpdates()}).
 *
 * @author Arturs Licis
 */
public class ActiveMQJmxMonitor {
	public static enum NotificationType {
		SLOW_CONSUMER,
		PRODUCER_BLOCKED
	}

	public static enum State {
		START,
		END
	}

	public static interface JmxMonitorListener {
		void onEvent(NotificationType notificationType, State state, ObjectInstance objectInstance);
	}

	private static QueryExp SLOW_CONSUMER_EXP = Query.eq(Query.attr("SlowConsumer"), Query.value(true));
	private static QueryExp PRODUCER_BLOCKED_EXP = Query.eq(Query.attr("ProducerBlocked"), Query.value(true));

	private Set<ObjectInstance> slowConsumers = new HashSet<>();
	private Set<ObjectInstance> blockedProducers = new HashSet<>();

	private ObjectName consumersObjectName;
	private ObjectName producersObjectName;

	private MBeanServerConnection mBeanConnection;

	private List<JmxMonitorListener> listeners = new LinkedList<>();

	public ActiveMQJmxMonitor(String jmxUrl, String brokerName, String[] credentials) throws IOException, MalformedObjectNameException {
		Map<String, String[]> env = new HashMap<>();
		if (credentials != null) {
			env.put(JMXConnector.CREDENTIALS, credentials);
		}
		JMXServiceURL jmxServiceUrl = new JMXServiceURL(jmxUrl);
		JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceUrl, env);

		mBeanConnection = jmxConnector.getMBeanServerConnection();
		initObjectNames(brokerName);
	}

	/**
	 * This is a master method to check for updates.
	 *
	 * @throws IOException
	 */
	public void checkUpdates() throws IOException {
		querySlowConsumers();
		queryBlockedProducers();
	}


	private void initObjectNames(String brokerName) throws MalformedObjectNameException {
		consumersObjectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName + ",destinationType=*,destinationName=*,endpoint=Consumer,clientId=*,consumerId=*");
		producersObjectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName + ",destinationType=*,destinationName=*,endpoint=Producer,clientId=*,producerId=*");
	}

	public void addListener(JmxMonitorListener listener) {
		listeners.add(listener);
	}

	public void removeListener(JmxMonitorListener listener) {
		listeners.remove(listener);
	}

	private void notifyListeners(NotificationType notificationType, State state, ObjectInstance objectInstance) {
		for (JmxMonitorListener listener : listeners) {
			listener.onEvent(notificationType, state, objectInstance);
		}
	}

	protected void query(Set<ObjectInstance> currentInstances, Set<ObjectInstance> queryResultInstances, NotificationType notificationType) throws IOException {
		Set<ObjectInstance> removed = new HashSet<>();
		for (ObjectInstance old : currentInstances) {
			if (!queryResultInstances.contains(old)) {
				removed.add(old);
				notifyListeners(notificationType, State.END, old);
			}
		}
		currentInstances.removeAll(removed);

		for (ObjectInstance objectInstance : queryResultInstances) {
			if (!currentInstances.contains(objectInstance)) {
				currentInstances.add(objectInstance);
				notifyListeners(notificationType, State.START, objectInstance);
			}
		}
	}

	protected void querySlowConsumers() throws IOException {
		Set<ObjectInstance> objectInstances = mBeanConnection.queryMBeans(consumersObjectName, SLOW_CONSUMER_EXP);
		query(slowConsumers, objectInstances, NotificationType.SLOW_CONSUMER);
	}

	protected void queryBlockedProducers() throws IOException {
		Set<ObjectInstance> objectInstances = mBeanConnection.queryMBeans(producersObjectName, PRODUCER_BLOCKED_EXP);
		query(blockedProducers, objectInstances, NotificationType.PRODUCER_BLOCKED);
	}
}
