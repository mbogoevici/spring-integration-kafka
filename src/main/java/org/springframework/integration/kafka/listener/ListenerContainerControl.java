package org.springframework.integration.kafka.listener;

import org.springframework.integration.kafka.core.Partition;

/**
 * @author Marius Bogoevici
 */
public interface ListenerContainerControl {

	void startListening(Partition partition);

	void stopListening(Partition partition, long timeout);

}
