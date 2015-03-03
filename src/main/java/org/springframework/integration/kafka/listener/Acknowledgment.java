package org.springframework.integration.kafka.listener;

/**
 * Handle for acknowledging a Kafka Message.
 *
 * @author Marius Bogoevici
 */
public interface Acknowledgment {

	void acknowledge();

}
