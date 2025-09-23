package org.acme.kafka.consumer;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class KafkaConsumer {

    @Broadcast
    @Incoming("source")
    @Outgoing("target")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public String processAndProduceAuditMessage(Message<byte[]> incoming) {
      // do process
    	return getAuditMessage(incoming);
    }

    @Incoming("target")
    public void print(String processed) {
        Log.info(processed);
    }

    private String getAuditMessage(Message<byte[]> incoming) throws RuntimeException {
    	IncomingKafkaRecordMetadata<?, ?> record = incoming.getMetadata(IncomingKafkaRecordMetadata.class)
        .orElseThrow(RuntimeException::new);
      return String.format("Processed message on partition %d at offset %d", record.getPartition(), record.getOffset());
    }

}