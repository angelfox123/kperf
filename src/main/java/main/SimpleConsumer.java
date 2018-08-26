package main;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.UniformReservoir;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {
    public static void main(String[] args) {
        int expectedSpeed = Integer.valueOf(args[0]);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-perf-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("output"));

        consumer.poll(0);
        int recNum=0;

        Reservoir totalRes = new UniformReservoir();
        Reservoir secondRes = new UniformReservoir();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for(ConsumerRecord<String,String> record : records){
                long sendTime = Long.valueOf(record.value().split("-")[0]);
                long takeTime = System.nanoTime() - sendTime;
                if(recNum> 10000) {
                    totalRes.update(takeTime);
                    secondRes.update(takeTime);
                }
                recNum++;
                if(recNum % expectedSpeed ==0){
                    if(secondRes.getSnapshot().getMax()/1000000 > 100){
                        System.out.println("///////////////bad happen/////////////");
                        System.out.println("----"+Instant.now()+"----"+ recNum + "-------");
                        System.out.println("  mean: " + secondRes.getSnapshot().getMean()/1000000);
                        System.out.println("  75%: " + secondRes.getSnapshot().get75thPercentile()/1000000);
                        System.out.println("  99%: " + secondRes.getSnapshot().get99thPercentile()/1000000);
                        System.out.println("  99.9%: " + secondRes.getSnapshot().get999thPercentile()/1000000);
                        System.out.println("  Max: " + secondRes.getSnapshot().getMax()/1000000);
                    }
                    System.out.println("----"+Instant.now()+"----"+ recNum + "-------");
                    secondRes = new UniformReservoir();
                }
                if(recNum % (expectedSpeed * 30) == 0){
                    System.out.println("======="+Instant.now()+"========"+ recNum + "============");
                    System.out.println("  mean: " + totalRes.getSnapshot().getMean()/1000000);
                    System.out.println("  75%: " + totalRes.getSnapshot().get75thPercentile()/1000000);
                    System.out.println("  99%: " + totalRes.getSnapshot().get99thPercentile()/1000000);
                    System.out.println("  99.9%: " + totalRes.getSnapshot().get999thPercentile()/1000000);
                    System.out.println("  Max: " + totalRes.getSnapshot().getMax()/1000000);
                    System.out.println("========================================");
                    totalRes = new UniformReservoir();
                }
            };

        }

    }
}
