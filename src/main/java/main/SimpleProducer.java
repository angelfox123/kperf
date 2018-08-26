package main;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import Util.BusyTimer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {
    public static void main(String[] args) {
        int time = Integer.valueOf(args[0]);
        int interval = Integer.valueOf(args[1]);
        int batch = Integer.valueOf(args[2]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-perf-test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(props);

        StringBuffer buffer = new StringBuffer();
        for(int i=0; i<10240; i++) {
            buffer.append('a');
        }
        String value = buffer.toString();

        final long speed = 1000000/interval;


        Runnable task = new Runnable() {
            int sendNum=0;
            @Override
            public void run() {

                for(int i=0; i<batch; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>("input", System.nanoTime() + "-" + value);
                    kafkaProducer.send(record);
                    sendNum++;
                }

                if(sendNum % (speed * batch) == 0){
                    System.out.println(System.currentTimeMillis() + " : " + sendNum);
                }
            }
        };

        BusyTimer timer = new BusyTimer(interval,time, task);
        timer.spaceMessageWithInterval();
    }
}
