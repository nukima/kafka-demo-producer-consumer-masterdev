package ghtk.masterdev.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "manhnk9-java-producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.80.26:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //Create the producer
        final var producer = new KafkaProducer<String, String>(props);

        //Read csv file
        try {
                File data = new File("src/main/resources/customer-data.csv");
                Scanner scanner = new Scanner(data);
                //skip header
                scanner.nextLine();
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    //get id from line
                    String id = line.split(",")[0];
                    ProducerRecord<String, String> record = new ProducerRecord<>("customer", id, line);
                    producer.send(record);
                }
                scanner.close();
                //Close the producer
                producer.flush();
                producer.close();
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }


    }
}
