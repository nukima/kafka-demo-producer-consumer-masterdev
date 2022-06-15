package ghtk.masterdev.kafka;


import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws IOException {
        //Create the consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Manh-Java-Consumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.80.26:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create the consumer
        final var consumer = new KafkaConsumer<String, String>(props);

        //Create filtered-data csv file
        File filteredData = new File("src/main/resources/filtered-data.csv");
        CSVWriter csvWriter = new CSVWriter(new FileWriter(filteredData),
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.NO_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END);
        // with header
        csvWriter.writeNext(new String[]{"id", "num_oder", "name", "age", "tel"});
        csvWriter.close();
        //Subscribe to the topic
        consumer.subscribe(Collections.singleton("customer"));

        //Loop to consume the messages
        //noinspection InfiniteLoopStatement
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(1000);
            //check if there is new message
            if(records.count() > 0){
                //start to write to csv file
                filteredData = new File("src/main/resources/filtered-data.csv");
                csvWriter = new CSVWriter(new FileWriter(filteredData, true),
                        CSVWriter.DEFAULT_SEPARATOR,
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.NO_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);

                //filter records and write to csv
                for(ConsumerRecord<String, String> record : records) {
                    String[] line = record.value().split(",");
                    //parse to Customer object
                    Customer customer = new Customer(
                            Integer.parseInt(line[0]),
                            Integer.parseInt(line[1]),
                            Integer.parseInt(line[2]),
                            line[3]
                    );
                    //filter
                    if(customer.getAge() < 20 && customer.getNum_order() > 100 && isNotDuplicate(line[0])){
                        //write to csv
                        csvWriter.writeNext(new String[]{
                                String.valueOf(customer.getId()),
                                String.valueOf(customer.getNum_order()),
                                String.valueOf(customer.getAge()),
                                customer.getTel()}
                        );
                    }
                }
                //exort to csv
                csvWriter.close();
            }
        }
    }

    //check if the customer is duplicate in filtered-data.csv
    public static boolean isNotDuplicate(String id) throws FileNotFoundException {
        File filteredData = new File("src/main/resources/filtered-data.csv");
        FileReader fileReader = new FileReader(filteredData);
        //read every line
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                String[] lineArray = line.split(",");
                if(id.equals(lineArray[0])){
                    System.out.println("ID is existed");
                    return false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        return true;
    }

}

