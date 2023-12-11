package com.stream.streamApp.config;

import com.kafka.producerapp.EmployeeAdressDetails;
import com.kafka.producerapp.EmployeeDetails;
import com.kafka.producerapp.EmployeePersonalDetails;
import com.kafka.producerapp.EmployeeVehicleDetails;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
@Service
public class StreamProcessing {

    @Autowired
    private EmployeeDetailsFactory getEmployeeDetails;


    public void processStream(){
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-stream-app");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        // Serde for keys and values
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        streamsConfiguration.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Avro for Employee class
        Serde<EmployeeAdressDetails> addressSerde = new SpecificAvroSerde<>();
        Serde<EmployeePersonalDetails> personalSerde = new SpecificAvroSerde<>();
        Serde<EmployeeVehicleDetails> vehicleSerde = new SpecificAvroSerde<>();


        //Stream builder

        StreamsBuilder builder = new StreamsBuilder();

        //Kstream -- define the input topics
        KStream<String, EmployeeAdressDetails> addressKStream = builder.stream("Employee_address_topic", Consumed.with(Serdes.String(), addressSerde));
        KStream<String, EmployeePersonalDetails> personalKStream = builder.stream("Employee_personal_topic", Consumed.with(Serdes.String(), personalSerde));
        KStream<String, EmployeeVehicleDetails> vehicleKStream = builder.stream("Employee_vehicle_topic", Consumed.with(Serdes.String(), vehicleSerde));
        personalKStream.peek((k,v)-> System.out.println(v.getEmployeeId()));

        //OuterJoin operations

        KStream<Integer, EmployeeDetails> outerJoinedStream=personalKStream.outerJoin(
                        addressKStream,
                        EmployeeDetailsFactory::setEmployeeDetails,
                        JoinWindows.of(1000))
                .selectKey((k,v) -> v.getEmployeeId())
                .groupByKey()
                .aggregate(EmployeeDetails::new,
                        (aggKey,oldValue,newValue) -> getEmployeeDetails.aggregateSet(oldValue,newValue),
                        Materialized.as("queryable-store-name"))
                .toStream();

        System.out.println("Stream After Outer-Join Operation:"+outerJoinedStream);

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

    }
}
