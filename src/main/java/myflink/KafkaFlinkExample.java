package myflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.File;
import java.util.Properties;


public class KafkaFlinkExample {

    public static void main(String[] args) throws Exception {

        /*
        * bootstrap_servers=f"{host}:{sasl_port}",
   sasl_mechanism="PLAIN",
   sasl_plain_password=password,
   sasl_plain_username=username,
   security_protocol="SASL_SSL",
   ssl_cafile="ca.crt",
        * */


        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-2841cbd0-kth-3cbd.aivencloud.com:16173");
        props.put("sasl_mechanism", "PLAIN");
        props.put("sasl_plain_password", "cjynhlerx4rbumtw");
        props.put("sasl_plain_username", "avnadmin");
        props.put("security_protocol", "SASL_SSL");

        File file = new File("src/main/resources/ca.pem");
        String path = file.getAbsolutePath();
        props.put("ssl_cafile", path);

//        props.put("security.protocol", "SSL");
//        props.put("ssl.endpoint.identification.algorithm", "");
//        props.put("ssl.truststore.location", "client.truststore.jks");
//        props.put("ssl.truststore.password", "secret");
//        props.put("ssl.keystore.type", "PKCS12");
//        props.put("ssl.keystore.location", "client.keystore.p12");
//        props.put("ssl.keystore.password", "secret");
//        props.put("ssl.key.password", "secret");
//        props.put("group.id", "test-flink-input-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test-flink-input", new SimpleStringSchema(), props);
        //DataStream<String> stringInputStream = environment.addSource(consumer);

        DataStream<String> stringInputStream = environment.socketTextStream("localhost", 9999);


        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("test-flink-output", new SimpleStringSchema(), props);
        stringInputStream.map(new WordsCapitalizer()).addSink(producer);
        environment.execute("FlinkExample");

    }
}


class WordsCapitalizer implements MapFunction<String, String> {
    public String map(String s) {
        return s.toUpperCase();
    }
}
