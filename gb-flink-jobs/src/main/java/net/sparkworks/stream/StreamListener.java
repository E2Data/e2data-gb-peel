package net.sparkworks.stream;

import net.sparkworks.SparkConfiguration;
import net.sparkworks.functions.SensorDataMapFunction;
import net.sparkworks.model.SensorData;
import net.sparkworks.out.RMQOut;
import net.sparkworks.serialization.SensorDataSerializationSchema;
import net.sparkworks.util.RBQueue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * A simple flink stream processing engine that connects to the SparkWorks message broker
 * and outputs the messages received without any further processing.
 *
 * @author ichatz@gmail.com
 */
public class StreamListener {

    public static void main(String[] args) throws Exception {

        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Setup the connection settings to the RabbitMQ broker
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(SparkConfiguration.brokerHost)
                .setPort(SparkConfiguration.brokerPort)
                .setUserName(SparkConfiguration.username)
                .setPassword(SparkConfiguration.password)
                .setVirtualHost(SparkConfiguration.brokerVHost)
                .build();

        final DataStream<String> rawStream = env
                .addSource(new RBQueue<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        SparkConfiguration.queue, // name of the RabbitMQ queue to consume
                        true,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema())); // deserialization schema to turn messages into Java objects

        final DataStream<SensorData> dataStream = // convert RabbitMQ messages to SensorData
                rawStream.map(new SensorDataMapFunction());

        if (SparkConfiguration.doOutput) {
            dataStream.addSink(new RMQOut<SensorData>(connectionConfig, SparkConfiguration.outExchange, new SensorDataSerializationSchema()));
        }
        // print the results with a single thread, rather than in parallel
        dataStream.print().setParallelism(1);

        env.execute("SparkWorks Stream Listener");
    }

}
