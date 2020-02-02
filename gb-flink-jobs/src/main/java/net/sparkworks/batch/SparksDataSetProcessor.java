package net.sparkworks.batch;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.DoubleStream;

public class SparksDataSetProcessor {
    
    public static void main(String[] args) throws Exception {
        
        
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        // Setup the connection settings to the RabbitMQ broker
/*
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(SparkConfiguration.brokerHost)
                .setPort(SparkConfiguration.brokerPort)
                .setUserName(SparkConfiguration.username)
                .setPassword(SparkConfiguration.password)
                .setVirtualHost(SparkConfiguration.brokerVHost)
                .build();
*/
        
        final String filename;
        Integer parallelism = null;
        try {
            // access the arguments of the command line tool
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/tmp/sensordata.csv";
                System.err.println("No filename specified. Please run 'WindowProcessor " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }
            
            if (params.has("parallelism")) {
                parallelism = params.getInt("parallelism");
            }
            
        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'WindowProcessor " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }
        
        if (Objects.nonNull(parallelism)) {
            env.setParallelism(parallelism);
        }
        
        final DataSource<String> stringDataSource = env.readTextFile(filename);
        
        final UnsortedGrouping<Tuple3<String, Double, Long>> groupedDataSource = stringDataSource
                .flatMap(new SparksSensorDataLineSplitter())
                .map(new TimestampMapFunction())
                .groupBy(0, 2);
        //.sortGroup(2, Order.ASCENDING);
        //groupedDataSource
        //        .aggregate(Aggregations.SUM, 1).andMax(1).andMin(1)
        //        .print();
        
        System.out.println("Min Reduction " +
                groupedDataSource
                        .reduceGroup(new MinGroupReduceFunction())
                        .count());
        
        System.out.println("Max Reduction " +
                groupedDataSource
                        .reduceGroup(new MaxGroupReduceFunction())
                        .count());
        
        System.out.println("Sum Reduction " +
                groupedDataSource
                        .reduceGroup(new SumGroupReduceFunction())
                        .count());
        
        System.out.println("Average Reduction " +
                groupedDataSource
                        .reduceGroup(new AverageGroupReduceFunction())
                        .count());
        
        
        System.out.println("Outliers Detection Reduction " +
                groupedDataSource
                        .reduceGroup(new OutliersDetectionGroupReduceFunction()).count());
        
        //        final JobExecutionResult jobExecutionResult = env.execute("SparkWorks DataSet Window Processor");
        
        System.out.println(String.format("SparkWorks DataSet Window Processor Job took: %d ms with parallelism: %d",
                env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
    }
    
    public static class SparksSensorDataLineSplitter implements FlatMapFunction<String, Tuple3<String, Double, Long>> {
        
        @Override
        public void flatMap(String line, Collector<Tuple3<String, Double, Long>> out) {
            
            String[] tokens = line.split("(,|;)\\s*");
            if (tokens.length != 3) {
                throw new IllegalStateException("Invalid record: " + line);
            }
            out.collect(new Tuple3<>(tokens[0], Double.parseDouble(tokens[2]), Long.parseLong(tokens[1])));
        }
        
    }
    
    public static class TimestampMapFunction implements
            MapFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>> {
        
        public final int DEFAULTWINDOWMINUTES = 5;
        
        public int windowMinutes;
        
        public TimestampMapFunction() {
            this.windowMinutes = DEFAULTWINDOWMINUTES;
        }
        
        public int getWindowMinutes() {
            return windowMinutes;
        }
        
        public void setWindowMinutes(final int windowMinutes) {
            this.windowMinutes = windowMinutes;
        }
        
        public Tuple3<String, Double, Long> map(Tuple3<String, Double, Long> value) throws Exception {
            Calendar local = Calendar.getInstance();
            local.setTimeInMillis(value.getField(2));
            local.set(Calendar.MILLISECOND, 0);
            local.set(Calendar.SECOND, 0);
            int minutes = local.get(Calendar.MINUTE) / getWindowMinutes();
            local.set(Calendar.MINUTE, getWindowMinutes() * minutes);
            value.setField(local.getTimeInMillis(), 2);
            return value;
        }
    }
    
    public static class SumGroupReduceFunction implements
            GroupReduceFunction<Tuple3<String, Double, Long>, Double> {
        
        @Override
        public void reduce(Iterable<Tuple3<String, Double, Long>> values, Collector<Double> out) {
            Double sum = 0d;
            for (Tuple3<String, Double, Long> t : values) {
                sum += (Double) t.getField(1);
            }
            out.collect(sum);
        }
        
    }
    
    public static class AverageGroupReduceFunction implements
            GroupReduceFunction<Tuple3<String, Double, Long>, Double> {
        
        @Override
        public void reduce(Iterable<Tuple3<String, Double, Long>> values, Collector<Double> out) {
            Double sum = 0d;
            long lenght = 0;
            for (Tuple3<String, Double, Long> t : values) {
                sum += (Double) t.getField(1);
                lenght++;
            }
            out.collect(sum / lenght);
        }
        
    }
    
    public static class MinGroupReduceFunction implements
            GroupReduceFunction<Tuple3<String, Double, Long>, Double> {
        
        @Override
        public void reduce(Iterable<Tuple3<String, Double, Long>> values, Collector<Double> out) {
            Double min = 0d;
            for (Tuple3<String, Double, Long> t : values) {
                min = Math.min(min, t.getField(1));
            }
            out.collect(min);
        }
        
    }
    
    public static class MaxGroupReduceFunction implements
            GroupReduceFunction<Tuple3<String, Double, Long>, Double> {
        
        @Override
        public void reduce(Iterable<Tuple3<String, Double, Long>> values, Collector<Double> out) {
            Double max = 0d;
            for (Tuple3<String, Double, Long> t : values) {
                max = Math.max(max, t.getField(1));
            }
            out.collect(max);
        }
        
    }
    
    public static class OutliersDetectionGroupReduceFunction implements
            GroupReduceFunction<Tuple3<String, Double, Long>, Double> {
        
        @Override
        public void reduce(Iterable<Tuple3<String, Double, Long>> values, Collector<Double> out) {
            DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
            for (Tuple3<String, Double, Long> sensorData : values) {
                descriptiveStatistics.addValue(sensorData.getField(1));
            }
            double std = descriptiveStatistics.getStandardDeviation();
            double lowerThreshold = descriptiveStatistics.getMean() - 2 * std;
            double upperThreshold = descriptiveStatistics.getMean() + 2 * std;
            
            DoubleStream.of(descriptiveStatistics.getValues()).boxed().forEach(sd -> {
                if (sd < lowerThreshold || sd > upperThreshold) {
                    System.out.println(String.format("Detected Outlier value: %s.", sd));
                    out.collect(sd);
                }
            });
            
        }
        
    }
    
}