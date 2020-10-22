package net.sparkworks.batch;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.DoubleStream;

public class SparksDataSetProcessor {
    
    public static void main(String[] args) throws Exception {
        
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
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
                .map(new SparksSensorDataLineSplitterMapFunction())
                .map(new TimestampMapFunction())
                .groupBy(0, 2);
        
        System.out.println("Min Reduction of Device, TimeWindow pairs #: " +
                groupedDataSource.reduceGroup(new MinGroupReduceFunction()).count());
    
        long jobRuntime = env.getLastJobExecutionResult().getJobExecutionResult()
                             .getNetRuntime(TimeUnit.MILLISECONDS);
        
        System.out.println(String.format("Min Reduction: %d ms", jobRuntime));
        
        long totalRuntime = jobRuntime;

/*
        System.out.println("Max Reduction of Device, TimeWindow pairs #: " +
                groupedDataSource
                        .reduceGroup(new MaxGroupReduceFunction())
                        .count());

        jobRuntime = env.getLastJobExecutionResult()
                             .getNetRuntime(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Max Reduction: %d ms", jobRuntime));
        totalRuntime += jobRuntime;

        System.out.println("Sum Reduction of Device, TimeWindow pairs #: " +
                groupedDataSource
                        .reduceGroup(new SumGroupReduceFunction())
                        .count());

        jobRuntime = env.getLastJobExecutionResult()
                        .getNetRuntime(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Sum Reduction: %d ms", jobRuntime));
        totalRuntime += jobRuntime;

        System.out.println("Average Reduction of Device, TimeWindow pairs #: " +
                groupedDataSource
                        .reduceGroup(new AverageGroupReduceFunction())
                        .count());

        jobRuntime = env.getLastJobExecutionResult()
                        .getNetRuntime(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Average Reduction: %d ms", jobRuntime));
        totalRuntime += jobRuntime;

        System.out.println("Outliers Detection Reduction of Device, TimeWindow pairs #: " +
                groupedDataSource
                        .reduceGroup(new OutliersDetectionGroupReduceFunction()).count());

        jobRuntime = env.getLastJobExecutionResult()
                        .getNetRuntime(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Outliers Detection Reduction: %d ms", jobRuntime));
        totalRuntime += jobRuntime;

        //        final JobExecutionResult jobExecutionResult = env.execute("SparkWorks DataSet Window Processor");
*/

        System.out.println(String.format("SparkWorks DataSet Window Processor Job took: %d ms with parallelism: %d",
                                         totalRuntime, env.getParallelism()));
    }
    
    public static class SparksSensorDataLineSplitterMapFunction
            implements MapFunction<String, Tuple3<String, Double, Long>> {
        
        @Override
        public Tuple3<String, Double, Long> map(String line) {
            String[] tokens = line.split("(,|;)\\s*");
            if (tokens.length != 3) {
                throw new IllegalStateException("Invalid record: " + line);
            }
            return new Tuple3<>(tokens[0], Double.parseDouble(tokens[2]), Long.parseLong(tokens[1]));
        }
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
        
        public final int DEFAULT_WINDOW_MINUTES = 5;
        
        public int windowMinutes;
        
        public TimestampMapFunction() {
            this.windowMinutes = DEFAULT_WINDOW_MINUTES;
        }
        
        public int getWindowMinutes() {
            return windowMinutes;
        }
        
        public void setWindowMinutes(final int windowMinutes) {
            this.windowMinutes = windowMinutes;
        }
        
        public Tuple3<String, Double, Long> map(Tuple3<String, Double, Long> value) {
            LocalDateTime timestamp =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getField(2)),
                            ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
            int minute = Math.floorDiv(timestamp.get(ChronoField.MINUTE_OF_HOUR), getWindowMinutes());
            timestamp.with(ChronoField.MINUTE_OF_HOUR, minute);
            return new Tuple3<>(value.getField(0),
                    value.getField(1),
                    timestamp.toInstant(ZoneOffset.UTC).toEpochMilli());
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
            GroupReduceFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>> {
        
        @Override
        public void reduce(Iterable<Tuple3<String, Double, Long>> values,
                           Collector<Tuple3<String, Double, Long>> out) {
            Double min = 0d;
            String device = null;
            Long timestampWindow = null;
            for (Tuple3<String, Double, Long> t : values) {
                min = Math.min(min, t.getField(1));
                device = t.getField(0);
                timestampWindow = t.getField(2);
            }
            Objects.requireNonNull(device);
            Objects.requireNonNull(timestampWindow);
            out.collect(new Tuple3<>(device, min, timestampWindow));
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