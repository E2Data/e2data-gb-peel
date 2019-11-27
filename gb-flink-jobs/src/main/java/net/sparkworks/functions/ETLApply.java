package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * An implementation of an ETL using the Apply operator.
 *
 * <p>Note that this function requires that all data in the windows is buffered until the window
 * is evaluated, as the function provides no means of incremental aggregation.
 *
 * @author ichatz@gmail.com
 */
public class ETLApply implements WindowFunction<SensorData, SensorData, String, TimeWindow> {
    public void apply(String s, TimeWindow window, Iterable<SensorData> input, Collector<SensorData> out) throws Exception {
        SummaryStatistics summaryStatistics = new SummaryStatistics();
        int sum = 0;
        int count = 0;
        for (SensorData t : input) {
            sum += t.getValue();
            count++;
        }
        final SensorData outData = new SensorData();
        outData.setValue(sum / count);
        outData.setUrn(input.iterator().next().getUrn());
        outData.setTimestamp(input.iterator().next().getTimestamp());
        out.collect(outData);
    }
}
