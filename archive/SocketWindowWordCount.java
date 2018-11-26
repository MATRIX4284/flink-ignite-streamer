package com.samaitra;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
//import org.apache.ignite.sink.flink.IgniteSink;
//import org.apache.ignite.sink.flink.IgniteSink;


public class SocketWindowWordCount {

    private static final String ID_TMPL = "%s,%s";

    public static void main(String[] args) throws Exception {
        /** Ignite test configuration file. */
        final String GRID_CONF_FILE = "/home/system/FLINK/flink-ignite/config/example-ignite.xml";



        IgniteSink igniteSink = new IgniteSink("kaustav9", GRID_CONF_FILE);

        igniteSink.setAllowOverwrite(true);

        igniteSink.setAutoFlushFrequency(5L);

        Configuration p = new Configuration();
        igniteSink.open(p);


        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        DataStream<String> text = env.socketTextStream("localhost", 12000);

        // parse the data, group it, window it, and aggregate the counts
        SingleOutputStreamOperator<Map<String, String>> windowCounts = text.flatMap(new Splitter());

        // print the results with a single thread, rather than in parallel

        windowCounts.print();

        windowCounts.addSink(igniteSink);

        //IgniteSink.streamer.addData((Map)windowCounts);

        //windowCounts.print();
        //System.out.println("Done");
        env.execute("Window WordCount");
    }

    public static final class Splitter implements FlatMapFunction<String, Map<String, String>> {

        @Override
        public void flatMap(String value, Collector<Map<String, String>> collector) throws Exception {
            String[] tokens = value.split(";");
            Map<String, String> data = new HashMap<>();
            if (tokens.length > 1) {
                String id = tokens[1];
                if (id != null) {
                    for (int i = 2; i < tokens.length - 1; i += 2) {
                        if (tokens[i] != null && tokens[i + 1] != null) {
                            //data.put(String.format(ID_TMPL, id, tokens[i]), tokens[i + 1]);
                            data.put(String.format(ID_TMPL, id , tokens[i + 1]), tokens[i + 1]);
                        }
                    }
                }
            }
            if (data.size() > 0) {
                collector.collect(data);
            }
        }
    }

}