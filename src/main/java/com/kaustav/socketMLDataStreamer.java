package com.kaustav;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.parser.LongParser;
import org.apache.flink.types.parser.LongValueParser;
import org.apache.flink.util.Collector;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityUuid;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

//import org.apache.ignite.sink.flink.IgniteSink;
//import org.apache.ignite.sink.flink.IgniteSink;

public class socketMLDataStreamer {

    private static final String ID_TMPL = "%s,%s";
    static IgniteSink igniteSink;
    static long counter = 9100000000L;
    static IgniteCache<Long, Long> stmCache2;
    private static final String CACHE_NAME2 = "maxSeqCache";
    public static void main(String[] args) throws Exception {
        /** Ignite test configuration file. */
        final String GRID_CONF_FILE = "/home/system/FLINK/flink-ignite-tf/config/example-ignite.xml";


        Ignite ignite = Ignition.start("config/example-ignite.xml");
        CacheConfiguration<Long, Long> cfg2 = new CacheConfiguration<>(CACHE_NAME2);
        stmCache2 = ignite.getOrCreateCache(cfg2);
        cfg2.setCacheMode(CacheMode.PARTITIONED);
        cfg2.setIndexedTypes(Long.class, Long.class);






        igniteSink = new IgniteSink("mlCAche001", GRID_CONF_FILE);

        igniteSink.setAllowOverwrite(true);

        igniteSink.setAutoFlushFrequency(5L);

        Configuration p = new Configuration();
        igniteSink.open(p);


        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStream<String> text = env.socketTextStream("localhost", 12000);


        final int windowSize = 30;
        final int slideSize = 10;

        // parse the data, group it, window it, and aggregate the counts
        SingleOutputStreamOperator<Map<Long, String>> windowCounts = text
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.of(2500, MILLISECONDS), Time.of(2500, MILLISECONDS))
                .sum(1)
                .map(new Mapper());


        //windowCounts.print();


        windowCounts.addSink(igniteSink);





        //System.out.println("Done");
        env.execute("Window WordCount");
    }





    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = value.split(";");
            Tuple2<String, Integer> data = new Tuple2<>();
            //String mlData;
            if (tokens.length > 0) {
                //long i=1000;
                //Persona person1 = new Persona(Long.parseLong(tokens[0]),tokens[1],30);
                //person.Person_setter(i,tokens[1],30);


                String mlData = tokens[0] +"^"+tokens[1] +"^"+ tokens[2]+"^"+tokens[3] +"^"+ tokens[4]+"^"+tokens[5] +"^"+ tokens[6]+"^"+tokens[7] +"^"+ tokens[8];
                //data.put(String.format(ID_TMPL, id, tokens[i]), tokens[i + 1]);
                //data.put(i,mlData);

                collector.collect(new Tuple2<String, Integer>(mlData, 1));


            }


        }

    }

    public static class Mapper implements MapFunction<Tuple2<String, Integer>, Map<Long, String>> {
        @Override
        public Map<Long, String> map(Tuple2<String, Integer> tuple2) throws Exception {
            Map<Long, String> myWordMap = new HashMap<>();

            myWordMap.put(counter++, tuple2.f0);
            //stmCache2.put(counter,counter);
            return myWordMap;

        }
    }



    public static class countIncr implements MapFunction<String, String> {
        @Override
        public String map(String IN) throws Exception {
            String myWordTxt = IN;
            counter++;

             return myWordTxt;
        }
    }


    }




