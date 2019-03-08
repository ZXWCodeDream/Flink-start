package geo.zxw.flink.first;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 *
 * Flink入门例子：
 * 通过命令行窗口输入一行数据，统计每一个单词出现的个数。
 * 在10.111.32.222需要先启动netcat输入流，端口号9999
 * 在client中输入一行数据，每个单词以空格分离，统计次数
 *
 */

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        //Set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //build the input data and connect the origin client and execute FlatMap function to calculate
        DataStream<Tuple2<String,Integer>> input  = env.socketTextStream("10.111.32.222",9999)
                .flatMap(new Splitter()).keyBy(0).timeWindow(Time.seconds(5)).sum(1);
        input.print();
        env.execute("word count");
    }

    public static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
             for (String word : s.split(" ")){
                    collector.collect(new Tuple2<String, Integer>(word,1));
             }
        }
    }
}
