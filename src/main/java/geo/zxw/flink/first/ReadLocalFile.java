package geo.zxw.flink.first;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink基础练习
 * 在文件中写入数字，用行分隔。Flink读取文件数据作为DataStream初始数据，
 * 将获取的数据进行解析，String类型转为Integer类型。同时Flink窗口默认设定为1s中读取，
 * writeAsText()方法将每一秒读取的数据存放到一个文件中
 * print()方法则将所有数据输出。key（时间s）> value(值，转换后的数值)
 */
public class ReadLocalFile {

    public static void main(String[] args) throws Exception {

        //构造执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建初始数据
        DataStream<String> text = env.readTextFile("static/a");
        //数据转换
        DataStream<String> input = text;
        DataStream<Integer> parsed = input.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });
        //指定结果存放位置
        parsed.writeAsText("static/b");
        parsed.print();
        //触发执行程序
        env.execute();
    }
}
