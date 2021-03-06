package wangwei.papercode;



import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 *测试拓扑-1：拓扑结构简单
 * 拓扑结构：DataSourceSpout--Bolt1--Bolt2--Bolt3--Bolt4--Bolt5--Bolt6
 * 拓扑功能：
 * DataSourceSpout发射数据包（内容不限）；
 * Bolt1不作处理转发数据包，记录处理流量
 * Bolt2不作处理转发数据包，记录处理流量
 * Bolt3不作处理转发数据包，记录处理流量
 * Bolt4不作处理转发数据包，记录处理流量
 * Bolt5不作处理转发数据包，记录处理流量
 * Bolt6接受数据包后只记录接收流量
 */
public class ComplexTopology {
    //****************************************************************************************************
    // Java中int类型32位,4字节
    static final int tupleSize = 4;
    /**
     *DataSourceSpout--发射数据包（内容不限）；
     */
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        int tuple = 1;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            this.collector.emit(new Values(tuple++));
            System.out.println("Spout 发送： " + tuple +" 个tuple，共" + tuple * tupleSize + "字节");
            //*****************************************************************************************************
            // 控制发送速率，单位：毫秒
            Utils.sleep(2);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

            declarer.declare(new Fields("myField001"));
        }
    }

    /**
     * Bolt1不作处理转发数据包，记录处理流量
     */
    public static class Bolt1 extends BaseRichBolt {
        int tupleNum = 0;
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            Integer integer = input.getIntegerByField("myField001");
            System.out.println("Bolt1 共接收： " + tupleNum++ +" 个tuple，共" + tupleNum * tupleSize + "字节");
            collector.emit(new Values(integer)); //原样转发
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("myField002"));
        }
    }
    /**
     * Bolt2不作处理转发数据包，记录处理流量
     */
    public static class Bolt2 extends BaseRichBolt{
        int tupleNum = 0;
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            Integer integer = input.getIntegerByField("myField002");
            System.out.println("Bolt2 共接收： " + tupleNum++ +" 个tuple，共" + tupleNum * tupleSize + "字节");
            collector.emit(new Values(integer)); //原样转发
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("myField003"));
        }
    }
    /**
     * Bolt3不作处理转发数据包，记录处理流量
     */
    public static class Bolt3 extends BaseRichBolt{
        int tupleNum = 0;
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            Integer integer = input.getIntegerByField("myField003");
            System.out.println("Bolt3 共接收： " + tupleNum++ +" 个tuple，共" + tupleNum * tupleSize + "字节");
            collector.emit(new Values(integer)); //原样转发
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("myField004"));
        }
    }
    /**
     * Bolt4不作处理转发数据包，记录处理流量
     */
    public static class Bolt4 extends BaseRichBolt{
        int tupleNum = 0;
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            Integer integer = input.getIntegerByField("myField004");
            System.out.println("Bolt4 共接收： " + tupleNum++ +" 个tuple，共" + tupleNum * tupleSize + "字节");
            collector.emit(new Values(integer)); //原样转发
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("myField005"));
        }
    }
    /**
     * Bolt5不作处理转发数据包，记录处理流量
     */
    public static class Bolt5 extends BaseRichBolt{
        int tupleNum = 0;
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            Integer integer = input.getIntegerByField("myField005");
            System.out.println("Bolt5 共接收： " + tupleNum++ +" 个tuple，共" + tupleNum * tupleSize + "字节");
            collector.emit(new Values(integer)); //原样转发
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("myField006"));
        }
    }

    /**
     * Bolt6接受数据包后只记录接收流量
     */
    public static class Bolt6 extends BaseRichBolt{
        int tupleNum = 0;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        @Override
        public void execute(Tuple input) {
            input.getIntegerByField("myField006");
            System.out.println("Bolt6 共接收： " + tupleNum++ +" 个tuple，共" + tupleNum * tupleSize + "字节");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        //构建Topology，DataSourceSpout--Bolt1--Bolt2--Bolt3--Bolt4--Bolt5--Bolt6
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout", new DataSourceSpout());
        builder.setBolt("bolt1", new Bolt1()).shuffleGrouping("Spout");
        builder.setBolt("bolt2", new Bolt2()).shuffleGrouping("bolt1");
        builder.setBolt("bolt3", new Bolt3()).shuffleGrouping("bolt2");
        builder.setBolt("bolt4", new Bolt4()).shuffleGrouping("bolt3");
        builder.setBolt("bolt5", new Bolt5()).shuffleGrouping("bolt4");
        builder.setBolt("bolt6", new Bolt6()).shuffleGrouping("bolt5");

//        //创建一个本地的storm集群：本地模式运行，不需要搭建storm集群
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("simpleTopology", new Config(), builder.createTopology());

        //提交到集群上运行
        String topoName = ComplexTopology.class.getSimpleName();
        try {
            StormSubmitter.submitTopology(topoName, new Config(), builder.createTopology());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
