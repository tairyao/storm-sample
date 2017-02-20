package com.tencent.tairyao.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.tencent.tairyao.storm.bolt.ExclamationBolt;
import com.tencent.tairyao.storm.bolt.SaveMysqlBolt;
import com.tencent.tairyao.storm.spout.TestWordSpout;

/**
 * Created by tairyao on 2017/1/15.
 */
public class ExclamationTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestWordSpout(), 3);     //每次插入3条数据
        builder.setBolt("bolt1", new ExclamationBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("bolt2", new SaveMysqlBolt(), 2).shuffleGrouping("bolt1");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
//            Utils.sleep(10000);
//            cluster.killTopology("test");
//            cluster.shutdown();
        }
    }
}
