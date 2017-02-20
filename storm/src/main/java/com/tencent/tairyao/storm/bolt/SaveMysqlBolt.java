package com.tencent.tairyao.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.tencent.tairyao.storm.util.MysqlUtil;

import java.util.Map;

/**
 * Created by tairyao on 2017/2/11.
 */
public class SaveMysqlBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private MysqlUtil mysqlUtil;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        this.mysqlUtil = new MysqlUtil();
    }

    @Override
    public void execute(Tuple tuple) {
        String time = tuple.getStringByField("time");
        int value = tuple.getIntegerByField("value");
        String sql = "insert into storm_bolt (time, value) values (?, ?)";
        mysqlUtil.insertResult(sql, time, value);
        this._collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("name", "value"));
    }

}
