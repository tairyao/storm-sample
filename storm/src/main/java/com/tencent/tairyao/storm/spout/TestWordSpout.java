package com.tencent.tairyao.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.tencent.tairyao.storm.util.MysqlUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tairyao on 2017/1/15.
 */
public class TestWordSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private MysqlUtil mysqlUtil;
    private static int n = 0;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        this.mysqlUtil = new MysqlUtil();
    }

    public void nextTuple() {
        Utils.sleep(1000);
        String sql = "select * from storm_spout limit " + n + ", 1";
        HashMap<String, Object> result = mysqlUtil.getResult(sql);
        String time = (String) result.get("time");
        int value = (Integer) result.get("success");
        _collector.emit(new Values(time, value));   //从这个spout发射一个tuple
        if (n>=2) {
            n = 0;
        } else {
            n++;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "value"));  //声明发射的tuple有哪些列
    }
}
