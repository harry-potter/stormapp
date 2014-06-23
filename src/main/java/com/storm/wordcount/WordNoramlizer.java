package com.storm.wordcount;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



public class WordNoramlizer implements IRichBolt {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Value is read from the tuple by position. After processing of each tuple
	 * ack() is invoked.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				@SuppressWarnings("rawtypes")
				List list = new ArrayList();
				list.add(input);
				collector.emit(list, new Values(word));
			}
		}
		collector.ack(input);
	}
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


}
