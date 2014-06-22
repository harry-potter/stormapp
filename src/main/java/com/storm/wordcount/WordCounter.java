package com.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounter implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String, Integer> wordCounter;
	private OutputCollector collector;
	Integer id;
	String name;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	this.collector = collector;
	wordCounter = new HashMap<String, Integer>();
	id = context.getThisTaskId();
	name = context.getThisComponentId();
	}
	/**
	 * Increments the counter on each word
	 */
	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		
		if(wordCounter.containsKey(word)){
			Integer count = wordCounter.get(word) + 1;
			wordCounter.put(word, count);
		}else{
			wordCounter.put(word, 1);
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
	System.out.println("WordCounter["+name+"-"+id+"]");
	for(Map.Entry<String, Integer> entry : wordCounter.entrySet()){
		System.out.println("********"+entry.getKey() +":" + entry.getValue());
	}
		
	}

	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
