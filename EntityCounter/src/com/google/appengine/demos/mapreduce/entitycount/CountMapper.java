package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Counts occurrences of characters in the key and the "payload" property of datastore entities.
 * The output key is a human-readable description of the property, the value is the number of
 * occurrences.
 *
 * @author 
 */
class CountMapper extends Mapper<Entity, String, Long> {

  private static final long serialVersionUID = 4973057382538885270L;

  private void incrementCounter(String name, long delta) {
    getContext().getCounter(name).increment(delta);
  }

 
  private void mapWOWs(String name, String s) {
	  String[] splited = s.split(" ");
	  HashMap<String, Integer> counts = new HashMap<>();
    
    // here split by word
    for (int i = 0; i < splited.length; i++) {
      String word = splited[i];
      if(word.endsWith("wow")){
    	  incrementCounter("total WOWs", 1);
          Integer count = counts.get(word);
          if (count == null) {
            counts.put(word, 1);
          } else {
            counts.put(word, count + 1);
          }
    	  
      }
    }
    for (Entry<String, Integer> kv : counts.entrySet()) {
    	// key is entity name and value is number of occurences
      emit(name, Long.valueOf(kv.getValue()));
    }
  }
  
  @Override
  public void map(Entity entity) {
    incrementCounter("total entities", 1);
    incrementCounter("map calls in shard " + getContext().getShardNumber(), 1);

    String name = entity.getKey().getName();
    if (name != null) {
      incrementCounter("total entity key size", name.length());
     }

   Text property = (Text) entity.getProperty("payload");
   if (property != null) {
     incrementCounter("total entity payload size", property.getValue().length());
     mapWOWs(name, property.getValue()) ;
   }
  }
}
