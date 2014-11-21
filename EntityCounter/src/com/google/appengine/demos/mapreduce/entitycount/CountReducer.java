package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerInput;

/**
 * Sums a list of numbers. The key identifies a hotel and values are found
 * occurrences of 'wow'. The output value is the sum of all input values for
 * the given key, i.e. the total amount of 'wow's.
 *
 * @author 
 */
class CountReducer extends Reducer<String, Long, KeyValue<String, Long>> {

  private static final long serialVersionUID = 1316637485625852869L;

  @Override
  public void reduce(String key, ReducerInput<Long> values) {
    long total = 0;
    while (values.hasNext()) {
      total += values.next();
    }
    emit(KeyValue.of(key, total));
  }
}