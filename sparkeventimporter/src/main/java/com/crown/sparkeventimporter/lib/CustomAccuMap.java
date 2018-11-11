package com.crown.sparkeventimporter.lib;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CustomAccuMap extends AccumulatorV2<Map<String, Double>, Map<String, Double>> {

    private Map<String, Double> myMap = new HashMap<String, Double>();

    public CustomAccuMap() {
        this(new HashMap<String, Double>());
    }

    private CustomAccuMap(Map<String,Double> initialValue) {
        if (initialValue != null) {
            myMap = initialValue ;
        }
    }

    public boolean isZero() {
        return (myMap.size() == 0);
    }

    public AccumulatorV2<Map<String, Double>, Map<String, Double>> copy() {
        return (new CustomAccuMap(value()));
    }

    public void reset() {
        myMap.clear();
    }

    public void add(Map<String, Double> v) {
        Map<String, Double> newMap = new HashMap<String, Double>(v);
        for (Map.Entry<String, Double> entry: newMap.entrySet()) {
            if (myMap.containsKey(entry.getKey())) {
                myMap.put(entry.getKey(), myMap.get(entry.getKey()) + entry.getValue());
            } else {
                myMap.put(entry.getKey(), entry.getValue());
            }
        }
//        Iterator<String> dIterator =
//                v.keySet().iterator();
//        while(dIterator.hasNext()) {
//            String key = dIterator.next();
//            if ( myMap.containsKey(key)) {
//                myMap.put(key, myMap.get(key) +
//                        v.get(key));
//            }
//            else {
//                myMap.put(key, v.get(key));
//            }
//        }
    }

    public void merge(AccumulatorV2<Map<String, Double>, Map<String, Double>> other) {
        add(other.value());
    }

    public Map<String, Double> value() {
        return myMap;
    }
}
