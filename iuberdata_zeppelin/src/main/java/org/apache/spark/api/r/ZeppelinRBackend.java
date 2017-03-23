package org.apache.spark.api.r;

import eleflow.uberdata.core.IUberdataContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by paulomagalhaes on 7/7/15.
 */
//public class ZeppelinRBackend extends RBackend {
//    static public JavaSparkContext createSparkContext(){
//        return new JavaSparkContext(IUberdataContext.getUC().sparkContext());
//    }
//
//    static public HiveContext createSqlContext(JavaSparkContext jsc){
//        return IUberdataContext.getUC().sqlContext();
//    }
//
//    public String put(Object obj){
//        return JVMObjectTracker.put(obj);
//    }
//}
