package com.radiumone.dw.etl3.poc.kafkastreams;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.utils.Utils;

/**
 * Created by broy on 1/13/17.
 */
public class Test {

    public static void main(String[] args) {

        AppIdPartitioner appIdPartitioner = new AppIdPartitioner();

        Partitioner ni = null;

        try {

            ni = Utils.newInstance("com.radiumone.dw.etl3.poc.kafkastreams.AppIdPartitioner", Partitioner.class);



        } catch (Throwable t) {
            t.printStackTrace();
        }

        System.out.println("appIdPartitioner=" + appIdPartitioner);
        System.out.println("ni=" + ni);

    }

}
