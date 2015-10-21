package com.griddynamics.test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

public class Test {
    public static void main(String[] args) {
        //int cores = Runtime.getRuntime().availableProcessors();
        //System.out.println(cores);

        System.out.println(Runtime.getRuntime().maxMemory());

        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        System.out.println(memoryBean.getHeapMemoryUsage().getMax());
    }
}
