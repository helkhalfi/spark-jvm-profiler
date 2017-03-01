package com.etsy.statsd.profiler.profilers;

import org.junit.Test;

import static org.junit.Assert.*;

public class MemoryProfilerTest {
    @Test
    public void getRssMemory() throws Exception {
        assertEquals(MemoryProfiler.getRssMemory("VmRSS:    248484 kB"), 248484L * 1024L);

        assertEquals(MemoryProfiler.getRssMemory("VmRSS:\t 2117224 kB"), 2117224L * 1024L);
    }

}