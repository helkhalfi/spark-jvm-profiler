package com.etsy.statsd.profiler.profilers;

import com.etsy.statsd.profiler.Arguments;
import com.etsy.statsd.profiler.Profiler;
import com.etsy.statsd.profiler.reporter.Reporter;
import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Profiles memory usage and GC statistics
 *
 * @author Andrew Johnson
 */
public class MemoryProfiler extends Profiler {
    private static final Logger LOGGER = Logger.getLogger(MemoryProfiler.class.getName());

    private static final long PERIOD = 10;

    private final MemoryMXBean memoryMXBean;
    private final List<GarbageCollectorMXBean> gcMXBeans;
    private final HashMap<GarbageCollectorMXBean, AtomicLong> gcTimes = new HashMap<>();
    private final ClassLoadingMXBean classLoadingMXBean;
    private final List<MemoryPoolMXBean> memoryPoolMXBeans;
    private final Integer pid;

    public MemoryProfiler(Reporter reporter, Arguments arguments) {
        super(reporter, arguments);
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();

        for (GarbageCollectorMXBean b : gcMXBeans) {
            gcTimes.put(b, new AtomicLong());
        }

        String processName = ManagementFactory.getRuntimeMXBean().getName();
        pid = Integer.parseInt(processName.split("@")[0]);
        LOGGER.info("Process id: " + pid);
    }

    /**
     * Profile memory usage and GC statistics
     */
    @Override
    public void profile() {
        recordStats();
    }

    @Override
    public void flushData() {
        recordStats();
    }

    @Override
    public long getPeriod() {
        return PERIOD;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.SECONDS;
    }

    @Override
    protected void handleArguments(Arguments arguments) { /* No arguments needed */ }

    /**
     * Records all memory statistics
     */
    private void recordStats() {
        long finalizationPendingCount = memoryMXBean.getObjectPendingFinalizationCount();
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();
        Map<String, Long> metrics = Maps.newHashMap();

        metrics.put("pending-finalization-count", finalizationPendingCount);
        recordMemoryUsage("heap.total", heap, metrics);
        recordMemoryUsage("nonheap.total", nonHeap, metrics);

        for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
            String gcName = gcMXBean.getName().replace(" ", "_");
            metrics.put("gc." + gcName + ".count", gcMXBean.getCollectionCount());

            final long time = gcMXBean.getCollectionTime();
            final long prevTime = gcTimes.get(gcMXBean).get();
            final long runtime = time - prevTime;

            metrics.put("gc." + gcName + ".time", time);
            metrics.put("gc." + gcName + ".runtime", runtime);

            if (runtime > 0) {
                gcTimes.get(gcMXBean).set(time);
            }
        }

        long loadedClassCount = classLoadingMXBean.getLoadedClassCount();
        long totalLoadedClassCount = classLoadingMXBean.getTotalLoadedClassCount();
        long unloadedClassCount = classLoadingMXBean.getUnloadedClassCount();

        metrics.put("loaded-class-count", loadedClassCount);
        metrics.put("total-loaded-class-count", totalLoadedClassCount);
        metrics.put("unloaded-class-count", unloadedClassCount);

        for (MemoryPoolMXBean memoryPoolMXBean: memoryPoolMXBeans) {
            String type = poolTypeToMetricName(memoryPoolMXBean.getType());
            String name = poolNameToMetricName(memoryPoolMXBean.getName());
            String prefix = type + '.' + name;
            MemoryUsage usage = memoryPoolMXBean.getUsage();

            recordMemoryUsage(prefix, usage, metrics);
        }

        try {
            Process process = Runtime.getRuntime().exec("cat /proc/" + pid + "/status");
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("VmRSS:")) {
                    metrics.put("process.rss", getRssMemory(line));
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.severe(e.getMessage());
        }
        
        recordGaugeValues(metrics);
    }

    static long getRssMemory(String line) {
        return 1024 * Long.parseLong(
            line.replace("VmRSS:", "").replace(" kB", "").replace(" ", "").replace("\t", "")
        );
    }

    /**
     * Records memory usage
     *
     * @param prefix The prefix to use for this object
     * @param memory The MemoryUsage object containing the memory usage info
     */
    private static void recordMemoryUsage(String prefix, MemoryUsage memory, Map<String, Long> metrics) {
        metrics.put(prefix + ".init", memory.getInit());
        metrics.put(prefix + ".used", memory.getUsed());
        metrics.put(prefix + ".committed", memory.getCommitted());
        metrics.put(prefix + ".max", memory.getMax());
    }

    /**
     * Formats a MemoryType into a valid metric name
     *
     * @param memoryType a MemoryType
     * @return a valid metric name
     */
    private static String poolTypeToMetricName(MemoryType memoryType) {
        switch (memoryType) {
            case HEAP:
                return "heap";
            case NON_HEAP:
                return "nonheap";
            default:
                return "unknown";
        }
    }

    /**
     * Formats a pool name into a valid metric name
     *
     * @param poolName a pool name
     * @return a valid metric name
     */
    private static String poolNameToMetricName(String poolName) {
        return poolName.toLowerCase().replaceAll("\\s+", "-");
    }
}
