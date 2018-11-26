package com.kaustav;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.AffinityUuid;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.cache.CacheMode;
import java.util.Map;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 * Apache Flink Ignite sink implemented as a RichSinkFunction.
 */
public class IgniteSink<IN> extends RichSinkFunction<IN> {
    /** Default flush frequency. */
    private static final long DFLT_FLUSH_FREQ = 10000L;

    /** Logger. */
    private transient IgniteLogger log;

    /** Automatic flush frequency. */
    private long autoFlushFrequency = DFLT_FLUSH_FREQ;

    /** Enables overwriting existing values in cache. */
    private boolean allowOverwrite = false;

    /** Flag for stopped state. */
    private volatile boolean stopped = true;

    /** Ignite instance. */
    protected transient Ignite ignite;

    /** Ignite Data streamer instance. */
    protected transient IgniteDataStreamer<Long, String> streamer;

    /** Ignite grid configuration file. */
    protected final String igniteCfgFile;

    /** Cache name. */
    protected final String cacheName;

    protected static IgniteCache <?, ?> mlcache;



    /**
     * Gets the cache name.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Gets Ignite configuration file.
     *
     * @return Configuration file.
     */
    public String getIgniteConfigFile() {
        return igniteCfgFile;
    }

    /**
     * Gets the Ignite instance.
     *
     * @return Ignite instance.
     */
    public Ignite getIgnite() {
        return ignite;
    }

    /**
     * Obtains data flush frequency.
     *
     * @return Flush frequency.
     */
    public long getAutoFlushFrequency() {
        return autoFlushFrequency;
    }

    /**
     * Specifies data flush frequency into the grid.
     *
     * @param autoFlushFrequency Flush frequency.
     */
    public void setAutoFlushFrequency(long autoFlushFrequency) {
        this.autoFlushFrequency = autoFlushFrequency;
    }

    /**
     * Obtains flag for enabling overwriting existing values in cache.
     *
     * @return True if overwriting is allowed, false otherwise.
     */
    public boolean getAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * Enables overwriting existing values in cache.
     *
     * @param allowOverwrite Flag value.
     */
    public void setAllowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;
    }

    /**
     * Default IgniteSink constructor.
     *
     * @param cacheName Cache name.
     */
    public IgniteSink(String cacheName, String igniteCfgFile) {
        this.cacheName = cacheName;
        this.igniteCfgFile = igniteCfgFile;

    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    @Override
    public void open(Configuration parameter) {
                try {
            // if an ignite instance is already started in same JVM then use it.
            this.ignite = Ignition.ignite();
        } catch (IgniteIllegalStateException e) {
            Ignition.setClientMode(true);
            this.ignite = Ignition.start(this.igniteCfgFile);
        }

        CacheConfiguration<Long, String> cfg = new CacheConfiguration<>(cacheName);
        cfg.setCacheMode(CacheMode.PARTITIONED);




        cfg.setIndexedTypes(Long.class, String.class);

        this.ignite.getOrCreateCache(cfg);



        this.log = this.ignite.log();

        this.streamer = this.ignite.dataStreamer(this.cacheName);
        this.streamer.autoFlushFrequency(autoFlushFrequency);
        this.streamer.allowOverwrite(allowOverwrite);

        stopped = false;
    }

    /**
     * Stops streamer.
     *
     * @throws IgniteException If failed.
     */
    @Override
    public void close() {
        if (stopped)
            return;

        stopped = true;

        this.streamer.close();
    }

    /**
     * Transfers data into grid. It is called when new data
     * arrives to the sink, and forwards it to {@link IgniteDataStreamer}.
     *
     * @param in IN.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void invoke(IN in) {
        try {
            if (!(in instanceof Map))
                throw new IgniteException("Map as a streamer input is expected!");



            this.streamer.addData((Map)in);

            System.out.println("loading");

        }
        catch (Exception e) {
            log.error("Error while processing IN of " + this.cacheName, e);
        }
    }
}
