package datawave.microservice.query.executor.util;

import java.util.Set;

import org.apache.log4j.Logger;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryStorageLock;
import datawave.microservice.querymetric.BaseQueryMetric;

public class CacheUpdateUtil {
    private static final Logger log = Logger.getLogger(CacheUpdateUtil.class);
    private final String queryId;
    private final QueryStorageCache cache;
    
    public CacheUpdateUtil(String queryId, QueryStorageCache cache) {
        this.queryId = queryId;
        this.cache = cache;
    }
    
    public QueryStatus getCachedStatus() {
        return cache.getQueryStatus(queryId);
    }
    
    public void startPlanning() {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            log.debug("Starting planning for " + queryId);
            QueryStatus status = cache.getQueryStatus(queryId);
            status.setQueryStartMillis(System.currentTimeMillis());
            status.setCreateStage(QueryStatus.CREATE_STAGE.PLAN);
            cache.updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
    }
    
    public void startQuery(int maxConcurrentNextCalls, String queryString, boolean longRunningQuery, GenericQueryConfiguration checkpoint) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            log.debug("Starting" + (longRunningQuery ? " long running " : " ") + "query execution for " + queryId);
            QueryStatus status = cache.getQueryStatus(queryId);
            status.setMaxConcurrentNextCalls(maxConcurrentNextCalls);
            status.setPlan(queryString);
            status.setAllowLongRunningQueryEmptyPages(longRunningQuery);
            status.setConfig(checkpoint);
            cache.updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
        
    }
    
    public void setStage(QueryStatus.CREATE_STAGE createStage) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = cache.getQueryStatus(queryId);
            log.debug("Setting create stage to " + createStage + " for " + queryId);
            status.setCreateStage(createStage);
            cache.updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
    }
    
    public void setPlan(String plan) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = cache.getQueryStatus(queryId);
            log.debug("Setting plan for " + queryId);
            status.setPlan(plan);
            cache.updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
    }
    
    public void setPredictions(Set<BaseQueryMetric.Prediction> predictions) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = cache.getQueryStatus(queryId);
            log.debug("Setting predictions for " + queryId);
            status.setPredictions(predictions);
            cache.updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
    }
    
    public static class QueryStatusMetrics {
        private int numResultsGenerated = 0;
        private long seekCount = 0;
        private long nextCount = 0;
        
        public int getNumResultsGenerated() {
            return numResultsGenerated;
        }
        
        public void incrementNumResultsGenerated() {
            numResultsGenerated++;
        }
        
        public long getSeekCount() {
            return seekCount;
        }
        
        public void incrementSeekCount(long count) {
            seekCount += count;
        }
        
        public long getNextCount() {
            return nextCount;
        }
        
        public void incrementNextCount(long count) {
            nextCount += count;
        }
        
        public void clear() {
            numResultsGenerated = 0;
            seekCount = nextCount = 0;
        }
    }
    
    /**
     * Increment the query metrics in the query storage
     */
    public void updateCacheMetrics(QueryStatusMetrics queryMetrics) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            // get the cached query status
            QueryStatus cachedStatus = cache.getQueryStatus(queryId);
            // sum up the counts
            cachedStatus.incrementNextCount(queryMetrics.nextCount);
            cachedStatus.incrementSeekCount(queryMetrics.seekCount);
            cachedStatus.incrementNumResultsGenerated(queryMetrics.numResultsGenerated);
            // stored the summed counts
            log.debug("Updating summed results (" + cachedStatus.getNumResultsReturned() + ") for " + queryId);
            cache.updateQueryStatus(cachedStatus);
            // clear the counts in the local instance
            queryMetrics.clear();
        } finally {
            lock.unlock();
        }
    }
    
}
