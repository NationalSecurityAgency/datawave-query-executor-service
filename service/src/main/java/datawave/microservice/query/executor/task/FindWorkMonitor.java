package datawave.microservice.query.executor.task;

import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bus.event.PathDestinationFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.storage.QueryStorageCache;

@Component
@ConditionalOnProperty(name = "datawave.query.executor.monitor.enabled", havingValue = "true", matchIfMissing = true)
public class FindWorkMonitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final ExecutorProperties executorProperties;
    private final QueryStorageCache queryStorageCache;
    private final QueryExecutor queryExecutor;
    private final ExecutorStatusLogger executorStatusLogger = new ExecutorStatusLogger();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    private final String originService;
    private final String destinationService;
    
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    public FindWorkMonitor(ExecutorProperties executorProperties, QueryProperties queryProperties, QueryStorageCache cache, QueryExecutor executor) {
        this.executorProperties = executorProperties;
        this.queryStorageCache = cache;
        this.queryExecutor = executor;
        
        PathDestinationFactory pathDestinationFactory = new PathDestinationFactory();
        this.originService = pathDestinationFactory.getDestination(queryProperties.getQueryServiceName()).getDestinationAsString();
        this.destinationService = pathDestinationFactory
                        .getDestination(String.join("-", Arrays.asList(queryProperties.getExecutorServiceName(), executorProperties.getPool())))
                        .getDestinationAsString();
    }
    
    // this runs in a separate thread every 30 seconds (by default)
    @Scheduled(cron = "${datawave.query.executor.monitor.scheduler-crontab:*/30 * * * * ?}")
    public void monitorTaskScheduler() {
        // perform some upkeep
        if (taskFuture != null) {
            if (taskFuture.isDone()) {
                try {
                    taskFuture.get();
                } catch (InterruptedException e) {
                    log.warn("Query Monitor task was interrupted");
                } catch (CancellationException e) {
                    log.warn("Query Monitor task was cancelled");
                } catch (Exception e) {
                    log.error("Query Monitor task failed", e);
                }
                taskFuture = null;
            } else if (isTaskLeaseExpired()) {
                // if the lease has expired for the future, cancel it and wait for next scheduled task
                log.warn("Query Monitor task being cancelled");
                taskFuture.cancel(true);
            }
        }
        
        // schedule a new monitor task if the previous one has finished
        if (taskFuture == null) {
            taskStartTime = System.currentTimeMillis();
            taskFuture = executor.submit(new FindWorkTask(queryStorageCache, queryExecutor, originService, destinationService, executorStatusLogger));
        }
    }
    
    private boolean isTaskLeaseExpired() {
        return (System.currentTimeMillis() - taskStartTime) > executorProperties.getMonitorTaskLeaseMillis();
    }
}
