package iguazio.home.assignment;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;

import java.net.URI;

import static iguazio.home.assignment.BigBang._logger;

/**
 * A single object per endpoint to communicate with the endpoint in an async manner.
 * Once result callback returns we use {@link JobQueueClearance} to clear the head of the queue from ready jobs
 * (sure not every time the result is for the job at the head of the queue but the check is in
 */
class AsyncEndpoint implements AutoCloseable {
    private final HttpClient        client;
    private final int               index;
    private final Vertx             vertx;
    private final JobQueueClearance queueClearance;

    AsyncEndpoint(URI uri, int index, JobQueueClearance queueClearance) {
        this.index = index;
        this.queueClearance = queueClearance;
        this.vertx = Vertx.vertx();
        HttpClientOptions options = new HttpClientOptions()
                .setDefaultHost(uri.getHost())
                .setDefaultPort(uri.getPort());
        this.client = vertx.createHttpClient(options);
    }

    /**
     * Use VertX to invoke the async call to the endpoint.
     * When callback returns we
     */
    void invokeAsync(final Job job) {
        final HttpClientRequest request = client.post("/", response -> {

            //if ( 200 < response.statusCode() || response.statusCode() >= 300)   ... we agreed to ignore this scenario. benefits of exercise vs. the real world

            response.bodyHandler(buffer -> {

                //each AsyncEndpoint has a reserved place (for its result) in the Job object
                job.setResult(index, buffer.toString());

                if (_logger.isDebugEnabled()) {
                    if (job.isReady()) {
                        _logger.debug("JobReady: id=[{}] line=[{}] status=[{}]", job.getJobId(), job.getLine(), (queueClearance.shouldTriggerClearance(job) ? "JobQueueClearance" : "NothingToDo"));
                    }
                }

                //avoid tons of async that will eventually do nothing since the ready job is not first in the queue
                if (queueClearance.shouldTriggerClearance(job)) {
                    vertx.executeBlocking( queueClearance.getRequestHandler(), queueClearance.getResponseHandler());
                }
            });
        });
        request.putHeader("Content-Length", String.valueOf(job.getLine().length()));
        request.write(job.getLine());
        request.end();
    }

    public void close() {
        _logger.debug("Closing AsyncEndpoint #{}", index);
        this.client.close();
        this.vertx.close();
    }

}
