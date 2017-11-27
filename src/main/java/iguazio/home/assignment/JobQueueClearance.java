package iguazio.home.assignment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static iguazio.home.assignment.BigBang._logger;

/**
 * Call it we need to clear the queue.
 * We will {@link Queue#poll()} from the queue all "Ready" jobs (until we hit first non ready) and write them to file!
 *
 * We use it as the callback for {@link io.vertx.core.Vertx#executeBlocking(Handler, boolean, Handler)}
 * This is why we have {@link #getRequestHandler()} and {@link #getResponseHandler()}
 *
 * Actual "clearance" logic is in {@link RequestHandler#handle(Future)}
 */
class JobQueueClearance implements AutoCloseable{
    private final BlockingQueue<Job> queue;
    private final BufferedWriter     bufferedWriter;
    private final RequestHandler     requestHandler;
    private final ResponseHandler    responseHandler;

    JobQueueClearance(BlockingQueue<Job> queue, String fileName) throws FileNotFoundException {
        this.queue           = queue;
        this.requestHandler  = new RequestHandler();
        this.responseHandler = new ResponseHandler();
        this.bufferedWriter  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(fileName))));
    }

    RequestHandler getRequestHandler() {
        return requestHandler;
    }

    ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public void close() throws IOException {
        _logger.debug("Closing JobQueueClearance");
        this.bufferedWriter.close();
    }

    /**
     * If the job is ready and is the first in the queue then we can print it to the output.   i.e. trigger Clearance
     * If it is the first but not ready then next {@link AsyncEndpoint} will make it ready
     * If it is ready but not the first it will have to wait for all the jobs before it to become ready.
     * In this case we will handle it in the callback of one of these other jobs.
     */
    boolean shouldTriggerClearance(Job job) {
        return job.isReady() && queue.peek().equals( job );
    }

    class RequestHandler implements Handler<Future<List<Job>>> {

        /**
         * Why "synchronized"?
         * We do not want 2 threads to do it in parallel because the peek and the poll must refer to the same Job.
         * It doesn't really slows us down to synchronize because:
         *  [1] One thread will clear all ready jobs and the other (that awaited the lock) will do nothing.
         *  [2] It is done on a background thread and not on the event loop thread.
         */
        @Override
        public synchronized void handle(Future<List<Job>> future) {

            if (nothingToDo()) return ;        //a few less LinkedList object creation :-)

            List<Job> jobs = new LinkedList<>();
            while (queue.peek()!=null && queue.peek().isReady()) {
                try {
                    final Job job = queue.poll();
                    bufferedWriter.write(job.calculateAnswer() + "\n");
                    jobs.add(job);
                } catch (Exception e) {
                    future.fail(e);
                }
            }
            future.complete(jobs);
            _logger.debug("JobQueueClearance - END - jobs written to disk=["+jobs.size()+"]");
        }
    }

    boolean nothingToDo() {
        return queue.isEmpty() || !queue.peek().isReady();
    }

    class ResponseHandler implements Handler<AsyncResult<List<Job>>> {

        @Override
        public void handle(AsyncResult<List<Job>> result) {
            if (result.failed()) {
                _logger.error("Failure during JobQueueClearance. Error: {}", result.cause().getMessage());
                _logger.debug("Details", result.cause());

            }
            else {
                if (!result.result().isEmpty()) _logger.debug("Writing: " + result.result().stream().map(j -> String.valueOf(j.getJobId())).collect(Collectors.joining(",")));
            }
        }
    }


}