package iguazio.home.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Everyone Has to Start Somewhere
 *
 * To activate DEBUG logging add "-Ddebug" to the commandline
 */
public class BigBang {
    static final Logger _logger = initLogger();
    private static final int MAX_PARALLELISM = 128;

    public static void main(String[] args) throws Exception {

        //Command Line Arguments (skipping input validation :~)
        final String inputFile  = args[0];
        final String outputFile = args[1];
        final URI    endpoint1  = URI.create(args[2]);
        final URI    endpoint2  = URI.create(args[3]);

        final BlockingQueue<Job> queue = new ArrayBlockingQueue<>(MAX_PARALLELISM); ////Blocking property is used for throttling 128 endpoint max

        try (
                final JobQueueClearance  jobQueueConsumer = new JobQueueClearance(queue, outputFile);
                final AsyncEndpoint      ep1              = new AsyncEndpoint(endpoint1, 0, jobQueueConsumer);
                final AsyncEndpoint      ep2              = new AsyncEndpoint(endpoint2, 1, jobQueueConsumer)
        ) {

            logConfig(inputFile, outputFile, endpoint1, endpoint2);

            digestInputFile(inputFile, queue, ep1, ep2);

            waitForAsyncCallbacks(queue);
        }

        _logger.info("Done!");
    }

    private static Logger initLogger() {
        if (System.getProperty("debug")!=null) {
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug"                 );
            System.setProperty("org.slf4j.simpleLogger.showDateTime", "true"                     );
            System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss,SSS");
            System.setProperty("org.slf4j.simpleLogger.showShortLogName", "true"                 );
            System.setProperty("vertx.disableDnsResolver", "true"                                ); //avoid annoying error in DEBUG mode on Windows   // IllegalArgumentException: \etc\resolv.conf    // https://github.com/eclipse/vert.x/issues/2204
        }
        return LoggerFactory.getLogger(BigBang.class);
    }

    private static void logConfig(String inputFile, String outputFile, URI endpoint1, URI endpoint2) {
        _logger.info("Configuration: \n" +
                        "\tInput file:    {}\n" +
                        "\tOutput file:   {}\n" +
                        "\tEndpoint #1:   {}\n" +
                        "\tEndpoint #2:   {}",
                inputFile,
                outputFile,
                endpoint1,
                endpoint2

        );
    }

    /**
     * Use a single thread to consume lines from log while maintaining original order
     * For each line:
     *      [1] Add a Job to the queue
     *      [2] Invoke 2 async calls to the 2 given endpoints
     */
    private static void digestInputFile(String inputFile, final BlockingQueue<Job> queue, final AsyncEndpoint ep1, final AsyncEndpoint ep2) {
        try (Stream<String> stream = Files.lines(Paths.get(inputFile))) {
            stream.forEachOrdered(line -> {
                try {
                    _logger.debug("Queue size=" + queue.size());
                    final Job job = new Job(line);
                    if (job.getJobId()%100==0) {
                        _logger.info("Lines processed so far {} ...", job.getJobId());
                    }
                    queue.put(job);     //here we will block if we reach parallel limit
                    ep1.invokeAsync(job);
                    ep2.invokeAsync(job);
                } catch (InterruptedException ignored) { }
            });
        } catch (IOException e) {
            _logger.error("Failed to process input file. Error: {}", e.getMessage());
            _logger.debug("Details", e);
        }
    }

    /**
     * In case the main thread has finished processing the file but the queue still has non-ready Jobs.
     * So we wait a little until the queue is empty.
     */
    private static void waitForAsyncCallbacks(BlockingQueue<Job> queue) throws InterruptedException {
        while (!queue.isEmpty()) {
            _logger.debug("WAITING . . .");
            Thread.sleep(1000);
            _logger.debug("Remaining Jobs ["+queue.size()+"] " + queue.stream().map(Job::getLine).collect(Collectors.joining(" , ")));
        }
    }

}
