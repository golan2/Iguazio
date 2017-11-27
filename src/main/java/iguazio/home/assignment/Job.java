package iguazio.home.assignment;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We have a Job object per line in the input file.
 * It "sits" in the queue until it is ready to be written to file and all jobs before it are ready as well.
 */
class Job {
    private static final int RESULTS_SIZE = 2;
    private static long jobOrdinalIndexer = 0;

    private final long          jobId;      //to show progress in the command line // can be removed if not interesting
    private final String        line;
    private final AtomicInteger count;      //how many results we have so far
    private final String[]      results;

    Job(String line) {
        this.jobId   = ++jobOrdinalIndexer;
        this.line    = line;
        this.count   = new AtomicInteger();
        this.results = new String[RESULTS_SIZE];
    }

    void setResult(int index, String result) {
        this.results[index] = result;
        count.incrementAndGet();
    }

    boolean isReady() {
        return count.get()==RESULTS_SIZE;
    }

    String getLine() {
        return line;
    }

    long getJobId() {
        return jobId;
    }

    boolean calculateAnswer() {
        return Objects.equals(results[0], results[1]);
    }

}
