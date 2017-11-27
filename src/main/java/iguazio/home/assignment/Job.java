package iguazio.home.assignment;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We have a Job object per line in the input file.
 * It "sits" in the queue until it is ready to be written to file and all jobs before it are ready as well.
 */
class Job {
    private static final int RESULTS_SIZE = 2;

    private final String        line;
    private final AtomicInteger count   = new AtomicInteger();
    private final String[]      results = new String[RESULTS_SIZE];

    Job(String line) {
        this.line = line;
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

    boolean calculateAnswer() {
        return Objects.equals(results[0], results[1]);
    }

}
