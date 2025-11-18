package sn1.assignment1_bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Driver implements Tool {

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    private int countDocs(Configuration conf, Path inputPath) throws IOException {
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] statuses = fs.listStatus(inputPath);
        int count = 0;
        if (statuses != null) {
            for (FileStatus st : statuses) {
                if (st.isFile()) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Driver <input_path> <output_base_path> [topN]");
            return -1;
        }

        int topN = 5; // default
        if (args.length >= 3) {
            try {
                topN = Integer.parseInt(args[2]);
            } catch (NumberFormatException ignored) {
            }
        }

        Configuration configuration = (getConf() != null) ? getConf() : new Configuration();

        Path inputPath = new Path(args[0]);
        Path baseOutput = new Path(args[1]);

        Path step1Output = new Path(baseOutput, "step1_termcount");
        Path step2Output = new Path(baseOutput, "step2_tf");
        Path step3Output = new Path(baseOutput, "step3_idf");
        Path step4Output = new Path(baseOutput, "step4_tfidf");
        Path step5Output = new Path(baseOutput, "step5_topn_tfidf");

        // --- Job 1: term counts + doc totals ---
        Job job1 = Job1_TermCount.configureJob(configuration, inputPath, step1Output);
        boolean ok = job1.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job1_TermCount failed");
            return 1;
        }

        // --- Job 2: TF computation ---
        Job job2 = Job2_TF.configureJob(configuration, step1Output, step2Output);
        ok = job2.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job2_TF failed");
            return 1;
        }

        // --- Job 3: IDF computation ---
        int numDocs = countDocs(configuration, inputPath);
        System.out.println("Detected number of documents: " + numDocs);

        Job job3 = Job3_IDF.configureJob(configuration, step1Output, step3Output, numDocs);
        ok = job3.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job3_IDF failed");
            return 1;
        }

        // --- Job 4: TFIDF computation (join TF + IDF) ---
        Job job4 = Job4_TFIDF.configureJob(configuration, step2Output, step3Output, step4Output);
        ok = job4.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job4_TFIDF failed");
            return 1;
        }

        // --- Job 5: Top-N TFIDF per document ---
        Job job5 = Job5_TopNTFIDF.configureJob(configuration, step4Output, step5Output, topN);
        ok = job5.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job5_TopNTFIDF failed");
            return 1;
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
