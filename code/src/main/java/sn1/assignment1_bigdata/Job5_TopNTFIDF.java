package sn1.assignment1_bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Comparator;

public class Job5_TopNTFIDF {

    public static final String CONF_TOP_N = "assignment1.topN";

    /**
     * Input lines from step4_tfidf:
     *   "term@doc\t tfidf"
     *
     * Mapper emits:
     *   key   = docId
     *   value = "term\t tfidf"
     */
    public static class TopNMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\\t");
            if (parts.length != 2) {
                return;
            }

            String compositeKey = parts[0]; // term@doc
            String tfidfStr = parts[1];

            int atPos = compositeKey.lastIndexOf('@');
            if (atPos <= 0 || atPos == compositeKey.length() - 1) {
                return;
            }

            String term = compositeKey.substring(0, atPos);
            String docId = compositeKey.substring(atPos + 1);

            outKey.set(docId);
            outValue.set(term + "\t" + tfidfStr);
            context.write(outKey, outValue);
        }
    }

    /**
     * Reducer:
     *  For each docId:
     *    - collect all (term, tfidf)
     *    - keep only top N by tfidf using a min-heap
     *    - emit one line per top term:
     *        key   = docId
     *        value = term\t tfidf
     */
    public static class TopNReducer extends Reducer<Text, Text, Text, Text> {

        private int topN;

        private static class TermScore {
            String term;
            double score;

            TermScore(String term, double score) {
                this.term = term;
                this.score = score;
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topN = conf.getInt(CONF_TOP_N, 5);
            if (topN <= 0) {
                topN = 5;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Min-heap: smallest score at the top, so we can drop it when size > N
            PriorityQueue<TermScore> heap = new PriorityQueue<>(
                    Comparator.comparingDouble(ts -> ts.score)
            );

            for (Text v : values) {
                String[] parts = v.toString().split("\\t");
                if (parts.length != 2) {
                    continue;
                }
                String term = parts[0];
                double score;
                try {
                    score = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                heap.offer(new TermScore(term, score));
                if (heap.size() > topN) {
                    heap.poll(); // remove smallest
                }
            }

            // Now heap contains at most topN highest scores, but in ascending order.
            // Put them into an array to output in descending order.
            TermScore[] top = heap.toArray(new TermScore[0]);

            // Simple bubble sort / insertion sort is fine, but we can just do a basic sort:
            java.util.Arrays.sort(top, (a, b) -> Double.compare(b.score, a.score));

            for (TermScore ts : top) {
                // key: docId, value: "term\tscore"
                context.write(key, new Text(ts.term + "\t" + ts.score));
            }
        }
    }

    public static Job configureJob(Configuration conf, Path input, Path output, int topN) throws IOException {
        conf.setInt(CONF_TOP_N, topN);

        Job job = Job.getInstance(conf, "Job5_TopNTFIDF");
        job.setJarByClass(Job5_TopNTFIDF.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
