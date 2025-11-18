package sn1.assignment1_bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Job3_IDF:
 *  Input: Job1_TermCount output
 *    - "term@doc        f(t,d)"
 *    - "DOC_TOTAL@doc   sum_f(t',d)"   (ignored here)
 *
 *  Output:
 *    - "term    idf(t)"  where idf(t) = log(N / df(t))
 *      N = total number of documents (passed in configuration)
 *      df(t) = number of documents containing t
 */
public class Job3_IDF {

    public static final String CONF_NUM_DOCS = "assignment1.numDocs";

    /**
     * Mapper:
     *  - Ignore "DOC_TOTAL@doc" lines
     *  - For "term@doc" lines, emit (term, 1)
     *    Since Job1 has only one record per (term,doc), this directly counts df(t)
     */
    public static class IDFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outKey = new Text();
        private final IntWritable one = new IntWritable(1);

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

            String compositeKey = parts[0]; // e.g. "word@input1.txt" or "DOC_TOTAL@input1.txt"

            if (compositeKey.startsWith("DOC_TOTAL@")) {
                // This is the per-document total; IDF doesn't need it
                return;
            }

            int atPos = compositeKey.lastIndexOf('@');
            if (atPos <= 0) {
                return;
            }

            String term = compositeKey.substring(0, atPos);
            // String docId = compositeKey.substring(atPos + 1); // not needed here

            outKey.set(term);
            context.write(outKey, one);
        }
    }

    /**
     * Reducer:
     *  For each term:
     *    - df(t) = sum of values
     *    - idf(t) = log( N / df(t) ), N from configuration
     */
    public static class IDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private final DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int df = 0;
            for (IntWritable v : values) {
                df += v.get();
            }
            if (df <= 0) {
                return;
            }

            Configuration conf = context.getConfiguration();
            int numDocs = conf.getInt(CONF_NUM_DOCS, 1);
            if (numDocs <= 0) {
                numDocs = 1;
            }

            double idf = Math.log((double) numDocs / (double) df);
            outValue.set(idf);
            context.write(key, outValue);
        }
    }

    public static Job configureJob(Configuration conf, Path input, Path output, int numDocs) throws IOException {
        conf.setInt(CONF_NUM_DOCS, numDocs);

        Job job = Job.getInstance(conf, "Job3_IDF");
        job.setJarByClass(Job3_IDF.class);

        job.setMapperClass(IDFMapper.class);
        job.setReducerClass(IDFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
