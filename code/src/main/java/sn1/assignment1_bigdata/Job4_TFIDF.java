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
import java.util.ArrayList;
import java.util.List;

/**
 * Job4_TFIDF:
 *  Inputs:
 *    - step2_tf:   "term@doc \t tf"
 *    - step3_idf:  "term \t idf"
 *
 *  Output:
 *    - "term@doc   tfidf"
 */
public class Job4_TFIDF {

    /**
     * Mapper:
     *  For TF input lines:
     *    "term@doc \t tf"  -> key=term, value="TF\t" + doc + "\t" + tf
     *
     *  For IDF input lines:
     *    "term \t idf"     -> key=term, value="IDF\t" + idf
     *
     *  NOTE: Both inputs use the same TextInputFormat, so we just inspect the line.
     */
    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

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

            String left = parts[0];
            String right = parts[1];

            if (left.contains("@")) {
                // This is from TF: "term@doc"
                int atPos = left.lastIndexOf('@');
                if (atPos <= 0 || atPos == left.length() - 1) {
                    return;
                }
                String term = left.substring(0, atPos);
                String docId = left.substring(atPos + 1);

                outKey.set(term);
                outValue.set("TF\t" + docId + "\t" + right); // right = tf
                context.write(outKey, outValue);
            } else {
                // This is from IDF: "term"
                String term = left;
                outKey.set(term);
                outValue.set("IDF\t" + right); // right = idf
                context.write(outKey, outValue);
            }
        }
    }

    /**
     * Reducer:
     *  For each term:
     *    - read IDF from the "IDF" record
     *    - read all TF records (doc, tf)
     *    - emit (term@doc, tf * idf)
     */
    public static class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double idf = 0.0;
            boolean hasIdf = false;
            List<String[]> tfRecords = new ArrayList<>();

            for (Text v : values) {
                String[] parts = v.toString().split("\\t");
                if (parts.length < 2) {
                    continue;
                }
                String tag = parts[0];

                if ("IDF".equals(tag)) {
                    // IDF\tidf
                    if (parts.length >= 2) {
                        try {
                            idf = Double.parseDouble(parts[1]);
                            hasIdf = true;
                        } catch (NumberFormatException ignored) {
                        }
                    }
                } else if ("TF".equals(tag)) {
                    // TF\tdoc\tf
                    if (parts.length >= 3) {
                        tfRecords.add(new String[]{parts[1], parts[2]}); // {docId, tf}
                    }
                }
            }

            if (!hasIdf) {
                // No IDF -> can't compute tfidf
                return;
            }

            String term = key.toString();
            for (String[] rec : tfRecords) {
                String docId = rec[0];
                double tf;
                try {
                    tf = Double.parseDouble(rec[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                double tfidf = tf * idf;
                outKey.set(term + "@" + docId);
                outValue.set(tfidf);
                context.write(outKey, outValue);
            }
        }
    }

    public static Job configureJob(Configuration conf, Path tfInput, Path idfInput, Path output) throws IOException {
        Job job = Job.getInstance(conf, "Job4_TFIDF");
        job.setJarByClass(Job4_TFIDF.class);

        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Both TF and IDF are text files; add both as inputs
        TextInputFormat.addInputPath(job, tfInput);
        TextInputFormat.addInputPath(job, idfInput);

        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
