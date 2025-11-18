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
 * Job2_TF:
 *  Input: output of Job1_TermCount
 *    - "term@doc \t f(t,d)"
 *    - "DOC_TOTAL@doc \t sum_f(t',d)"
 *
 *  Output:
 *    - "term@doc \t tf(t,d)"
 */
public class Job2_TF {

    /**
     * Mapper:
     *  Reads each line from Job1 output and emits:
     *
     *    key = docId
     *    value = "T\tterm\tf"   for term@doc lines
     *    value = "D\tTOTAL\tF"  for DOC_TOTAL@doc lines
     */
    public static class TFMapper extends Mapper<LongWritable, Text, Text, Text> {

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
                return; // Unexpected format
            }

            String compositeKey = parts[0]; // e.g. "word@input1.txt" or "DOC_TOTAL@input1.txt"
            String countStr = parts[1];

            if (compositeKey.startsWith("DOC_TOTAL@")) {
                String docId = compositeKey.substring("DOC_TOTAL@".length());
                outKey.set(docId);
                outValue.set("D\tTOTAL\t" + countStr);
                context.write(outKey, outValue);
            } else {
                int atPos = compositeKey.lastIndexOf('@');
                if (atPos <= 0 || atPos == compositeKey.length() - 1) {
                    return; // malformed; skip
                }
                String term = compositeKey.substring(0, atPos);
                String docId = compositeKey.substring(atPos + 1);

                outKey.set(docId);
                outValue.set("T\t" + term + "\t" + countStr);
                context.write(outKey, outValue);
            }
        }
    }

    /**
     * Reducer:
     *  For each docId:
     *    - Reads its total term count from the "D" record
     *    - Reads all "T" records (term, f)
     *    - Emits ( "term@doc", tf )
     */
    public static class TFReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int docTotal = 0;
            List<String[]> termCounts = new ArrayList<>();

            for (Text v : values) {
                String[] parts = v.toString().split("\\t");
                if (parts.length < 3) {
                    continue;
                }
                String recordType = parts[0];

                if ("D".equals(recordType)) {
                    // D\tTOTAL\tF
                    try {
                        docTotal = Integer.parseInt(parts[2]);
                    } catch (NumberFormatException ignored) {
                    }
                } else if ("T".equals(recordType)) {
                    // T\tterm\tf
                    termCounts.add(new String[]{parts[1], parts[2]}); // {term, f}
                }
            }

            if (docTotal <= 0) {
                // Can't compute TF without a valid doc total
                return;
            }

            String docId = key.toString();
            for (String[] tc : termCounts) {
                String term = tc[0];
                int freq;
                try {
                    freq = Integer.parseInt(tc[1]);
                } catch (NumberFormatException e) {
                    continue;
                }
                double tf = (double) freq / (double) docTotal;
                outKey.set(term + "@" + docId);
                outValue.set(tf);
                context.write(outKey, outValue);
            }
        }
    }

    public static Job configureJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "Job2_TF");
        job.setJarByClass(Job2_TF.class);

        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
