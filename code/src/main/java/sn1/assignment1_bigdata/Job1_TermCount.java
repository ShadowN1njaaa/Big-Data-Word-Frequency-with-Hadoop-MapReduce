package sn1.assignment1_bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;

public class Job1_TermCount {

    /**
     * Mapper:
     *  - Emits ( "term@doc", 1 ) for each token
     *  - Also emits ( "__DOC__@doc", 1 ) for each token to count total terms per doc
     */
    public static class TermCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text termAndDoc = new Text();
        private final Text docKey = new Text();
        private final IntWritable one = new IntWritable(1);
        private String currentDocId;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            currentDocId = split.getPath().getName(); // document id = filename
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            List<String> tokens = Tokenizer.tokenize(value.toString());
            for (String token : tokens) {
                // (term@doc, 1)  -> for f(t,d)
                termAndDoc.set(token + "@" + currentDocId);
                context.write(termAndDoc, one);

                // (__DOC__@doc, 1) -> for doc total tokens
                docKey.set("__DOC__@" + currentDocId);
                context.write(docKey, one);
            }
        }
    }

    /**
     * Reducer:
     *  - For keys "term@doc": outputs  (term@doc, f(t,d))
     *  - For keys "__DOC__@doc": outputs (DOC_TOTAL@doc, Σ f(t',d))
     */
    public static class TermCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable outValue = new IntWritable();
        private final Text outKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            String keyStr = key.toString();

            if (keyStr.startsWith("__DOC__@")) {
                // This is a document-total key
                String docId = keyStr.substring("__DOC__@".length());
                outKey.set("DOC_TOTAL@" + docId);
                outValue.set(sum);
                context.write(outKey, outValue);
            } else {
                // Normal term@doc key
                outValue.set(sum);
                context.write(key, outValue);
            }
        }
    }

    public static Job configureJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "Job1_TermCount");
        job.setJarByClass(Job1_TermCount.class);

        job.setMapperClass(TermCountMapper.class);
        job.setReducerClass(TermCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job;
    }
}
