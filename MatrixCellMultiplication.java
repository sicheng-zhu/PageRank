import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class calculate PageRank scores that each linked page can receive from corresponding start page.
 */
public class MatrixCellMultiplication {
	// This class reads all lines in transition.txt, splits data in each line, and put splitted data into context.
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
		// Process one line of data.
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {			
            String line = value.toString().trim(); // Convert one line of data from transition.txt to String format.
            String[] fromTo = line.split("\t"); // Split data into String array.

            // If this page ID has no links to other pages, add warning message to context.
            if (fromTo.length == 1 || fromTo[1].trim().length() == 0) {
                String info = String.format("Warning: web page %s has no links to other pages. Origin line: %s", fromTo[0], line);
                context.getCounter("Warnings", info);
                return;
            }

            String fromId = fromTo[0]; // Get start page ID.
            String[] toIds = fromTo[1].split(","); // Get linked page ID.
            double prob = ((double) 1) / toIds.length; // Divisor of start page's PageRank score

            // Add start page ID, linked page ID and start page PageRank score's divisor to context so that other classes can receive and do calculation.
            for (String toId : toIds) {
                context.write(new Text(fromId), new Text(toId + '=' + prob));
            }
        }
    }

    // This class reads all lines in prX.txt, splits data in each line, and put splitted data into context.
    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
		// Process one line of data.
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pr = value.toString().trim().split("\t"); // Split data in one line of prX.txt into String array.
            context.write(new Text(pr[0]), new Text(pr[1])); // Add start page ID, and its PageRank score to context so that other classes can receive and do calculation.
        }
    }

    // This class calculate PageRank scores that each linked page can receive from corresponding start page.
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        private float beta;

        // Get damping factor from context, and assign to beta variable.
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

		// Process one key and all associated values.
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
            List<String> transitionUnits = new ArrayList<>(); // Store all linked page ID and this page's PageRank score's divisor of this key.
            double prUnit = 0;

            // Iterate all values.
            for (Text value : values) {
                String val = value.toString();
				
                if (val.contains("=")) { // If value contains "=", put this value into transitionUnits array.
                    transitionUnits.add(val);
                } else { // If value doesn't contain "=", then this is this key's PageRank score as of last iteration, so assign to prUnit.
                    prUnit = Double.parseDouble(val);
                }
            }

            // Split each element in array into linked page ID in String format and start page PageRank score's divisor in double format.
            for (String unit : transitionUnits) {
                String outputKey = unit.split("=")[0];
                double relation = Double.parseDouble(unit.split("=")[1]);

                /**
				 * Each linked page receives PageRank score from start page, and the formual to 
				 * calculate score from start page is 
				 * start page PageRank score's divisor * start page's PageRank score as of last iteration * (1 - beta).
				 */
                String outputVal = String.valueOf(relation * prUnit * (1 - beta));
                context.write(new Text(outputKey), new Text(outputVal)); // Add linked page ID and its PageRank score into context.
            }
        }
    }
 
    public static void main(String[] args) throws Exception {
        Path transitionMatrixPath = new Path(args[0]); // Create path to retrieve transition file.
        Path pageRankMatrixPath = new Path(args[1]);   // Create path to retrieve file for each page's PageRank score as of last iteration.
        Path outputPath = new Path(args[2]);           // Create path to place output file.
        float beta = 0.2f;  // Create a default damping factor if user misses this.

        if (args.length > 3) {
            beta = Float.parseFloat(args[3]); // Assign to beta if user has defined damping factor.
        }

        Configuration conf = new Configuration();
        conf.setFloat("beta", beta); // Put damping factor into configuration, so that mapper and reducer class can use.

        Job job = Job.getInstance(conf);
        job.setJarByClass(MatrixCellMultiplication.class); // Set up JAR file.

        // Set up input and output format for two map classes, and pass external configuration to two classes.
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class); // Set up reduce class.
        job.setOutputKeyClass(Text.class); // Set up output key data type.
        job.setOutputValueClass(Text.class); // Set up output value data type.

        // Assign input file paths for mapper classes, and output file paths for reducer class.
        MultipleInputs.addInputPath(job, transitionMatrixPath, TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, pageRankMatrixPath, TextInputFormat.class, PRMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
		
        job.waitForCompletion(true);
    }
}