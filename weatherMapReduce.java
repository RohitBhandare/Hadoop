import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {
    
    public static class WeatherMapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {
        private Text date = new Text();
        private WeatherWritable weather = new WeatherWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 8) {
                date.set(tokens[0]);
                weather.set(Double.parseDouble(tokens[1]), Double.parseDouble(tokens[2]), Double.parseDouble(tokens[3]));
                context.write(date, weather);
            }
        }
    }

    public static class WeatherReducer extends Reducer<Text, WeatherWritable, Text, Text> {
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<WeatherWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double tempSum = 0.0;
            double dewSum = 0.0;
            double windSum = 0.0;
            for (WeatherWritable value : values) {
                count++;
                tempSum += value.getTemperature();
                dewSum += value.getDewPoint();
                windSum += value.getWindSpeed();
            }
            double tempAvg = tempSum / count;
            double dewAvg = dewSum / count;
            double windAvg = windSum / count;
            outputValue.set("Average Temperature: " + tempAvg + ", Average Dew Point: " + dewAvg + ", Average Wind Speed: " + windAvg);
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WeatherAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(WeatherAnalysis.class);
        job.setJobName("Weather Analysis");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WeatherWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private static final int TEMPERATURE_COLUMN = 1;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > TEMPERATURE_COLUMN) {
                String date = fields[0];
                double temperature = Double.parseDouble(fields[TEMPERATURE_COLUMN]);
                context.write(new Text(date), new DoubleWritable(temperature));
            }
        }
    }

    public static class MaxTemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double maxTemperature = Double.NEGATIVE_INFINITY;
            for (DoubleWritable value : values) {
                double temperature = value.get();
                if (temperature > maxTemperature) {
                    maxTemperature = temperature;
                }
            }
            context.write(key, new DoubleWritable(maxTemperature));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaxTemperature");
        job.setJarByClass(MaxTemperature.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
