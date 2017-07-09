
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.PriorityQueue;


public class RecommanderListGenerator {
    public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //user \t movie:rating
            String[] userAndMovieAndRating = value.toString().trim().split("\t");
            int user = Integer.parseInt(userAndMovieAndRating[0]);
            String[] movieAndRating = userAndMovieAndRating[1].split("0");
            context.write(new IntWritable(user), new Text(movieAndRating[0] + ":" + movieAndRating[1]));
        }
    }

    public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Top K: recommend top k movies with highest calculated ratings for each user
            int k = 5;
            PriorityQueue<Movie> heap = new PriorityQueue<Movie>();
            //movie :rating
            for (Text value : values) {
                String[] movieAndRating = value.toString().trim().split(":");
                int movieId = Integer.parseInt(movieAndRating[0]);
                double rating = Double.parseDouble(movieAndRating[1]);
                if (heap.size() < k) {
                    heap.offer(new Movie(movieId, rating));
                } else {
                    if (heap.peek().getRating() < rating) {
                        heap.poll();
                        heap.offer(new Movie(movieId, rating));
                    }
                }
            }
            while (!heap.isEmpty()) {
                Movie movie = heap.poll();
                context.write(key, new Text(movie.getMovieId() + ":" + movie.getRating()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(RecommenderListGeneratorMapper.class);
        job.setReducerClass(RecommenderListGeneratorReducer.class);

        job.setJarByClass(RecommanderListGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
