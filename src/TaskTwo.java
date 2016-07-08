/**
 * Created by darnell on 7/6/16.
 */


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.htrace.fasterxml.jackson.annotation.JsonFormat;
import org.jruby.RubyProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class TaskTwo {

    public static class CountMapper extends Mapper<Object, Text, Text, Text> {
        Text a = new Text(), b = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = (line.substring(0, line.length() - 1)).split(" ");
            int len = splits.length;
            assert len >= 2;
            for (int i = 0; i < len; i++) {
                for (int j = 0; j < len; j++) {
                    if(i == j)
                        continue;
                    a.set(splits[i] + "," + splits[j]);
                    b.set("1");
                    context.write(a, b);
                }
            }
        }
    }

    public static class TTPartionner extends HashPartitioner<Text, Text>{
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks){
            String str = key.toString();
            return super.getPartition(new Text(str.substring(0, str.indexOf(",") - 1)), value, numReduceTasks);
        }
    }

    public static class CountCombiner extends Reducer<Text, Text, Text, Text> {
        Text info = new Text();
        Text term = new Text();

        @SuppressWarnings("unused")
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // sum the count
            int sum = 0;
            for (Text value : values)
                sum += 1;
            String s[] = key.toString().split(",");
            assert (s.length != 2);
            term.set(s[0]);
            info.set(s[1] + "," + Integer.toString(sum));
            context.write(term, info);
        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> list = new ArrayList<>();
            int sum = 0;
            for (Text value : values) {
                list.add(value.toString());
                sum += Integer.parseInt(value.toString().split(",")[1]);
            }

            StringBuilder str = new StringBuilder();
            for (String value : list) {
                String s[] = value.split(",");
                double rate = (double) Integer.parseInt(s[1]) / (double) sum;
                str.append(" ").append(s[0]).append(",").append(Double.toString(rate));
            }
            str.deleteCharAt(0);
            result.set(String.valueOf(str));
            context.write(key, result);
        }
    }
}

