//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package example;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.*;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
        private Text keyInfo=new Text();
        private Text valueInfo=new Text();
        private FileSplit split;

        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            //获得<key,value>对所属的对象
            split=(FileSplit)context.getInputSplit();
            StringTokenizer itr=new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                //key值有单词和url组成，如"mapreduce:1.txt"
                String temp = (split.getPath().getName());
                if(temp.contains("."))
                    keyInfo.set(itr.nextToken()+":"+ temp.substring(0, temp.indexOf('.')));
                else
                    keyInfo.set(itr.nextToken() + ":" + temp);
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }

        }
    }
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
        private Text info=new Text();
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException {
            //统计词频
            int sum = 0;
            for (Text value : values)
                sum += 1;

            /*
            int splitIndex=key.toString().indexOf(":");
            //重新设置value值由url和词频组成
            info.set(key.toString().substring(splitIndex+1)+":"+sum);
            //重新设置key值为单词
            key.set(key.toString().substring(0,splitIndex));
            */
            String []splits = key.toString().split(":");
            info.set(splits[1] + ":" + Integer.toString(sum));
            context.write(new Text(splits[0]), info);
        }
    }

    public static class NewPartitioner extends HashPartitioner<Text, Text> {
        private Text term = new Text();
        public int getPartition(Text key, Text value, int numReduceTasks){
            term.set(key.toString().split(":")[0]);
            return super.getPartition(term, value, numReduceTasks);
        }
    }
    public static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
            ArrayList<String> list = new ArrayList<>();
            int sum = 0;
            int size = 0;
            for (Text value:values) {
                list.add(value.toString());
                sum += Integer.parseInt(value.toString().split(":")[1]);
                size++;
            }
            Collections.sort(list);

            StringBuilder str = new StringBuilder();
            for(String value : list)
                str.append(";").append(value);
            str.deleteCharAt(0);
            str.insert(0, String.valueOf((double)sum / size) + ",");
            result.set(String.valueOf(str));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        if(args.length != 2){
            System.out.println("args not match!");
            System.exit(-1);
        }

        File dir = new File(args[1]);
        if(dir.exists())
            Tools.deleteDir(dir);
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length!=2) {
            System.err.println("Usage:invertedindex<in><out>");
            System.exit(2);
        }
        Job job=new Job(conf,"InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReduce.class);
        //job.setPartitionerClass(NewPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)?0:1);

    }

}
