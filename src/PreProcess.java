import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.HashSet;
import java.util.List;

public class PreProcess {

    public static class MyMapper extends Mapper<Object, Text, Text, Text>{
        private static HashSet<String> library;
        private static Text nullText;
        private static BufferedReader fr;

        public void setup(Context context) throws IOException {
            library = new HashSet<>();
            nullText = new Text("");
            try {
                Path path = new Path(context.getConfiguration().get("cache"));

                fr = new BufferedReader(new FileReader(path.toString()));
                String text = fr.readLine();
                while(text != null) {
                    library.add(text);
                    text = fr.readLine();
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache:" + e);
            }
            for(String str:library){
                UserDefineLibrary.insertWord(str,"nr",1000);
            }
        }

        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            HashSet<String> set = new HashSet<>();
            String s = value.toString();
            StringBuilder builder = new StringBuilder();


            List<Term> names = ToAnalysis.parse(s);
            int count = 0;
            for(Term t:names) {
                String name = t.getRealName();
                if(library.contains(name) && !set.contains(name)) {
                    builder.append(t.getRealName());
                    builder.append(" ");
                    set.add(name);
                    count++;
                }
            }

            if(count == 1 || count == 0)
                return;
            builder.delete(builder.length() - 1, builder.length());
            Text text = new Text(builder.toString());
            context.write(text,nullText);
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
            for(Text t : values)
                context.write(key,new Text(""));
        }
    }

    public static void deleteDir(File file) throws FileNotFoundException {
        if(!file.exists())
            throw new FileNotFoundException();
        if(file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files)
                deleteDir(f);
        }
    }

    public static void run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        if(args.length != 3){
            System.out.println("args not match!");
            System.exit(-1);
        }




        File dir = new File(args[2]);
        if(dir.exists())
            deleteDir(dir);
        Configuration conf=new Configuration();
        conf.set("cache", args[1].substring(args[1].lastIndexOf("/") + 1, args[1].length()));
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length!=3) {
            System.err.println("Usage:PreProcess<in><out>");
            System.exit(2);
        }
        Job job=new Job(conf,"PreProcess");

        job.addCacheFile(new Path(args[1]).toUri());
        job.setJarByClass(PreProcess.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        Path outputPath = new Path(otherArgs[2]);

        System.out.println(System.getProperty("user.dir"));
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        outputPath.getFileSystem(conf).delete(outputPath,true);

        boolean res =  job.waitForCompletion(true);
        if(!res){
            System.out.printf("task preprocess return false\n");
            System.exit(-1);

        }
    }


    public static void main(String[] args) throws Exception {
        run(args);
    }

}
