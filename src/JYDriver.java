import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.*;

/**
 * Created by darnell on 5/26/16.
 */

public class JYDriver {
    static interface JobRunable{
        public void run(Configuration conf, String input, String output) throws InterruptedException, IOException, ClassNotFoundException;
    }
    static public void runJob(Configuration conf, String jobName, Class jarClass, Class mapperClass, Class mapperKeyClass,
                              Class mapperValueClass, Class reducerClass, Class reducerKeyClass, Class  reducerValueClass,
                              String inputFileName, String outputFileName, Class comparator, Class combiner, Class partitionner, int reduceNum
    ) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=new Job(conf, jobName);
        job.setJarByClass(jarClass);
        job.setNumReduceTasks(reduceNum);

        if(comparator != null)
            job.setSortComparatorClass(comparator);

        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(mapperKeyClass);
        job.setMapOutputValueClass(mapperValueClass);

        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(reducerKeyClass);
        job.setOutputValueClass(reducerValueClass);

        if(combiner != null)
            job.setCombinerClass(combiner);
        if(partitionner != null)
            job.setPartitionerClass(partitionner);

        Path path = new Path(outputFileName);// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }

        FileInputFormat.addInputPath(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));
        boolean res = job.waitForCompletion(true);
        if(!res){
            System.out.printf("task %s return false\n", jobName);
            System.exit(-1);
        }
    }



    static String iterMapReduce(Configuration conf, String input, String output, int times, JobRunable runner, JobRunable cleanner) throws InterruptedException, IOException, ClassNotFoundException {
        String prefix = output.substring(0, output.lastIndexOf("/"));
        String outpath1 = prefix + "/out1";
        String outpath2 = prefix + "/out2";
        String in = null, out = null;
        conf.set("times", String.valueOf(times));
        for(int i = 0; i < times; i++) {
            if(i == 0) {
                in = input;
                out = outpath1;
            }else if(i == 1){
                in = outpath1;
                out = outpath2;
            }else{
                String temp = out;
                out = in;
                in = temp;
            }
            conf.setInt("cur_times", i);
            runner.run(conf, in, out);
        }

        conf.setInt("cur_times", times);
        cleanner.run(conf, out, output);
        return out;
    }


    static void generateGraphConfFile(String input) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(input + "/part-r-00000"));
        String prefix = input.substring(0, input.lastIndexOf('/'));
        PrintWriter pw1 = new PrintWriter(prefix + "GraphEdgeConfFile.csv");
        PrintWriter pw2 = new PrintWriter(prefix + "GraphNodeConfFile.csv");
        pw1.println("Source,Target,Weight, Class");
        pw2.println("Id,Label,Class,Pagerank");

        String line = br.readLine();
        while(line != null){
            String[] tuple = line.split("\\t");
            assert tuple.length == 4;
            String[] linkList = tuple[1].split(" ");
            for(String link : linkList){
                String[] pair = link.split(",");
                pw1.printf("%s,%s,%s,%s\n", tuple[0], pair[0], pair[1], tuple[3]);
            }
            pw2.printf("%s,%s,%s,%s\n", tuple[0], tuple[0], tuple[3], tuple[2]);
            line = br.readLine();
        }
        br.close();
        pw1.close();
        pw2.close();


    }

    static void run(String input, String nametable, String output){
        try {
            String prefix = output.substring(0, output.lastIndexOf("/"));
            Configuration conf = new Configuration();



            //preprocess
            String preprocess_output = prefix + "/preprocess";
            String[] args = {input, nametable, preprocess_output};
            PreProcess.run(args);


            //feature process
            String feature_output = prefix + "/feature";
            runJob(conf, "feature select", TaskTwo.class, TaskTwo.CountMapper.class, Text.class, Text.class,
                    TaskTwo.CountReducer.class, Text.class, Text.class, preprocess_output, feature_output,
                    null, TaskTwo.CountCombiner.class, TaskTwo.TTPartionner.class, 4);


            //pagerank
            conf.setDouble("PR_init", 0.5);
            conf.setDouble("damp", 0.85);
            int times = 15;
            String pagerank_output = prefix + "/pagerank";
            String tempPath = iterMapReduce(conf, feature_output, pagerank_output, times, new JobRunable() {
                        @Override
                        public void run(Configuration conf, String input, String output) throws InterruptedException, IOException, ClassNotFoundException {
                            runJob(conf, "PageRankIter",
                                    PageRankIter.class, PageRankIter.PRIterMapper.class, Text.class, Text.class,
                                    PageRankIter.PRIterReducer.class, Text.class, Text.class, input, output, null, null, null, 4);
                        }
                    },
                    new JobRunable() {
                        @Override
                        public void run(Configuration conf, String input, String output) throws InterruptedException, IOException, ClassNotFoundException {
                            runJob(conf, "Sort", PageRankSort.class, PageRankSort.SortMapper.class, MyK2.class, Text.class,
                                    PageRankSort.SortReducer.class, Text.class, MyK2.class, input, output, null, null, null, 1);
                        }
                    });
            generateGraphConfFile(tempPath);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] argv){
        if(argv.length != 3){
            System.err.printf("error\n");
            System.exit(-1);
        }
        run(argv[0], argv[1], argv[2]);
    }
}
