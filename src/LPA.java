import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by darnell on 7/6/16.
 */
public class LPA {
    private static Text t1 = new Text();
    private static Text t2 = new Text();
    static class LPAMapper extends Mapper<Object, Text, Text, Text> {
        private boolean isover = false;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            int cur = Integer.parseInt(conf.get("cur_times"));
            int times = Integer.parseInt(conf.get("times"));
            isover = cur == times;
        }
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tuple = line.split("\t");
            String pageKey = tuple[0];
            if(tuple.length != 3)
                t2.set(pageKey);
            else
                t2.set(tuple[2]);

            if(isover){
                t1.set(tuple[0]);
                context.write(t2, t1);
                return;
            }

            if(!tuple[1].equals("NULL")) {
                String[] linklist = tuple[1].split(" ");
                for (String link : linklist) {
                    String[] splits = link.split(",");
                    assert splits.length == 2;
                    t1.set(splits[0]);
                    context.write(t1, t2);
                }
            }

            t1.set(pageKey);
            t2.set("|" + tuple[1]);
            context.write(t1, t2);
        }

    }


    static class LPAReducer extends Reducer<Text, Text, Text, Text> {

        private boolean isover = false;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            int cur = Integer.parseInt(conf.get("cur_times"));
            int times = Integer.parseInt(conf.get("times"));
            isover = cur == times;
        }
        private Random random = new Random();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(isover){
                StringBuilder str = new StringBuilder();
                for(Text value : values)
                    str.append(value.toString()).append(",");
                str.delete(str.length() - 1, str.length() - 1);
                t2.set(str.toString());
                context.write(key, t2);
                return;
            }
            String links = "NULL";
            HashMap<String, Integer> map = new HashMap<>();
            for(Text value : values){
                String temp = value.toString();
                if(temp.startsWith("|")){
                    links = temp.substring(1);
                    continue;
                }
                if(!map.containsKey(temp))
                    map.put(temp, 0);
                map.put(temp, map.get(temp) + 1);
            }

            String label = null;
            int max = -1;
            for(String t_label : map.keySet()){
                int temp = map.get(t_label);
                if(temp > max){
                    label = t_label;
                    max = temp;
                }
                if(temp == max){
                    int ran = random.nextInt();
                    if(ran >= 0)
                        label = t_label;
                }
            }
            t1.set(links + "\t" + label);
            context.write(key, t1);
        }
    }
}
