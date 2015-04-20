package edu.pitt.sis.CharCount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendCount extends Configured implements Tool {
	
	static public class FriendCountWritable implements Writable {
	    public Long user;
	    public Long mutualFriend;

	    public FriendCountWritable(Long user, Long mutualFriend) {
	        this.user = user;
	        this.mutualFriend = mutualFriend;
	    }

	    public FriendCountWritable() {
	        this(-1L, -1L);
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeLong(user);
	        out.writeLong(mutualFriend);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	        user = in.readLong();
	        mutualFriend = in.readLong();
	    }

	    @Override
	    public String toString() {
	        return " toUser: "
	                + Long.toString(user) + " mutualFriend: "
	                + Long.toString(mutualFriend);
	    }
	}
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, FriendCountWritable> {
	    private Text word = new Text();

	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line[] = value.toString().split("\t");
	        Long fromUser = Long.parseLong(line[0]);
	        List toUsers = new ArrayList();

	        if (line.length == 2) {
	            StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
	            while (tokenizer.hasMoreTokens()) {
	                Long toUser = Long.parseLong(tokenizer.nextToken());
	                toUsers.add(toUser);
	                context.write(new LongWritable(fromUser),
	                        new FriendCountWritable(toUser, -1L));
	            }

	            for (int i = 0; i < toUsers.size(); i++) {
	                for (int j = i + 1; j < toUsers.size(); j++) {
	                    context.write(new LongWritable((long) toUsers.get(i)),
	                            new FriendCountWritable((Long) (toUsers.get(j)), fromUser));
	                    context.write(new LongWritable((long) toUsers.get(j)),
	                            new FriendCountWritable((Long) (toUsers.get(i)), fromUser));
	                }
	                }
	            }
	        }
	    }
	
	public static class Reduce extends Reducer<LongWritable, FriendCountWritable, LongWritable, Text> {
	    @Override
	    public void reduce(LongWritable key, Iterable<FriendCountWritable> values, Context context)
	            throws IOException, InterruptedException {

	        // key is the recommended friend, and value is the list of mutual friends
	        final java.util.Map<Long, List> mutualFriends = new HashMap<Long, List>();
	        final java.util.Map<Long, List> alreadyFriends = new HashMap<Long,List>();
	        final ArrayList friends = new ArrayList();
	        final ArrayList alreadyfriends = new ArrayList();
	        for (FriendCountWritable val : values) {
	        	final Boolean isAlreadyFriend = (val.mutualFriend == -1);
	        	final Long toUser = val.user;
	        	final long mutualFriend = val.mutualFriend;
	        	if(friends.contains(toUser)){
	        		if(isAlreadyFriend){
	        			int index = friends.indexOf(toUser);
	        			friends.remove(index);
	        			alreadyfriends.add(toUser);
	        		}else{
	        			//
	        		}
	        	}else{
	        		if(isAlreadyFriend){
	        			alreadyfriends.add(toUser);
	        		}else{
	        			if(!alreadyfriends.contains(toUser)){
	        			friends.add(toUser);
	        			}
	        		}
	        	}
	        }
	       //if(key.equals(924)||key.equals(8941)||key.equals(8942)||key.equals(9019)||key.equals(9020)||key.equals(9021)||key.equals(9022)||key.equals(9990)||key.equals(9992)||key.equals(9993)){
	        context.write(key, new Text(friends.toString()));
	       //}
	}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "FriendCount");
	      job.setJarByClass(FriendCount.class);
	      job.setOutputKeyClass(LongWritable.class);
	      job.setOutputValueClass(FriendCountWritable.class);
	      
	    

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new FriendCount(), args);
	      
	      System.exit(res);

	}

}
