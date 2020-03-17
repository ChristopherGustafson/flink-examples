package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TwitterStreaming {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties twitterCredentials = new Properties();
        twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "WfU3vqLwJneAxjwyfAMzidpo2");
        twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "XXTuI8cvNQNCIFLgCj56tRPvlxFGjfAWZ5bkEV7L0u0fNBYivp");
        twitterCredentials.setProperty(TwitterSource.TOKEN, "35245669-LucieaYIvJSgOCJ52rr2PoYTritAeXpssT37IBS8X");
        twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "T0YYlBG8WyF9vpLJFHvwkSsaGjM9KyqxWRDiS4fUPzvaS");

        DataStream<String> tweetStream = env.addSource(new TwitterSource(twitterCredentials));

        tweetStream.flatMap(new TweetParser())
                .map(new MapFunction<Tweet, Tuple2<Tweet, Integer>>() {
                    @Override
                    public Tuple2<Tweet, Integer> map(Tweet tweet) throws Exception {
                        return new Tuple2<>(tweet,1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Tweet, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<Tweet, Integer> tweetIntegerTuple2) throws Exception {
                        return tweetIntegerTuple2.f0.source;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .print();

        env.execute("Twitter Streaming example");
    }


    public static class TweetParser implements FlatMapFunction<String, Tweet> {
        @Override
        public void flatMap(String s, Collector<Tweet> collector) throws Exception {
            Tweet tweet = Tweet.fromString(s);
            if (tweet != null) {
                collector.collect(tweet);
            }
        }
    }

    public static class Tweet {

        private String text;
        private String userName;
        public String rawText;
        public String lang;
        public String source;

        Tweet() {
        }

        public static Tweet fromString(String s) {

            ObjectMapper jsonParser = new ObjectMapper();
            Tweet tweet = new Tweet();
            tweet.rawText = s;

            try {
                JsonNode node = jsonParser.readValue(s, JsonNode.class);
                Boolean isLang = node.has("user") && node.get("user").has("lang");// &&
                       // !node.get("user").get("lang").asText().equals("null");

                if(isLang && node.has("text"))
                {
                    JsonNode userNode = node.get("user");
                    tweet.text = node.get("text").asText();
                    tweet.userName = userNode.get("name").asText();
                    tweet.lang = userNode.get("lang").asText();

                    if(node.has("source")){

                         String source = node.get("source").asText().toLowerCase();
                         if(source.contains("android"))
                             source = "Android";
                         else if (source.contains("iphone"))
                             source="iphone";
                         else if (source.contains("web"))
                             source="web";
                         else
                             source="unknow";

                        tweet.source =source;
                    }


                    return tweet;
                }

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;

        }

        @Override
        public String toString() {
            return this.userName+ "; "
                    +this.lang + "; "
                    +this.source + "; "
                    +this.text + "; ";
                    //+this.rawText + " ";
        }
    }
}
