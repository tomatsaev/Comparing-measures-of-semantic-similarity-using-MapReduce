package associations;

import helpers.TaggedPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import helpers.Stemmer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Input:
 * key: <line-id>
 * value: head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year
 * syntactic-ngram: "word/pos-tag/dep-label/head-index"
 * Output:
 * key: <LexemeTag, Lexeme>
 * value: <CountTag, Count>
 * or
 * key: <FeatureTag, Lexeme>
 * value: <Feature, Count>
 */
public class BiarcAnalyzer {
    public static class MapperClass extends Mapper<LongWritable, Text, TaggedPair, TaggedPair> {
        private Stemmer stemmer;

        protected void setup(Mapper.Context context) {
            stemmer = new Stemmer();
        }
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            Set<String> lexemes = new HashSet<>();
            String[] tokens = line.toString().split("\t");
            String[] syntacticNgrams = tokens[1].split(" ");
            long total_count = Long.parseLong(tokens[2]);
//            make array from syntacticNgram and stem each word
//            syntactic-ngram: "word/pos-tag/dep-label/head-index"
            String[][] ngrams = Arrays.stream(syntacticNgrams).map(s -> s.split("/")).toArray(String[][]::new);
            String[] words =
                    Arrays.stream(ngrams)
                            .map(s -> {
                                stemmer.add(s[0].toLowerCase(Locale.ROOT).toCharArray(), s[0].toCharArray().length);
                                stemmer.stem();
                                return stemmer.toString();
                            })
                            .toArray(String[]::new);
            for (int i = 0; i < ngrams.length; i++) {
                String[] ngram = ngrams[i];
                if (ngram.length != 4)
                    continue;
                String featureWord = words[i];
                int headIndex = Integer.parseInt(ngram[3]) - 1;
                if (headIndex < 0)
                    continue;
                String headWord = words[headIndex];
                if (!lexemes.contains(headWord)){
                    lexemes.add(headWord);
                    context.write(TaggedPair.of(Associations.Tag.Lexeme,new Text(headWord)), TaggedPair.of(Associations.Tag.Count,new Text(String.valueOf(total_count))));
                }
            }


        }
    }
}
