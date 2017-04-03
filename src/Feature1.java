/**
 * Created by hadoop-user on 4/3/17.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Feature1 {
    public static void main(String[] args) throws IOException {
        System.out.println(LocalDateTime.now());

        Comparator<Map.Entry<String, Integer>> byValue = (entry1, entry2) -> entry1.getValue().compareTo(
                entry2.getValue());
        Path outPath = Paths.get("/home/hadoop-user/Insight-Challenge/FansiteAnalytics/log_output/hosts.txt");
        final BufferedWriter writer = Files.newBufferedWriter(outPath);

        try(Stream<Path> paths = Files.walk(Paths.get("/home/hadoop-user/Insight-Challenge/FansiteAnalytics/log_input/"))) {

            paths
                    .filter(path -> Files.isRegularFile(path))
                    .flatMap(path -> {
                        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.IGNORE);
                        try {
                            Reader r = Channels.newReader(FileChannel.open(path), decoder, -1);
                            BufferedReader br = new BufferedReader(r);
                            return br.lines();
                        } catch (IOException e) {
                            return Stream.empty();
                        }
                    })
                    .flatMap(line -> {
                        try {
                            Matcher m = Pattern.compile("(.*?)(\\s-\\s-\\s)(.*)").matcher(line);
                            if (m.find())
                                return Stream.of(m.group(1));
                            else
                                return Stream.empty();
                        } catch (IllegalStateException e) {
                            System.out.println("Pattern match failure!!");
                            return Stream.empty();
                        }
                    })
                    .parallel()
                    .collect(Collectors.toConcurrentMap(w -> w, w -> 1, Integer::sum))
                    .entrySet()
                    .stream()
                    .sorted((Comparator<? super Map.Entry<String, Integer>>) byValue.reversed())
                    .limit(10)
                    .sorted((Comparator<? super Map.Entry<String, Integer>>) byValue.reversed())
                    .forEach(line -> {
                        try {
                            writer.write(line.getKey() + "," + line.getValue());
                            writer.newLine();
                        } catch (IOException e) {
                            System.out.println("Error writing to output file!!");;
                        }
                    });
        }

        writer.close();

        System.out.println(LocalDateTime.now());
    }
}

//"(.*)(\\s-\\s-\\s)(.*)"
// HOST "(.*?)(\\s-\\s-\\s)(.*)" group(1)
// TS "(.*?)(\\s-\\s-\\s)\\[(.*)\\](.*)" group(3)
// REQUEST "(.*?)(\\s-\\s-\\s)\\[(.*)\\]\"GET\\s(.*)\\sHTTP/1\\.0\"(.*)"
// REPLY
// BYTES

