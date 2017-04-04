/**
 * Created by hadoop-user on 4/3/17.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class Features {
    final int k = 100; // Priority queue size 'k'
    final int n = 10;  // Top 'n' elements from the final priority queue to be returned. n <= k

    public static void main(String[] args) throws IOException, InterruptedException {
        String log_input = "/home/hadoop-user/Insight-Challenge/FansiteAnalytics/log_input/";
        String log_output = "/home/hadoop-user/Insight-Challenge/FansiteAnalytics/log_output/";

        /* Check if log_input and log_output exists */
        if (!Files.exists(Paths.get(log_input)) ||
                !Files.exists(Paths.get(log_output))) {
            System.out.println("Input or Output directory does not exists. Exiting!!");
            System.exit(1);
        }

        // Create threads for each feature:
        Thread thread1 = new Thread() {
            public void run() {
                try {
                    new Features().getTop10Hosts(log_input, log_output);
                } catch (IOException e) {
                    System.out.println("System error!!");
                }
            }
        };

        Thread thread2 = new Thread() {
            public void run() {
                try {
                    new Features().getTop10Resources(log_input, log_output);
                } catch (IOException e) {
                    System.out.println("System error!!");
                }
            }
        };

        Thread thread3 = new Thread() {
            public void run() {
                try {
                    new Features().getTopt10Hours(log_input, log_output);
                } catch (IOException e) {
                    System.out.println("System error!!");
                }
            }
        };

        // Start the threads.
        thread1.start();
        thread2.start();
        thread3.start();

        // Wait for them both to finish
        thread1.join();
        thread2.join();
        thread3.join();

        /*
        Features obj = new Features();
        obj.getTop10Hosts(log_input, log_output);
        obj.getTop10Resources(log_input, log_output);
        obj.getTopt10Hours(log_input, log_output);
        */
    }

    public void getTop10Hosts(String log_input, String log_output)
            throws  IOException {
        System.out.println("TOP 10 HOSTS START TIME:" + LocalDateTime.now());
        System.out.println();

        try(Stream<Path> paths = Files.walk(Paths.get(log_input))) {
            MinMaxPriorityQueue<Pair<String, Long>> topk = paths
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
                            Matcher m = Pattern.compile("([^\\ ]*)").matcher(line);
                            if (m.find())
                                return Stream.of(m.group(1));
                            else
                                return Stream.empty();
                        } catch (IllegalStateException e) {
                            System.out.println("Pattern match failure!!");
                            return Stream.empty();
                        }
                    })
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .entrySet()
                    .parallelStream()
                    .map(e -> new MutablePair<String, Long>(e.getKey(), e.getValue()))
                    .collect(() -> MinMaxPriorityQueue.orderedBy(new Comparator<Pair<String, Long>>() {
                        @Override
                        public int compare(final Pair<String, Long> o1, final Pair<String, Long> o2) {
                            if (o1.getValue() == o2.getValue())
                                return 0;
                            else if (o1.getValue() < o2.getValue())
                                return 1;
                            else
                                return -1;
                        }
                    }).maximumSize(k).create(), (q, p) -> q.add(p), (q1, q2) -> q1.addAll(q2));

            for (int i = 0; i < n && i <= topk.size(); i++) {
                System.out.println(topk.removeFirst().toString());
            }
        }

        System.out.println();
        System.out.println("TOP 10 HOSTS END TIME:" + LocalDateTime.now());
    }

    public void getTop10Resources(String log_input, String log_output)
            throws IOException {
        System.out.println("TOP 10 RESOURCES START TIME:" + LocalDateTime.now());
        System.out.println();

        try(Stream<Path> paths = Files.walk(Paths.get(log_input))) {
            MinMaxPriorityQueue<Pair<String, Long>> topk = paths
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
                        String resource = "";
                        int httpsStatus = 0;
                        long bytes = 0L;
                        try {
                            Matcher m = Pattern.compile("[^\"]*\"(?:GET|POST)\\ ([^\\ ]*)[^\"]*\"\\ ([\\d]*)\\ ([\\d]*)").matcher(line);
                            if (m.find()) {
                                resource = m.group(1);
                                try {
                                    httpsStatus = Integer.parseInt(m.group(2));
                                } catch (NumberFormatException e) {
                                    httpsStatus = 0;
                                }
                                try {
                                    bytes = Long.parseLong(m.group(3));
                                } catch (NumberFormatException e) {
                                    bytes = 0L;
                                }
                                return Stream.of(new Resource(resource, httpsStatus, bytes));
                            }
                            else {
                                return Stream.empty();
                            }
                        } catch (IllegalStateException e) {
                            System.out.println("Pattern match failure!!");
                            return Stream.empty();
                        }
                    })
                    .filter(obj -> obj.getHttpStatusCode() >= 200 && obj.getHttpStatusCode() < 300) // only valid HTTP requests
                    .collect(Collectors.groupingBy(Resource::getResource, Collectors.summingLong(Resource::getBytes)))
                    .entrySet()
                    .parallelStream()
                    .map(e -> new MutablePair<String, Long>(e.getKey(), e.getValue()))
                    .collect(() -> MinMaxPriorityQueue.orderedBy(new Comparator<Pair<String, Long>>() {
                        @Override
                        public int compare(final Pair<String, Long> o1, final Pair<String, Long> o2) {
                            if (o1.getValue() == o2.getValue())
                                return 0;
                            else if (o1.getValue() < o2.getValue())
                                return 1;
                            else
                                return -1;
                        }
                    }).maximumSize(k).create(), (q, p) -> q.add(p), (q1, q2) -> q1.addAll(q2));

            for (int i = 0; i < n && i <= topk.size(); i++) {
                System.out.println(topk.removeFirst().toString());
            }
        }

        System.out.println();
        System.out.println("TOP 10 RESOURCES:" + LocalDateTime.now());
    }

    public void getTopt10Hours(String log_input, String log_output)
            throws IOException {
        System.out.println("TOP 10 HOURS START TIME:" + LocalDateTime.now());
        System.out.println();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
        try (Stream<Path> paths = Files.walk(Paths.get(log_input))) {
            MinMaxPriorityQueue<Pair<String, Long>> busyhours =
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
                                    Matcher m = Pattern.compile("([^\\[]*\\[([^\\]]*)\\])").matcher(line);
                                    if (m.find())
                                        return Stream.of(m.group(2));
                                    else
                                        return Stream.empty();
                                } catch (IllegalStateException e) {
                                    System.out.println("Pattern match failure!!");
                                    return Stream.empty();
                                }
                            })
                            .flatMap(tsp -> {
                                try {
                                    ZonedDateTime zDateTime = ZonedDateTime.parse(tsp, dtf);
                                    Calendar c = Calendar.getInstance();
                                    c.setTimeInMillis(zDateTime.toInstant().toEpochMilli());
                                    c.add(Calendar.MINUTE, -1 * (c.get(Calendar.MINUTE) % 60));
                                    c.set(Calendar.SECOND, 0);
                                    c.set(Calendar.MILLISECOND, 0);
                                    return Stream.of(ZonedDateTime.ofInstant(Instant.ofEpochMilli(c.getTimeInMillis()), zDateTime.getZone()).format(dtf));
                                } catch (DateTimeParseException e) {
                                    System.out.println("Timestamp match failure!!" + tsp);
                                    return Stream.empty();
                                }
                            })
                            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                            .entrySet()
                            .stream()
                            .map(e -> new MutablePair<String, Long>(e.getKey(), e.getValue()))
                            .collect(() -> MinMaxPriorityQueue.orderedBy((new Comparator<Pair<String, Long>>() {
                                @Override
                                public int compare(final Pair<String, Long> o1, final Pair<String, Long> o2) {
                                    if (o1.getValue() == o2.getValue())
                                        return 0;
                                    else if (o1.getValue() < o2.getValue())
                                        return 1;
                                    else
                                        return -1;
                                }
                            })).maximumSize(k).create(), (q, p) -> q.add(p), (q1, q2) -> q1.addAll(q2));

            for (int i = 0; i < n && i <= busyhours.size(); i++) {
                System.out.println(busyhours.removeFirst().toString());
            }
        }

        System.out.println();
        System.out.println("TOP 10 HOURS START TIME:" + LocalDateTime.now());
    }
}

class Resource {
    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public void setHttpStatusCode(int httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public Resource(String resource, int httpStatusCode, long bytes) {
        this.resource = resource;
        this.httpStatusCode = httpStatusCode;
        this.bytes = bytes;
    }

    @Override
    public String toString() {
        return "BandWidth{" +
                "resource='" + resource + '\'' +
                ", httpStatusCode=" + httpStatusCode +
                ", bytes=" + bytes +
                '}';
    }

    String resource;
    int httpStatusCode;
    long bytes;
}
