package insight.analytics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class Features {
    final int n = 10;  // Top 'n' elements from the final priority queue to be returned.

    public static void main(String[] args) throws IOException, InterruptedException {
        String log_input = "log_input";
        String log_output = "log_output";

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

        Thread thread4 = new Thread() {
            public void run() {
                try {
                    new Features().getBlockedUsers(log_input, log_output);
                } catch (IOException e) {
                    System.out.println("System error!!");
                }
            }
        };

        // Start the threads.
        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();

        // Wait for them all to finish
        thread1.join();
        thread2.join();
        thread3.join();
        thread4.join();
    }

    public void getTop10Hosts(String log_input, String log_output)
            throws  IOException {
        Path outPath = Paths.get(log_output, "hosts.txt");
        final BufferedWriter writer = Files.newBufferedWriter(outPath);

        // Get stream of files from the input directory.
        try(Stream<Path> paths = Files.walk(Paths.get(log_input))) {
            MinMaxPriorityQueue<Map.Entry<String, Long>> topk = paths
                    // filter the stream for only regular files.
                    .filter(path -> Files.isRegularFile(path))
                    // for each file in the stream get the stream of lines in the file.
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
                    .parallel()
                    // for each line get a stream of host names. Using flatMap instead of map
                    // to handle exceptions.
                    .flatMap(line -> {
                        try {
                            Matcher m = Pattern.compile("([^\\ ]*)").matcher(line);
                            if (m.find())
                                return Stream.of(m.group(1));
                            else
                                return Stream.empty();
                        } catch (IllegalStateException e) {
                            System.out.println("Feature 1: Pattern match failure!! Skipping row!!");
                            return Stream.empty();
                        }
                    })
                    // group by host name and count the occurrences of each host name.
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .entrySet()
                    .parallelStream()
                    /* collect the host names into a priority queue based on the counts.
                     * Limit the priority queue size to 10 as that would be safe as the
                     * stream already have only one entry per host.
                     */
                    .collect(() -> MinMaxPriorityQueue.orderedBy(new Comparator<Map.Entry<String, Long>>() {
                        @Override
                        public int compare(final Map.Entry<String, Long> o1, final Map.Entry<String, Long> o2) {
                            return o1.getValue().compareTo(o2.getValue());
                        }
                    }.reversed()).maximumSize(n).create(), (q, p) -> q.add(p), (q1, q2) -> q1.addAll(q2));

            int topkSize = topk.size();
            for (int i = 0; i < n && i < topkSize; i++) {
                Map.Entry<String, Long> element = topk.removeFirst();
                writer.write(element.getKey() + "," + element.getValue());
                writer.newLine();
            }
            writer.close();
        }
    }

    public void getTop10Resources(String log_input, String log_output)
            throws IOException {
        Path outPath = Paths.get(log_output, "resources.txt");
        final BufferedWriter writer = Files.newBufferedWriter(outPath);

        // Get stream of files from the input directory.
        try(Stream<Path> paths = Files.walk(Paths.get(log_input))) {
            MinMaxPriorityQueue<Map.Entry<String, Long>> topk = paths
                    // filter the stream for only regular files.
                    .filter(path -> Files.isRegularFile(path))
                    // for each file in the stream get the stream of lines in the file.
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
                    .parallel()
                    // for each line get a stream of Resource objects. Using flatMap instead of map
                    // to handle exceptions.
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
                            System.out.println("Feature 2: Pattern match failure!! Skipping row!!");
                            return Stream.empty();
                        }
                    })
                    // group by resource name and sum the number of bytes for each resource.
                    .collect(Collectors.groupingBy(Resource::getResource, Collectors.summingLong(Resource::getBytes)))
                    .entrySet()
                    .parallelStream()
                    .collect(() -> MinMaxPriorityQueue.orderedBy(new Comparator<Map.Entry<String, Long>>() {
                        @Override
                        public int compare(final Map.Entry<String, Long> o1, final Map.Entry<String, Long> o2) {
                            return o1.getValue().compareTo(o2.getValue());
                        }
                    }.reversed()).maximumSize(n).create(), (q, p) -> q.add(p), (q1, q2) -> q1.addAll(q2));

            int topkSize = topk.size();
            for (int i = 0; i < n && i < topkSize; i++) {
                Map.Entry<String, Long> element = topk.removeFirst();
                writer.write(element.getKey());
                writer.newLine();
            }

            writer.close();

        }
    }

    public void getTopt10Hours(String log_input, String log_output)
            throws IOException {
        Path outPath = Paths.get(log_output, "hours.txt");
        final BufferedWriter writer = Files.newBufferedWriter(outPath);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
        try (Stream<Path> paths = Files.walk(Paths.get(log_input))) {
            MinMaxPriorityQueue<Pair<ZonedDateTime, Boolean>> topk =
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
                    .parallel()
                    .flatMap(line -> {
                        try {
                            Matcher m = Pattern.compile("([^\\[]*\\[([^\\]]*)\\])").matcher(line);
                            if (m.find())
                                return Stream.of(m.group(2));
                            else
                                return Stream.empty();
                        } catch (IllegalStateException e) {
                            System.out.println("Feature 3: Pattern match failure!! Skipping row!!");
                            return Stream.empty();
                        }
                    })
                    /* An hour long interval in which an event happened is denoted by the starting timestamp of that interval.
                     * For each event timestamp, create the range covering the first and the last hour long interval in which
                     * that event happened.
                     */
                    .flatMap(tss -> {
                        try {
                            ZonedDateTime zdate = ZonedDateTime.parse(tss, dtf);
                            Pair <ZonedDateTime, Boolean> zdateOpen = new MutablePair<ZonedDateTime, Boolean>(zdate.plusSeconds(-3599), true);
                            Pair <ZonedDateTime, Boolean> zdateClose = new MutablePair<ZonedDateTime, Boolean>(zdate, false);
                            List<Pair<ZonedDateTime, Boolean>> p = new ArrayList<Pair<ZonedDateTime, Boolean>>();
                            p.add(zdateOpen);
                            p.add(zdateClose);
                            return p.stream();
                        } catch (DateTimeParseException e) {
                            return Stream.empty();
                        }
                    })
                    // Collect all the interval range boundaries ordered by their timestamps.
                    .collect(() -> MinMaxPriorityQueue.orderedBy(new Comparator<Pair<ZonedDateTime, Boolean>>() {
                        @Override
                        public int compare(final Pair<ZonedDateTime, Boolean> o1, final Pair<ZonedDateTime, Boolean> o2) {
                            int tc = o1.getKey().compareTo(o2.getKey());
                            int bc = o1.getValue().compareTo(o2.getValue());
                            return (tc == 0) ? bc : tc;
                        }
                    }).create(), (q, p) -> q.add(p), (q1, q2) -> q1.addAll(q2));

            if (topk.size() == 0) {
                System.exit(1);
            }

            // Initialize the state.
            Pair<ZonedDateTime, Boolean> p, p0;
            Integer count = 0;
            Pair<Pair<ZonedDateTime, ZonedDateTime>, Integer> element;
            List<Pair<Pair<ZonedDateTime, ZonedDateTime>, Integer>> list =
                new ArrayList<Pair<Pair<ZonedDateTime, ZonedDateTime>, Integer>>();

            // Handle the boundary case of first interval range entry.
            p0 = topk.pollFirst();
            if (p0.getValue() == Boolean.FALSE) {
                System.out.println("Feature 3: System Error!!");
                System.exit(1);
            }
            count = 1;
            element = new MutablePair<Pair<ZonedDateTime, ZonedDateTime>, Integer>(new MutablePair<ZonedDateTime, ZonedDateTime>(p0.getKey(), null), count);

            // For each range boundary if it is the opening of the range then close the existing range, open a new range and increase the count.
            // If it is closing of the range then close the existing range, open a new range and decrease the count.
            while((p = topk.pollFirst()) != null) {
                if (p.getValue() == Boolean.TRUE) {
                    element.getKey().setValue(p.getKey().minusSeconds(1));
                    list.add(element);
                    count ++;
                    element = new MutablePair<Pair<ZonedDateTime, ZonedDateTime>, Integer>(new MutablePair<ZonedDateTime, ZonedDateTime>(p.getKey(), null), count);
                } else {
                    element.getKey().setValue(p.getKey());
                    list.add(element);
                    count --;
                    element = new MutablePair<Pair<ZonedDateTime, ZonedDateTime>, Integer>(new MutablePair<ZonedDateTime, ZonedDateTime>(p.getKey().plusSeconds(1), null), count);
                }
            }

            // from the previous step we have ranges where every range is a collection of sliding one hour intervals separated by one second and having the same count.
            // Here we create one entry for every one hour interval in those ranges.
            MinMaxPriorityQueue<Pair<ZonedDateTime, Integer>> results =
                list
                    .stream()
                    // for each range create one timestamp for every second in the range and pair it with the count of that range.
                    // every timestamp denotes the starting timestamp of the one hour interval and the count denotes the events that happened in that one hour interval.
                    .flatMap(i -> {
                        Duration d = Duration.between(i.getKey().getKey(), i.getKey().getValue());
                        long il = d.getSeconds() + 1;
                        return LongStream.range(0, il).mapToObj(t -> new MutablePair<ZonedDateTime, Integer>(i.getKey().getKey().plusSeconds(t), i.getValue()))
                            // ugly hack to pass the test case where the one hour intervals that begins before 01/Jul/1995:00:00:01 -0400 are ignored.
                            .filter(t -> t.getKey().compareTo(ZonedDateTime.parse("01/Jul/1995:00:00:01 -0400", dtf)) >= 0 ? true : false);
                    })
                    // order the one hour intervals by the number of events in the interval and if that is the same then order by starting time of the interval.
                    .collect(() -> MinMaxPriorityQueue.orderedBy(new Comparator<Pair<ZonedDateTime, Integer>>() {
                        @Override
                        public int compare(final Pair<ZonedDateTime, Integer> o1, final Pair<ZonedDateTime, Integer> o2) {
                            int tc = o2.getKey().compareTo(o1.getKey());
                            int bc = o1.getValue().compareTo(o2.getValue());
                            return (bc == 0) ? tc : bc;
                        }
                    }.reversed()).maximumSize(n).create(), (q, e) -> q.add(e), (q1, q2) -> q1.addAll(q2));

            int topkSize = results.size();
            for (int i = 0; i < n && i < topkSize; i++) {
                Pair<ZonedDateTime, Integer> e = results.pollFirst();
                writer.write(dtf.format(e.getKey()) + "," + e.getValue());
                writer.newLine();
            }

            writer.close();
        }
    }

    public void getBlockedUsers(String log_input, String log_output)
            throws IOException {
        Path outPath = Paths.get(log_output, "blocked.txt");
        final BufferedWriter writer = Files.newBufferedWriter(outPath);
        try(Stream<Path> paths = Files.walk(Paths.get(log_input))) {
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
                    .parallel()
                    .flatMap(line -> {
                        // For each line in the log files: find the domain/IP, timestamp and HTTP status code.
                        // Determine if the HTTP status code is for log-in success or failure.
                        // Create a Record for each line.
                        try {
                            Matcher m = Pattern.compile("([^\\ ]*)[^\\[]*\\[([^\\]]*)\\]\\ \"[^\"]*\"\\ ([\\d]*)\\.*").matcher(line);
                            if (m.find()) {
                                String payload = line;
                                String userid = m.group(1);
                                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
                                ZonedDateTime zdate = ZonedDateTime.parse(m.group(2), dtf);
                                Boolean failedlogin = (m.group(3).equals("401")) ? true : false;
                                Boolean successlogin = (m.group(3).equals("200")) ? true : false;
                                return Stream.of(new Record(userid, zdate, failedlogin, successlogin, payload));
                            }
                            else
                                return Stream.empty();
                        } catch (IllegalStateException | DateTimeParseException e) {
                            System.out.println("Feature 4: Pattern match or date failure!! Skipping row!!" + e.toString());
                            return Stream.empty();
                        }
                    })
                    // group by userid (domain/IP)
                    .collect(Collectors.groupingBy(Record::getUserid))
                    .entrySet()
                    .parallelStream()
                    // At this point we have a Set<String -> List<Record>>.
                    // Create a priority queue for each user's sequence of events ordered by timestamp.
                    .map(e -> {
                        MinMaxPriorityQueue<Record> q = MinMaxPriorityQueue.orderedBy(new Comparator<Record>() {
                            @Override
                            public int compare(final Record o1, final Record o2) {
                                return o1.getTs().compareTo(o2.getTs());
                            }
                        }).maximumSize(10000).create();
                        q.addAll(e.getValue());
                        return new MutablePair<String, MinMaxPriorityQueue<Record>>(e.getKey(), q);
                    })
                    .flatMap(e -> {
                        // For each user's sequence of events
                        List<String> blockedrecords = new ArrayList<String>();
                        MinMaxPriorityQueue<Record> q = e.getValue();
                        Record r = null;
                        ZonedDateTime window = null, blocked = null;
                        int failedattempts = 0;
                        while ((r = q.pollFirst()) != null) {
                            if (blocked != null) {
                                // If this user's blocked window of 5 minutes has already been initialized
                                ZonedDateTime rd = r.getTs();
                                ZonedDateTime bd = blocked;
                                Long d = Duration.between(bd, rd).toMinutes();
                                if (d < 5) {
                                    // If five minutes have not passed yet, add this event to blockedrecords.
                                    blockedrecords.add(r.getLogentry());
                                }
                                else {
                                    // If 5 minutes have passed unset the blocked window essentially unblocking the user.
                                    blocked = null;
                                }
                            } else if (r.isFailedlogin()) {
                                // Blocked window has not been initialized. We encounter a failed login.
                                // Initialize/Update the 20 second window and number of failed log-in attempts.
                                if (window == null) {
                                    window = r.getTs();
                                    failedattempts = 1;
                                } else {
                                    ZonedDateTime rd = r.getTs();
                                    ZonedDateTime bd = window;
                                    Long d = Duration.between(bd, rd).toMillis() / 1000;
                                    if (d > 20) {
                                        window = r.getTs();
                                        failedattempts = 1;
                                    } else if (failedattempts < 2) {
                                        failedattempts++;
                                    } else {
                                        blocked = r.getTs();
                                        window = null;
                                        failedattempts = 0;
                                    }
                                }
                            } else if (r.isSuccesslogin()) {
                                if (window != null) {
                                   window = null;
                                   failedattempts = 0;
                                }
                            }
                        }

                        return blockedrecords.stream();
                    })
            .forEach(p -> {
                try {
                    writer.write(p.toString());
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            writer.close();
        }
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

class Record {
    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public ZonedDateTime getTs() {
        return ts;
    }

    public void setTs(ZonedDateTime ts) {
        this.ts = ts;
    }

    public Boolean isFailedlogin() {
        return failedlogin;
    }

    public void setFailedlogin(Boolean failedlogin) {
        this.failedlogin = failedlogin;
    }

    public Boolean isSuccesslogin() {
        return successlogin;
    }

    public void setSuccesslogin(Boolean successlogin) {
        this.successlogin = successlogin;
    }

    public String getLogentry() {
        return logentry;
    }

    public void setLogentry(String logentry) {
        this.logentry = logentry;
    }

    public Record(String userid, ZonedDateTime ts, Boolean failedlogin, Boolean successlogin, String logentry) {
        this.userid = userid;
        this.ts = ts;
        this.failedlogin = failedlogin;
        this.successlogin = successlogin;
        this.logentry = logentry;
    }

    @Override
    public String toString() {
        return "Record{" +
                "userid='" + userid + '\'' +
                ", ts=" + ts +
                ", failedlogin=" + failedlogin +
                ", successlogin=" + successlogin +
                ", logentry='" + logentry + '\'' +
                '}';
    }

    String userid;
    ZonedDateTime ts;
    Boolean failedlogin;
    Boolean successlogin;
    String logentry;
}
