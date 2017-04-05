# Insight Data Engineering Coding Challenge

A java solution to [Insight Data Engineering program coding challenge](https://github.com/InsightDataScience/fansite-analytics-challenge). The challenge is to analyze the NASA fan website's server log file that generates a lot of traffic. The main features for the challenge are to perform basic analytics on the server log file, provide useful metrics, and implement basic security measures by detecting consecutive login faliures from the same IP address. 

### Description

The main idea for the solution is to encode the flow of data transformations using functional programming paradigm through the use of Java Streams. Some of the individual transforms have used effiecient third party data structures like MinMaxPriortyQueue from Google's Guava library for aggregation and terminal sorting. 

### Dependencies


- Java 8
- [Apache Commons Lang jar](https://mvnrepository.com/artifact/org.apache.commons/commons-lang3/3.5)
- [Guava jar](http://www.java2s.com/Code/Jar/g/Downloadguavajar.htm)


This solution needs `javac` and `java` commands. So please make sure `java-version-openjdk` and `java-version-openjdk-devel` packages are installed on the Linux system.
This solution has been tested with 
<pre>
java-1.8.0_121-openjdk.x86_64
java-1.8.0_121-openjdk-devel.x86_64
</pre>

I have used Java 8 functional 8 functional progamming constructs and streams for developing the solution. So , it is necessary to use Java 8 while running the solution.

This solution also uses the [Apache Commons Lang jar](https://mvnrepository.com/artifact/org.apache.commons/commons-lang3/3.5) and  [Guava jar](http://www.java2s.com/Code/Jar/g/Downloadguavajar.htm). So please download the to the directory `./src/`, which is already included in this solution.
    

### High Level Software Design :
       
This program has been divided into four different parts- one for each problem. Each part starts with reading all the regular files in log_input folder to get the log entries as a stream, then process the stream using sequence of transforms like filter, map, flatmap and collect to solve the specific business problem. Care has been taken to cleanly separate variuos processing steps into individual self contained small functions which are easy to understand and test. Furthermore, the concern of parallel execution is de-coupled from the core business logic and left to the methods available at the stream layer.

- Method 1: getTop10Hosts :       
 Read in each of log records, extract the hostname/IP, group by hostname and count up the number of occurences, collect into a priority queue ordered by count and print the top 10 hosts to the output file.
         
- Method 2: getTop10Resources:    
    
 Read in the log records, extract the resource name and number of bytes, group by resource name and sumup the number of bytes, collect into a priority queue ordered by sum of bytes and print the top 10 resources to the output file.
         
- Method 3: getTopt10Hours   
    
 Read in the log records, extract the timestamp and transform it into range of one-hour intervals with range opening boundary beginning one-hour before the timestamp of the current log record and closing boundary at the current timestamp, collect the range boundaries into a priority queue ordered by timestamp of the range boundary, traverse the range boundary in chronological order and increase the count whenever a open range boundary is encountered and decrease the count whenever a closing range boundary is encountered thus calculating sliding range of one-hour intervals which have the same count. As a final step explode the range of intervals with common count to individual intervals with the same count and print the top 10 to the output file.
         
 - Method 4: getBlockedUsers    
    
  Read in the log records, extract the hostname, timestamp, whether it was a successful login, whether it was failed login and the whole record, group them into a chronologically ordered sequence by hostname, traverse the sequence and identify the blocked hosts using a state machine as found in problem description and print the blocked users to the output file.

### Execution

Instructions to run the code

## Instructions to Run the code
1.Clone the git repository
```
#mkdir InsightDataEngineer-FansiteAnalytics
#cd InsightDataEngineer-FansiteAnalytics
#git clone  https://github.com/mars137/InsightDataEngineer-FansiteAnalytics.git
```

2.Make run.sh executable
```
#cd InsightDataEngineer-FansiteAnalytics
#chmod +x run.sh
```

3.Run run.sh 
```
#./run.sh
```

It will utilize the jar file present in src to execute the code for all four features.

This solution is written in Java and contains one executable:
- src.Insight/Analytics/Features.java: the solution to the challenge

### Exception Handling : Following exceptions have been taken care of :

    I/O Exception : When files are not present in input and output folder; File streams are empty.
        
    Illegal State Exception : When pattern matching fails for a row.    
    
    NumberFormatException : When the bytes at the end of logs are not in proper number format.    
    
    DateTimeParseException : When the date present in the logs in not in proper format.


