package org.iu.engrcloudcomputing.mapreduce.mapred.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class JavaProcessBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

    private String jarPath;
    private int startingHeapSizeInMegabytes = 40;
    private int maximumHeapSizeInMegabytes = 128;
    private String workingDirectory;
    private List<String> classpathEntries = new ArrayList<>();
    private List<String> jarArguments = new ArrayList<>();
    private String javaRuntime = "java";

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public int getStartingHeapSizeInMegabytes() {
        return startingHeapSizeInMegabytes;
    }

    public void setStartingHeapSizeInMegabytes(int startingHeapSizeInMegabytes) {
        this.startingHeapSizeInMegabytes = startingHeapSizeInMegabytes;
    }

    public int getMaximumHeapSizeInMegabytes() {
        return maximumHeapSizeInMegabytes;
    }

    public void setMaximumHeapSizeInMegabytes(int maximumHeapSizeInMegabytes) {
        this.maximumHeapSizeInMegabytes = maximumHeapSizeInMegabytes;
    }

    private String getClasspath() {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        final int totalSize = classpathEntries.size();
        for (String classpathEntry : classpathEntries) {
            builder.append(classpathEntry);
            count++;
            if (count < totalSize) {
                builder.append(System.getProperty("path.separator"));
            }
        }
        return builder.toString();
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public void addClasspathEntry(String classpathEntry) {
        this.classpathEntries.add(classpathEntry);
    }

    public void addArgument(String argument) {
        this.jarArguments.add(argument);
    }

    public void setJavaRuntime(String javaRuntime) {
        this.javaRuntime = javaRuntime;
    }

    public Future<ProcessResult> asyncStartProcess() throws IOException {
        List<String> argumentsList = new ArrayList<>();
        argumentsList.add(this.javaRuntime);
        argumentsList.add(MessageFormat.format("-Xms{0}M", String.valueOf(this.startingHeapSizeInMegabytes)));
        argumentsList.add(MessageFormat.format("-Xmx{0}M", String.valueOf(this.maximumHeapSizeInMegabytes)));
        argumentsList.add("-classpath");
        argumentsList.add(getClasspath());
        argumentsList.add("-jar");
        argumentsList.add(this.jarPath);
        argumentsList.addAll(jarArguments);

//        argumentsList.forEach(LOGGER::info);
        return new ProcessExecutor()
                .command(argumentsList)
                .redirectOutput(Slf4jStream.of(LOGGER).asInfo())
                .start().getFuture();
    }
}