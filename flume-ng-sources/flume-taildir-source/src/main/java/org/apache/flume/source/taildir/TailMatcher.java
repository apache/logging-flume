package org.apache.flume.source.taildir;

import java.io.File;
import java.util.List;

/**
 * Identifies and caches the files matched
 */
public interface TailMatcher {
    List<File> getMatchingFiles();

    String getFileGroup();

    void deleteFileCache(File f);
}
