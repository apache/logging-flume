/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.taildir;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Identifies and caches the files matched by single file pattern for {@code TAILDIR} source.
 * <p></p>
 * file patterns apply to the fileNames and files in subdirectories,
 * implementation checks the parent directory and subdirectories for modification
 * (additional or removed files update modification time of dir)
 * If no modification happened to the dir that means the underlying files could only be
 * written to but no need to rerun the pattern matching on fileNames.
 * If a directory has modified, can only re-match the directory, no need to match other directories
 * <p></p>
 * This implementation provides lazy caching or no caching. Instances of this class keep the
 * result file list from the last successful execution of {@linkplain #getMatchingFiles()}
 * function invocation, and may serve the content without hitting the FileSystem for performance
 * optimization.
 * <p></p>
 * <b>IMPORTANT:</b> It is assumed that the hosting system provides at least second granularity
 * for both {@code System.currentTimeMillis()} and {@code File.lastModified()}. Also
 * that system clock is used for file system timestamps. If it is not the case then configure it
 * as uncached. Class is solely for package only usage. Member functions are not thread safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TaildirIncludeChildMatcher implements TailMatcher {
  private static final Logger logger = LoggerFactory.getLogger(TaildirIncludeChildMatcher.class);

  // flag from configuration to switch off caching completely
  private final boolean cachePatternMatching;
  // id from configuration
  private final String fileGroup;
  // plain string of the desired files from configuration
  private final String filePattern;

  // directory monitored for changes
  private Set<File> parentDirList = Sets.newLinkedHashSet();

  // Key is file path
  // Value is a two tuple, contains the lastSeenParentDirMTime and lastCheckedTime of the file
  private Map<String, LastTimeTuple> lastTimeMap = Maps.newHashMap();

  // cached content, files which matched the pattern within the parent directory
  private Set<File> lastMatchedFiles = Sets.newHashSet();

  // Array version cache of lastMatchedFilesSet, use this cache when the files has not been changed
  private List<File> lastMatchedFilesCache = new ArrayList<>();

  // file regex
  private final String regex;

  TaildirIncludeChildMatcher(String fileGroup, String filePattern, boolean cachePatternMatching) {
    // store whatever came from configuration
    this.fileGroup = fileGroup;
    this.filePattern = filePattern;
    this.cachePatternMatching = cachePatternMatching;

    // Path to the root directory to be monitored
    // The end of the path in the configuration file can be filled
    // with the file regular that needs to be matched
    // Note that if "/" is not written at the end of the path,
    // the end field will be treated as a regular
    String filePatternParent;

    if (filePattern.charAt(filePattern.length() - 1) == '/') {
      filePatternParent = filePattern;
      regex = "";
    } else {
      String[] res = filePattern.split("\\/");
      List<String> list = new ArrayList<>(Arrays.asList(res));
      regex = list.remove(list.size() - 1);
      filePatternParent = StringUtils.join(list, "/");
    }

    File f = new File(filePatternParent);

    // Scan from the top directory
    // Scan out all subdirectories and put them into cache
    getFileGroupChild(f, this.parentDirList);

    Preconditions.checkState(f.exists(),
            "Directory does not exist: " + f.getAbsolutePath());
  }

  /**
   * Lists those files within the parentDir
   * and subdirectory that match regex pattern passed in during object
   * instantiation. Designed for frequent periodic invocation
   * {@link org.apache.flume.source.PollableSourceRunner}.
   * <p></p>
   * Based on the modification of the parentDirList(parentDir and its subdirectories)
   * this function may trigger cache recalculation by
   * calling {@linkplain #updateMatchingFilesNoCache(File, List)} or
   * return the value stored in {@linkplain #lastMatchedFilesCache}.
   * Parentdir is allowed to be a symbolic link.
   * <p></p>
   * Files returned by this call are weakly consistent (see {@link DirectoryStream}).
   * It does not freeze the directory while iterating,
   * so it may (or may not) reflect updates to the directory that occur during the call,
   * In which case next call
   * will return those files (as mtime is increasing it won't hit cache but trigger recalculation).
   * It is guaranteed that invocation reflects every change which was observable at the time of
   * invocation.
   * <p></p>
   * Matching file list recalculation is triggered when caching was turned off or
   * if mtime is greater than the previously seen mtime
   * (including the case of cache hasn't been calculated before).
   * Additionally if a constantly updated directory was configured
   * as parentDir and its subdirectories
   * then multiple changes to the parentDirList may happen
   * within the same second so in such case (assuming at least second granularity of reported mtime)
   * it is impossible to tell whether a change of the dir happened before the check or after
   * (unless the check happened after that second).
   * Having said that implementation also stores system time of the previous invocation and previous
   * invocation has to happen strictly after the current mtime to avoid further cache refresh
   * (because then it is guaranteed that previous invocation resulted in valid cache content).
   * If system clock hasn't passed the second of
   * the current mtime then logic expects more changes as well
   * (since it cannot be sure that there won't be any further changes still in that second
   * and it would like to avoid data loss in first place)
   * hence it recalculates matching files. If system clock finally
   * passed actual mtime then a subsequent invocation guarantees that it picked up every
   * change from the passed second so
   * any further invocations can be served from cache associated with that second
   * (given mtime is not updated again).
   * <p></p>
   * Only the changed directories and new subdirectories will be rescanned each time
   *
   * @return List of files matching the pattern sorted by last modification time. No recursion.
   * No directories. If nothing matches then returns an empty list. If I/O issue occurred then
   * returns the list collected to the point when exception was thrown.
   */
  @Override
  public List<File> getMatchingFiles() {
    boolean lastMatchedFilesHasChange = false;

    long now = TimeUnit.SECONDS.toMillis(
            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));

    List<File> nonExistDirList = new ArrayList<>();

    List<File> newDirList = new ArrayList<>();

    // Traverse all monitored directories
    // if any directory changes are found, recalculate the file list of the directory
    for (File dir : this.parentDirList) {
      long currentParentDirMTime = dir.lastModified();

      if (currentParentDirMTime == 0) {
        this.lastTimeMap.remove(dir.getPath());
        nonExistDirList.add(dir);
        continue;
      }

      LastTimeTuple lastTimeTuple = lastTimeMap.get(dir.getPath());
      if (lastTimeTuple == null) {
        lastTimeTuple = new LastTimeTuple();
        lastTimeMap.put(dir.getPath(), lastTimeTuple);
      }

      Long lastSeenParentDirMTime = lastTimeTuple.getLastSeenParentDirMTime();
      Long lastCheckedTime = lastTimeTuple.getLastCheckedTime();

      // calculate matched files if
      // - we don't want to use cache (recalculate every time) OR
      // - directory was clearly updated after the last check OR
      // - last mtime change wasn't already checked for sure
      //   (system clock hasn't passed that second yet)
      if (!cachePatternMatching ||
              lastSeenParentDirMTime < currentParentDirMTime ||
              lastCheckedTime < currentParentDirMTime) {
        lastMatchedFilesHasChange = true;

        updateMatchingFilesNoCache(dir, newDirList);
        lastTimeTuple.setLastSeenParentDirMTime(currentParentDirMTime);
        lastTimeTuple.setLastCheckedTime(now);
      }
    }

    if (!nonExistDirList.isEmpty()) {
      this.parentDirList.removeAll(nonExistDirList);
    }

    if (!newDirList.isEmpty()) {
      this.parentDirList.addAll(newDirList);
    }

    if (lastMatchedFilesHasChange) {
      this.lastMatchedFilesCache = sortByLastModifiedTime(new ArrayList<>(this.lastMatchedFiles));
    }

    return this.lastMatchedFilesCache;
  }

  /**
   * Provides the actual files within the parentDir which
   * files are matching the regex pattern. Each invocation uses {@link Pattern}
   * to identify matching files.
   * <p>
   * Files returned by this call are weakly consistent
   * (new files will be set {@linkplain #lastMatchedFilesCache}).
   * It does not freeze the directory while iterating, so it may (or may not) reflect updates
   * to the directory that occur during the call. In which case next call will return those files.
   * <p>
   * New dir will be stored in the parameters newDirList
   * New file will be stored in the {@linkplain #lastMatchedFiles}
   *
   * @param dir        Directory to be scanned
   * @param newDirList Used to store the new directory
   */
  private void updateMatchingFilesNoCache(File dir, List<File> newDirList) {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath())) {
      if (stream != null) {
        for (Path child : stream) {
          if (Files.isDirectory(child)) {
            File newDir = child.toFile();
            if (!this.parentDirList.contains(newDir)) {
              newDirList.add(newDir);
              updateMatchingFilesNoCache(newDir, newDirList);
            }
          } else {
            if (child.toString().matches(regex) || regex == "") {
              this.lastMatchedFiles.add(child.toFile());
            }
          }
        }
      }
    } catch (IOException e) {
      logger.error("I/O exception occurred while listing parent directory. " +
              "Files already matched will be returned. " + dir.toPath(), e);
    }
  }

  /**
   * Scan all subdirectories of the specified directory and cache
   *
   * @param fileGroup     Directory to be scanned
   * @param fileGroupList Store the parent directory and its subdirectories
   * @return void
   */
  private static void getFileGroupChild(File fileGroup, Set<File> fileGroupList) {
    fileGroupList.add(fileGroup);

    File[] listFiles = fileGroup.listFiles();
    if (listFiles != null) {
      for (File child : listFiles) {
        if (Files.isDirectory(child.toPath())) {
          getFileGroupChild(child, fileGroupList);
        }
      }
    }
  }

  /**
   * Utility function to sort matched files based on last modification time.
   * Sorting itself use only a snapshot of last modification times captured before the sorting
   * to keep the number of stat system calls to the required minimum.
   *
   * @param files list of files in any order
   * @return sorted list
   */
  private static List<File> sortByLastModifiedTime(List<File> files) {
    final HashMap<File, Long> lastModificationTimes = new HashMap<File, Long>(files.size());
    for (File f : files) {
      lastModificationTimes.put(f, f.lastModified());
    }
    Collections.sort(files, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return lastModificationTimes.get(o1).compareTo(lastModificationTimes.get(o2));
      }
    });

    return files;
  }

  @Override
  public String toString() {
    return "{" +
            "filegroup='" + fileGroup + '\'' +
            ", filePattern='" + filePattern + '\'' +
            ", cached=" + cachePatternMatching +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaildirIncludeChildMatcher that = (TaildirIncludeChildMatcher) o;

    return fileGroup.equals(that.fileGroup);
  }

  @Override
  public int hashCode() {
    return fileGroup.hashCode();
  }

  @Override
  public String getFileGroup() {
    return fileGroup;
  }

  @Override
  public void deleteFileCache(File file) {
    this.lastMatchedFiles.remove(file);
  }

  private static class LastTimeTuple {
    // system time in milliseconds, stores the last modification time of the
    // parent directory seen by the last check, rounded to seconds
    // initial value is used in first check only when it will be replaced instantly
    // (system time is positive)
    private long lastSeenParentDirMTime = -1;

    // system time in milliseconds, time of the last check, rounded to seconds
    // initial value is used in first check only when it will be replaced instantly
    // (system time is positive)
    private long lastCheckedTime = -1;

    public long getLastCheckedTime() {
      return lastCheckedTime;
    }

    public long getLastSeenParentDirMTime() {
      return lastSeenParentDirMTime;
    }

    public void setLastCheckedTime(long lastCheckedTime) {
      this.lastCheckedTime = lastCheckedTime;
    }

    public void setLastSeenParentDirMTime(long lastSeenParentDirMTime) {
      this.lastSeenParentDirMTime = lastSeenParentDirMTime;
    }
  }
}
