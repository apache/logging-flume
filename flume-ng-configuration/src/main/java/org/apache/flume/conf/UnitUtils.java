package org.apache.flume.conf;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * <p>
 * UnitUtils is a tool class that converts a string with multiple units into a minimum unit value 
 * </p>
 * 
 * @author Lisheng Xia
 *
 */
public class UnitUtils {
  /**
   * the
   */
  private static final Pattern CAPACITY_PATTERN=Pattern.compile("((?<g>\\d+(\\.\\d+)?)(g|G))?((?<m>\\d+(\\.\\d+)?)(m|M))?((?<k>\\d+(\\.\\d+)?)(k|K))?((?<b>\\d+)(b|B|byte|BYTE)?)?");
  private static final Pattern TIME_PATTERN=Pattern.compile("((?<h>\\d+(\\.\\d+)?)(h|H))?((?<m>\\d+(\\.\\d+)?)(m|M))?((?<s>\\d+(\\.\\d+)?)(s|S))?((?<ms>\\d+)(ms|MS)?)?");
  
  private static final int CAPACITY_RATE=1024;
  
  /**
   * <p>
   * capacity units are g/G,m/M,k/K,b/B/byte/BYTE
   * </p>
   * <ul><li>2g or 2G -> 2147483648
   * <li>1.5g400m or 1.5G400M -> 2030043136
   * <li>200m1024b or 200M1024 ->  209716224(If there is no specified capacity unit, the default is byte)</ul>
   * 
   * @param value A string to be converted into bytes
   * @return result of the conversion
   */
  public static Long parseBytes(String value){
    Objects.requireNonNull(value);
    Matcher matcher = CAPACITY_PATTERN.matcher(value);
    if(matcher.matches()){
      long bytes=0;
      String gib=matcher.group("g");
      String mib=matcher.group("m");
      String kib=matcher.group("k");
      String b=matcher.group("b");
      if(gib!=null){
        bytes+=Math.round(Double.parseDouble(gib)*Math.pow(CAPACITY_RATE, 3));
      }
      if(mib!=null){
        bytes+=Math.round(Double.parseDouble(mib)*Math.pow(CAPACITY_RATE, 2));
      }
      if(kib!=null){
        bytes+=Math.round(Double.parseDouble(kib)*Math.pow(CAPACITY_RATE, 1));
      }
      if(b!=null){
        bytes+=Integer.parseInt(b);
      }
      return bytes;
    }else{
      throw new IllegalArgumentException("Invalid capacity specified.");
    }
  }
  
  /**
   * <p>
   * time units are h/H,m/M,s/S,ms/MS
   * </p>
   * <ul><li>1h or 1H -> 3600000 
   * <li>1.5h or 1.5H or 1h30m -> 5400000 
   * <li>3s or 3S -> 3000 </ul>
   * @param value A string to be converted into milliseconds
   * @return result of the conversion
   */
  public static Long parseMillisecond(String value){
    Objects.requireNonNull(value);
    Matcher matcher = TIME_PATTERN.matcher(value);
    if(matcher.matches()){
      long millisecond=0;
      String hour=matcher.group("h");
      String minute=matcher.group("m");
      String second=matcher.group("s");
      String ms=matcher.group("ms");
      if(hour!=null){
        millisecond+=Math.round(Double.parseDouble(hour)*60*60*1000);
      }
      if(minute!=null){
        millisecond+=Math.round(Double.parseDouble(minute)*60*1000);
      }
      if(second!=null){
        millisecond+=Math.round(Double.parseDouble(second)*1000);
      }
      if(ms!=null){
        millisecond+=Integer.parseInt(ms);
      }
      return millisecond;
    }else{
      throw new IllegalArgumentException("Invalid time specified.");
    }
  }
}
