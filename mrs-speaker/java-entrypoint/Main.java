package com.sbchild.mrs_speaker_android;

import android.content.Context;
import java.lang.reflect.Method;

public class Main {
  public static void main(String[] args) {
    System.out.println("Got " + args.length + " params.");
    // for (int i = 0; i < args.length; i++) {
    //   System.out.println("arg[" + i + "]: " + args[i]);
    // }
    String jsonConfig = args.length > 0 ? args[0] : "{}";
    try {
      Class<?> activityThreadClass =
          Class.forName("android.app.ActivityThread");
      Method systemMainMethod = activityThreadClass.getMethod("systemMain");
      Object thread = systemMainMethod.invoke(null);
      Method getSystemContextMethod =
          activityThreadClass.getMethod("getSystemContext");
      Context context = (Context)getSystemContextMethod.invoke(thread);
      // target/aarch64-linux-android/release/libmrs_speaker.so
      System.loadLibrary("mrs_speaker");
      launchMrsSpeakerAndroid(context, jsonConfig);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  private static native void launchMrsSpeakerAndroid(Object context,
                                                     String jsonConfig);
}
