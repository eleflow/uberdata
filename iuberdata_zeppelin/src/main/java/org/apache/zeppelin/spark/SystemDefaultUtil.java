package org.apache.zeppelin.spark;

/**
 * Created by dirceu on 10/02/16.
 */
public class SystemDefaultUtil {
    public static String getSystemDefault(
            String envName,
            String propertyName,
            String defaultValue) {

        if (envName != null && !envName.isEmpty()) {
            String envValue = System.getenv().get(envName);
            if (envValue != null) {
                return envValue;
            }
        }

        if (propertyName != null && !propertyName.isEmpty()) {
            String propValue = System.getProperty(propertyName);
            if (propValue != null) {
                return propValue;
            }
        }
        return defaultValue;
    }
}
