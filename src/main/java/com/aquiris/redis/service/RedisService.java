package com.aquiris.redis.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;


@Service
public class RedisService {

    private Map<String, String> data = new TreeMap<>();
    private Map<String, Map<String, String>> z = new TreeMap<>();

    /**
     * Add/update a key/value pair.
     * @param key key
     * @param val value
     */
    public synchronized void put(String key, String val) {
        data.put(key, val);
    }

    /**
     * Add/update a key/value pair that will exists until expires.
     * @param key key
     * @param val value
     * @param expire expire in seconds.
     */
    @Async("fileExecutor")
    public void put(String key, String val, String expire) {
        data.put(key, val);
        try {
            Thread.sleep( Long.parseLong(expire + "000"));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        data.remove(key);
    }

    /**
     * Get the value of a given key.
     * @param key key
     * @return the value of the key.
     */
    public synchronized String get(String key) {
        String ret;
        if (data.containsKey(key)) {
            ret = data.get(key);
        } else{
            ret = "(nil)";
        }
        return ret;
    }

    /**
     * Remove a key/value pair of a given key.
     * @param key key
     * @return "OK" if successful.
     */
    public synchronized String delete(String key) {
        String ret;
        if (data.containsKey(key)) {
            data.remove(key);
            ret = "OK";
        } else{
            ret = "(nil)";
        }
        return ret;
    }

    /**
     * Returns the number of elements.
     * @return number of keys.
     */
    public synchronized int size() {
        return data.size();
    }

    /**
     * Increments the value of a given key by one, if the value is a number.
     * If the key does not exist, it will be created with value 0.
     * @param key key
     * @return the value of the key after incrementation.
     */
    public synchronized String incr(String key) {
        String ret;

        if (data.containsKey(key)) {
            Integer val = 0;
            try {
                val = Integer.valueOf(data.get(key));
                ++val;
                data.put(key, String.valueOf(val));
                ret = "(integer) " + val;
            } catch (NumberFormatException nfe) {
                ret = nfe.getMessage();
            }
        } else {
            data.put(key, "0");
            ret = "(integer) 0";
        }
        return ret;
    }

    /**
     * Add/update a pair key/value where value is another pair key/value.
     * @param key key
     * @param val a string containing a key/value pair separated by "=".
     */
    public synchronized void zAdd(String key, String val) {

        Map<String, String> zValue;
        if (z.containsKey(key)) {
            zValue = z.get(key);
        } else {
            zValue = new TreeMap<>();
        }

        String[] pair = val.split("=");
        zValue.put(pair[0], pair[1]);
        z.put(key, zValue);
    }

    /**
     * Return number of members of a given key
     * @param key key
     * @return number of keys of the card.
     */
    public synchronized int zSize(String key) {

        int ret = 0;
        if (z.containsKey(key)) {
            ret = z.get(key).size();
        }
        return ret;
    }

    /**
     * Return index of a specific value from a given key.
     * @param key key
     * @return index of the value.
     */
    public synchronized int zRank(String key, String value) {

        int ret = -1;
        if (z.containsKey(key)) {
            List<String> list = new ArrayList<>(z.get(key).values());
            ret = list.indexOf(value);
        }
        return ret;
    }

    /**
     * Return values of a given key from start to stop.
     * @param key key
     * @param start start of the range
     * @param stop end of the range. If stop is negative, eg. -1 means lastIndex -1.
     * @return the list of values between start and stop.
     */
    public synchronized String zRange(String key, int start, int stop) {

        StringBuilder ret = new StringBuilder();
        int seq = 0;
        if (z.containsKey(key)) {
            List<String> list = new ArrayList<>(z.get(key).values());
            int end = stop;
            if (stop < 0) {
                end = list.size() + stop;
            }
            if (stop > list.size()) {
                end = list.size() -1;
            }
            for (int i=start; i <= end; i++) {
                ret.append(++seq).append(") \"").append(list.get(i)).append("\"").append("\n");
            }
            ret.delete(ret.length() - 1, ret.length());
        }
        return ret.toString();
    }

    /**
     * Not required. Just for dev purposes.
     * @return
     */
    public synchronized String getAll() {
        StringBuilder ret = new StringBuilder();
        ret.append("{");
        Set<String> keys = data.keySet();
        if (keys.size() > 0) {
            for (String key : keys) {
                ret.append("\"").append(key).append("\"").append(":").append("\"").append(data.get(key)).append("\"");
                ret.append(",");
        }
            ret.delete(ret.length() - 1, ret.length());
        }
        ret.append("}");

        return ret.toString();
    }

    /**
     * Not required. Just for dev purposes.
     * @return
     */
    public synchronized String getZAll() {

        StringBuilder ret = new StringBuilder();
        ret.append("{");
        Set<String> keys = z.keySet();
        if (keys.size() > 0) {
            for (String key : keys) {
                ret.append("\"").append(key).append("\"").append(":");
                Set<String> innerKeys = z.get(key).keySet();
                if (innerKeys.size() > 0) {
                    ret.append("[{");
                    for (String innerkey : innerKeys) {
                        ret.append("\"").append(innerkey).append("\"").append(":").append("\"").append(z.get(key).get(innerkey)).append("\"");
                        ret.append(",");
                    }
                    ret.delete(ret.length() - 1, ret.length());
                    ret.append("}]");
                }
                ret.append(",");
            }
            ret.delete(ret.length() - 1, ret.length());
        }
        ret.append("}");

        return ret.toString();
    }
}
