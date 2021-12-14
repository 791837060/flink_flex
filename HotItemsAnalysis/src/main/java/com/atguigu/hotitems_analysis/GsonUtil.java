package com.atguigu.hotitems_analysis;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class GsonUtil {
    private static Gson gson;

    static {
//        gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
        gson = new GsonBuilder()
                .registerTypeAdapter(Date.class, new DateLongFormatTypeAdapter())
                .create();
    }

    public static String GsonToString(Object object) {
        return gson.toJson(object);
    }
    public static byte[] GsonToJSONBytes(Object object) {
        return gson.toJson(object).getBytes();
    }

    public static <T> T GsonToBean(String gsonString, Class<T> tClass) {
        return gson.fromJson(gsonString, tClass);
    }

    public static <T> List<T> GsonToList(String gsonString) {
        return gson.fromJson(gsonString, new TypeToken<List<T>>(){}.getType());
    }

    public static <T> List<T> GsonToList(String gsonString, Type typeToken ) {
        return gson.fromJson(gsonString,typeToken);
    }

    public static <T> List<Map<String, T>> GsonToListMaps(String gsonString) {
        return gson.fromJson(gsonString, new TypeToken<List<Map<String, T>>>(){}.getType());
    }

    public static <T> Map<String, T> GsonToMaps(String gsonString) {
        return gson.fromJson(gsonString, new TypeToken<Map<String, T>>(){}.getType());
    }

    public static Timestamp getTimestamp(Long l){
        return new Timestamp(l);
    }
}
