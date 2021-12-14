package com.atguigu.hotitems_analysis;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.Timestamp;

public class DateLongFormatTypeAdapter extends TypeAdapter<Timestamp> {

    @Override
    public void write(JsonWriter out, Timestamp value) throws IOException {
        if(value != null) out.value(value.getTime());
        else out.nullValue();
    }

    @Override
    public Timestamp read(JsonReader in) throws IOException {
        return new Timestamp(in.nextLong());
    }

}