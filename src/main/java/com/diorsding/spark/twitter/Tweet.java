package com.diorsding.spark.twitter;

import java.io.Serializable;
import java.util.Date;

public class Tweet implements Serializable {

    private static final long serialVersionUID = 1l;

    private String user;
    private String text;
    private Date date;

    public Tweet() {

    }

    public Tweet(String user, String text, Date date) {
        this.user = user;
        this.text = text;
        this.date = date;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "[User]: " + user + " [Text] : " + text + " [Date]: " + date;
    }

}
