package com.diorsding.spark.twitter;

import java.io.Serializable;
import java.util.Date;

// TODO: Use Avro later
public class Tweet implements Serializable {

    private static final long serialVersionUID = 1l;

    private Long id;
    private String user;
    private String screenName;
    private String profileImageUrl;
    private String text;
    private Double latitude;
    private Double longitude;
    private int score;
    private Date date;

    public Tweet(String user, String text, Date date) {
        this.user = user;
        this.text = text;
        this.date = date;
    }

    public Tweet(Long id, String user, String screenName, String profileImageUrl, String text, Double latitude,
                 Double longitude, int score, Date date) {
        this.id = id;
        this.user = user;
        this.screenName = screenName;
        this.profileImageUrl = profileImageUrl;
        this.text = text;
        this.latitude = latitude;
        this.longitude = longitude;
        this.score = score;
        this.date = date;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public String getProfileImageUrl() {
        return profileImageUrl;
    }

    public void setProfileImageUrl(String profileImageUrl) {
        this.profileImageUrl = profileImageUrl;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }


    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }
}
