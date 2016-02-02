package org.xydata;

import org.xydata.avro.*;
import org.xydata.avro.ExtendedMediaEntity;
import org.xydata.avro.GeoLocation;
import org.xydata.avro.HashtagEntity;
import org.xydata.avro.MediaEntity;
import org.xydata.avro.Place;
import org.xydata.avro.RateLimitStatus;
import org.xydata.avro.Scopes;
import org.xydata.avro.Status;
import org.xydata.avro.SymbolEntity;

import java.util.*;

/**
 * Created by xavier on 16/2/1.
 *
 * Transformation of twitter4j.Status to org.xydata.avro.Status
 */
public class StatusBuilder {

    public static Status build(twitter4j.Status statusT4j){

        Status status = new Status();
        status.setAccessLevel(statusT4j.getAccessLevel());
        status.setContributors(getContributors(statusT4j.getContributors()));
        if(null!=statusT4j.getCreatedAt()){
            status.setCreatedAt(statusT4j.getCreatedAt().getTime());
        }
        status.setCurrentUserRetweetId(statusT4j.getCurrentUserRetweetId());
        status.setExtendedMediaEntities(getExtendedMediaEntities(statusT4j.getExtendedMediaEntities()));
        status.setFavoriteCount(statusT4j.getFavoriteCount());
        status.setGeoLocation(getGeoLocation(statusT4j.getGeoLocation()));
        status.setHashtagEntities(getHashtagEntities(statusT4j.getHashtagEntities()));
        status.setId(statusT4j.getId());
        status.setInReplyToScreenName(statusT4j.getInReplyToScreenName());
        status.setInReplyToStatusId(statusT4j.getInReplyToStatusId());
        status.setInReplyToUserId(statusT4j.getInReplyToUserId());
        status.setLang(statusT4j.getLang());
        status.setMediaEntities(getMediaEntites(statusT4j.getMediaEntities()));
        status.setPlace(getPlace(statusT4j.getPlace()));
        status.setPossiblySensitive(statusT4j.isPossiblySensitive());
        status.setRateLimitStatus(getRateLimitStatus(statusT4j.getRateLimitStatus()));
        status.setRetweet(statusT4j.isRetweet());
        status.setRetweetCount(statusT4j.getRetweetCount());
        status.setRetweeted(statusT4j.isRetweeted());
        status.setRetweetedByMe(statusT4j.isRetweetedByMe());
        if(null!=statusT4j.getRetweetedStatus()){
            status.setRetweetedStatus(build(statusT4j.getRetweetedStatus()));
        }


        twitter4j.Scopes s = statusT4j.getScopes();
        if(null!=s &&null!=s.getPlaceIds()){
            Scopes scopes = new Scopes();
            scopes.setPlaceIds(Arrays.asList(s.getPlaceIds()));
            status.setScopes(scopes);
        }
        status.setSource(statusT4j.getSource());
        status.setSymbolEntities(getSymbolEntities(statusT4j.getSymbolEntities()));
        status.setText(statusT4j.getText());
        status.setTruncated(statusT4j.isTruncated());
        //status.setUrlentities(statusT4j.getURLEntities());
        //status.setUser(statusT4j.getUser());
        //status.setUserMentionEntities(statusT4j.getUserMentionEntities());
        //status.setWithheldInCountries(statusT4j.getWithheldInCountries());

        return status;

    }

    private static List<SymbolEntity> getSymbolEntities(twitter4j.SymbolEntity[] symbolEntitiesT4j) {
        if(null==symbolEntitiesT4j){
            return null;
        }
        List<SymbolEntity> symbolEntities = new ArrayList<>();
        for(twitter4j.SymbolEntity se:symbolEntitiesT4j){
            SymbolEntity symbolEntity = new SymbolEntity();
            symbolEntity.setEnd(se.getEnd());
            symbolEntity.setStart(se.getStart());
            symbolEntity.setText(se.getText());
            symbolEntities.add(symbolEntity);
        }
        return symbolEntities;
    }

    private static RateLimitStatus getRateLimitStatus(twitter4j.RateLimitStatus rateLimitStatusT4j) {
        if (null == rateLimitStatusT4j) {
            return null;
        }
        RateLimitStatus rateLimitStatus = new RateLimitStatus();
        rateLimitStatus.setLimit(rateLimitStatusT4j.getLimit());
        rateLimitStatus.setRemaining(rateLimitStatusT4j.getRemaining());
        rateLimitStatus.setResetTimeInSeconds(rateLimitStatusT4j.getResetTimeInSeconds());
        rateLimitStatus.setSecondsUntilReset(rateLimitStatusT4j.getResetTimeInSeconds());
        return rateLimitStatus;
    }

    private static List<MediaEntity> getMediaEntites(twitter4j.MediaEntity[] mediaEntitiesT4j) {
        if(null==mediaEntitiesT4j){
            return null;
        }
        List<MediaEntity> mediaEntities = new ArrayList<>();
        for(twitter4j.MediaEntity emeT4j :mediaEntitiesT4j) {
            MediaEntity eme = new MediaEntity();
            eme.setDisplayURL(emeT4j.getDisplayURL());
            eme.setEnd(emeT4j.getEnd());
            eme.setExpandedURL(emeT4j.getExpandedURL());
            eme.setId(emeT4j.getId());
            eme.setMediaURL(emeT4j.getMediaURL());
            eme.setMediaURLHttps(emeT4j.getMediaURLHttps());
            eme.setSizes(getStringSizeMap(emeT4j.getSizes()));
            eme.setStart(emeT4j.getStart());
            eme.setText(emeT4j.getText());
            eme.setType(emeT4j.getType());
            eme.setUrl(emeT4j.getURL());
            mediaEntities.add(eme);
        }
        return mediaEntities;
    }

    private static Place getPlace(twitter4j.Place p) {
        if(null == p){
            return null;
        }
        Place place = new Place();
        place.setAccessLevel(p.getAccessLevel());
        //place.setBoundingBoxCoordinates(p.getBoundingBoxCoordinates());
        place.setBoundingBoxType(p.getBoundingBoxType());
        //place.setContainedWithIn(p.getContainedWithIn());
        place.setCountry(p.getCountry());
        place.setCountryCode(p.getCountryCode());
        place.setFullName(p.getFullName());
        //place.setGeometryCoordinates(p.getGeometryCoordinates());
        place.setGeometryType(p.getGeometryType());
        place.setId(p.getId());
        place.setName(p.getName());
        place.setPlaceType(p.getPlaceType());
        //place.setRateLimitStatus(p.getRateLimitStatus());
        place.setStreetAddress(p.getStreetAddress());
        place.setUrl(p.getURL());
        return place;
    }

    private static List<HashtagEntity> getHashtagEntities(twitter4j.HashtagEntity[] hashtagEntitiesT4j) {
        if(null==hashtagEntitiesT4j){
            return null;
        }
        List<HashtagEntity> hashtagEntities = new ArrayList<>();
        for(twitter4j.HashtagEntity he:hashtagEntitiesT4j){
            HashtagEntity hashtagEntity = new HashtagEntity();
            hashtagEntity.setEnd(he.getEnd());
            hashtagEntity.setStart(he.getStart());
            hashtagEntity.setText(he.getText());
            hashtagEntities.add(hashtagEntity);
        }

        return hashtagEntities;
    }

    private static GeoLocation getGeoLocation(twitter4j.GeoLocation geoLocationT4j) {
        if(null==geoLocationT4j){
            return null;
        }
        GeoLocation geoLocation = new GeoLocation();
        geoLocation.setLatitude(geoLocationT4j.getLatitude());
        geoLocation.setLongitude(geoLocationT4j.getLongitude());
        return geoLocation;
    }

    private static List<ExtendedMediaEntity> getExtendedMediaEntities(twitter4j.ExtendedMediaEntity[] extendedMediaEntitiesT4j) {
        if(null==extendedMediaEntitiesT4j){
            return null;
        }

        List<ExtendedMediaEntity> extendedMediaEntities = new ArrayList<>();
        for(twitter4j.ExtendedMediaEntity emeT4j :extendedMediaEntitiesT4j){
            ExtendedMediaEntity eme = new ExtendedMediaEntity();
            eme.setDisplayURL(emeT4j.getDisplayURL());
            eme.setEnd(emeT4j.getEnd());
            eme.setExpandedURL(emeT4j.getExpandedURL());
            eme.setId(emeT4j.getId());
            eme.setMediaURL(emeT4j.getMediaURL());
            eme.setMediaURLHttps(emeT4j.getMediaURLHttps());
            eme.setSizes(getStringSizeMap(emeT4j.getSizes()));
            eme.setStart(emeT4j.getStart());
            eme.setText(emeT4j.getText());
            eme.setType(emeT4j.getType());
            eme.setUrl(emeT4j.getURL());
            eme.setVideoAspectRatioHeight(emeT4j.getVideoAspectRatioHeight());
            eme.setVideoAspectRatioWidth(emeT4j.getVideoAspectRatioWidth());
            eme.setVideoDurationMillis(emeT4j.getVideoDurationMillis());
            eme.setVideoVariants(getVariants(emeT4j.getVideoVariants()));
            extendedMediaEntities.add(eme);
        }
        return extendedMediaEntities;
    }

    private static List<Variant> getVariants(twitter4j.ExtendedMediaEntity.Variant[] variantT4j) {
        if(null==variantT4j){
            return null;
        }
        List<Variant> variants = new ArrayList<>();
        for(twitter4j.ExtendedMediaEntity.Variant v : variantT4j){
            Variant variant = new Variant();
            variant.setBitrate(v.getBitrate());
            variant.setContentType(v.getContentType());
            variant.setUrl(v.getUrl());
            variants.add(variant);
        }
        return variants;
    }

    private static Map<String, Size> getStringSizeMap(Map<Integer, twitter4j.MediaEntity.Size> sizeMapT4j) {
        if(null==sizeMapT4j){
            return null;
        }
        Map<String,Size> sizes = new HashMap<>();
        for(Map.Entry<Integer,twitter4j.MediaEntity.Size> entry:sizeMapT4j.entrySet()){
            Size s = new Size();
            twitter4j.MediaEntity.Size sT4j = entry.getValue();
            if(null!=sT4j){
                s.setHeight(sT4j.getHeight());
                s.setResize(sT4j.getResize());
                s.setWidth(sT4j.getWidth());
            }
            sizes.put(Integer.toString(entry.getKey()),s);
        }
        return sizes;
    }

    private static List<Double> getContributors(long[] contributorsT4j) {
        if(null==contributorsT4j){
            return null;
        }
        List<Double> contributors = new ArrayList<>();
        for(Long d :contributorsT4j){
            contributors.add(d.doubleValue());
        }
        return contributors;
    }


}
