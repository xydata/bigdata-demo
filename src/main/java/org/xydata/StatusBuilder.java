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
        status.setContributors(getContributors(statusT4j));
        status.setCreatedAt(statusT4j.getCreatedAt().getTime());
        status.setCurrentUserRetweetId(statusT4j.getCurrentUserRetweetId());
        status.setExtendedMediaEntities(getExtendedMediaEntities(statusT4j));
        status.setFavoriteCount(statusT4j.getFavoriteCount());
        status.setGeoLocation(getGeoLocation(statusT4j));
        status.setHashtagEntities(getHashtagEntities(statusT4j));
        status.setId(statusT4j.getId());
        status.setInReplyToScreenName(statusT4j.getInReplyToScreenName());
        status.setInReplyToStatusId(statusT4j.getInReplyToStatusId());
        status.setInReplyToUserId(statusT4j.getInReplyToUserId());
        status.setLang(statusT4j.getLang());
        status.setMediaEntities(getMediaEntites(statusT4j));
        status.setPlace(getPlace(statusT4j));
        status.setPossiblySensitive(statusT4j.isPossiblySensitive());

        RateLimitStatus rateLimitStatus = new RateLimitStatus();
        twitter4j.RateLimitStatus rls = statusT4j.getRateLimitStatus();
        rateLimitStatus.setLimit(rls.getLimit());
        rateLimitStatus.setRemaining(rls.getRemaining());
        rateLimitStatus.setResetTimeInSeconds(rls.getResetTimeInSeconds());
        rateLimitStatus.setSecondsUntilReset(rls.getResetTimeInSeconds());


        status.setRateLimitStatus(rateLimitStatus);
        status.setRetweet(statusT4j.isRetweet());
        status.setRetweetCount(statusT4j.getRetweetCount());
        status.setRetweeted(statusT4j.isRetweeted());
        status.setRetweetedByMe(statusT4j.isRetweetedByMe());
        status.setRetweetedStatus(build(statusT4j.getRetweetedStatus()));

        Scopes scopes = new Scopes();
        twitter4j.Scopes s = statusT4j.getScopes();
        scopes.setPlaceIds(Arrays.asList(s.getPlaceIds()));
        status.setScopes(scopes);
        status.setSource(statusT4j.getSource());

        List<SymbolEntity> symbolEntities = new ArrayList<>();
        for(twitter4j.SymbolEntity se:statusT4j.getSymbolEntities()){
            SymbolEntity symbolEntity = new SymbolEntity();
            symbolEntity.setEnd(se.getEnd());
            symbolEntity.setStart(se.getStart());
            symbolEntity.setText(se.getText());
            symbolEntities.add(symbolEntity);
        }
        status.setSymbolEntities(symbolEntities);
        status.setText(statusT4j.getText());
        status.setTruncated(statusT4j.isTruncated());
        //status.setUrlentities(statusT4j.getURLEntities());
        //status.setUser(statusT4j.getUser());
        //status.setUserMentionEntities(statusT4j.getUserMentionEntities());
        //status.setWithheldInCountries(statusT4j.getWithheldInCountries());

        return status;

    }

    private static List<MediaEntity> getMediaEntites(twitter4j.Status statusT4j) {
        List<MediaEntity> mediaEntitys = new ArrayList<>();

        for(twitter4j.MediaEntity emeT4j :statusT4j.getMediaEntities()) {
            MediaEntity eme = new MediaEntity();
            eme.setDisplayURL(emeT4j.getDisplayURL());
            eme.setEnd(emeT4j.getEnd());
            eme.setExpandedURL(emeT4j.getExpandedURL());
            eme.setId(emeT4j.getId());
            eme.setMediaURL(emeT4j.getMediaURL());
            eme.setMediaURLHttps(emeT4j.getMediaURLHttps());


            Map<String, Size> sizes = new HashMap<>();
            for (Map.Entry<Integer, twitter4j.MediaEntity.Size> entry : emeT4j.getSizes().entrySet()) {
                Size s = new Size();
                s.setHeight(entry.getValue().getHeight());
                s.setResize(entry.getValue().getResize());
                s.setWidth(entry.getValue().getWidth());
                sizes.put(Integer.toString(entry.getKey()), s);
            }
            eme.setSizes(sizes);
            eme.setStart(emeT4j.getStart());
            eme.setText(emeT4j.getText());
            eme.setType(emeT4j.getType());
            eme.setUrl(emeT4j.getURL());

            mediaEntitys.add(eme);

        }

        return mediaEntitys;
    }

    private static Place getPlace(twitter4j.Status statusT4j) {
        Place place = new Place();
        twitter4j.Place p = statusT4j.getPlace();
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

    private static List<HashtagEntity> getHashtagEntities(twitter4j.Status statusT4j) {
        List<HashtagEntity> hashtagEntities = new ArrayList<>();
        for(twitter4j.HashtagEntity he:statusT4j.getHashtagEntities()){
            HashtagEntity hashtagEntity = new HashtagEntity();
            hashtagEntity.setEnd(he.getEnd());
            hashtagEntity.setStart(he.getStart());
            hashtagEntity.setText(he.getText());
            hashtagEntities.add(hashtagEntity);
        }

        return hashtagEntities;
    }

    private static GeoLocation getGeoLocation(twitter4j.Status statusT4j) {
        GeoLocation geoLocation = new GeoLocation();
        twitter4j.GeoLocation g  = statusT4j.getGeoLocation();
        geoLocation.setLatitude(g.getLatitude());
        geoLocation.setLongitude(g.getLongitude());
        return geoLocation;
    }

    private static List<ExtendedMediaEntity> getExtendedMediaEntities(twitter4j.Status statusT4j) {
        List<ExtendedMediaEntity> extendedMediaEntitys = new ArrayList<>();

        for(twitter4j.ExtendedMediaEntity emeT4j :statusT4j.getExtendedMediaEntities()){
            ExtendedMediaEntity eme = new ExtendedMediaEntity();
            eme.setDisplayURL(emeT4j.getDisplayURL());
            eme.setEnd(emeT4j.getEnd());
            eme.setExpandedURL(emeT4j.getExpandedURL());
            eme.setId(emeT4j.getId());
            eme.setMediaURL(emeT4j.getMediaURL());
            eme.setMediaURLHttps(emeT4j.getMediaURLHttps());



            Map<String,Size> sizes = new HashMap<>();
            for(Map.Entry<Integer,twitter4j.MediaEntity.Size> entry:emeT4j.getSizes().entrySet()){
                Size s = new Size();
                s.setHeight(entry.getValue().getHeight());
                s.setResize(entry.getValue().getResize());
                s.setWidth(entry.getValue().getWidth());
                sizes.put(Integer.toString(entry.getKey()),s);
            }
            eme.setSizes(sizes);
            eme.setStart(emeT4j.getStart());
            eme.setText(emeT4j.getText());
            eme.setType(emeT4j.getType());
            eme.setUrl(emeT4j.getURL());
            eme.setVideoAspectRatioHeight(emeT4j.getVideoAspectRatioHeight());
            eme.setVideoAspectRatioWidth(emeT4j.getVideoAspectRatioWidth());
            eme.setVideoDurationMillis(emeT4j.getVideoDurationMillis());

            List<Variant> variants = new ArrayList<>();

            for(twitter4j.ExtendedMediaEntity.Variant v : emeT4j.getVideoVariants()){
                Variant variant = new Variant();
                variant.setBitrate(v.getBitrate());
                variant.setContentType(v.getContentType());
                variant.setUrl(v.getUrl());
                variants.add(variant);
            }


            eme.setVideoVariants(variants);
            extendedMediaEntitys.add(eme);
        }
        return extendedMediaEntitys;
    }

    private static List<Double> getContributors(twitter4j.Status statusT4j) {
        List<Double> contributors = new ArrayList<>();
        for(Long d :statusT4j.getContributors()){
            contributors.add(d.doubleValue());
        }
        return contributors;
    }


}
