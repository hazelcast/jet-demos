package com.hazelcast.jet.demo.util;

/**
 * Helper methods for JSON parsing and geographic calculations.
 */
public class Util {

    public static float IST_LAT = 40.982555f;
    public static float IST_LON = 28.820829f;

    public static float LHR_LAT = 51.470020f;
    public static float LHR_LON = -0.454295f;

    public static float FRA_LAT = 50.110924f;
    public static float FRA_LON = 8.682127f;

    public static float ATL_LAT = 33.640411f;
    public static float ATL_LON = -84.419853f;

    public static float PAR_LAT = 49.0096906f;
    public static float PAR_LON = 2.54792450f;

    public static float TOK_LAT = 35.765786f;
    public static float TOK_LON = 140.386347f;

    public static float JFK_LAT = 40.6441666667f;
    public static float JFK_LON = -73.7822222222f;


    public static boolean inIstanbul(double lon, double lat) {
        return inBoundariesOf(lon, lat, boundingBox(IST_LON, IST_LAT, 80f));
    }

    public static boolean inLondon(double lon, double lat) {
        return inBoundariesOf(lon, lat, boundingBox(LHR_LON, LHR_LAT, 80f));
    }

    public static boolean inFrankfurt(double lon, double lat) {
        return inBoundariesOf(lon, lat, boundingBox(FRA_LON, FRA_LAT, 80f));
    }

    public static boolean inAtlanta(double lon, double lat) {
        return inBoundariesOf(lon, lat, boundingBox(ATL_LON, ATL_LAT, 80f));
    }

    public static boolean inParis(double lon, double lat) {
        return inBoundariesOf(lon, lat, boundingBox(PAR_LON, PAR_LAT, 80f));
    }

    public static boolean inTokyo(double lon, double lat) {
        return inBoundariesOf(lon, lat, boundingBox(TOK_LON, TOK_LAT, 80f));
    }

    public static boolean inNYC(double lon, double lat) {
        return inBoundariesOf(lon, lat, boundingBox(JFK_LON, JFK_LAT, 80f));
    }

    public static double[] boundingBox(double lon, double lat, float radius) {
        double boundingLon1 = lon + radius / Math.abs(Math.cos(Math.toRadians(lat)) * 69);
        double boundingLon2 = lon - radius / Math.abs(Math.cos(Math.toRadians(lat)) * 69);
        double boundingLat1 = lat + (radius / 69);
        double boundingLat2 = lat - (radius / 69);
        return new double[]{boundingLon1, boundingLat1, boundingLon2, boundingLat2};
    }

    public static boolean inBoundariesOf(double lon, double lat, double[] boundaries) {
        return !(lon > boundaries[0] || lon < boundaries[2]) &&
                !(lat > boundaries[1] || lat < boundaries[3]);
    }
}
