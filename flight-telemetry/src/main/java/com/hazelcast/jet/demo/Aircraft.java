package com.hazelcast.jet.demo;

import com.hazelcast.jet.demo.types.EngineMount;
import com.hazelcast.jet.demo.types.EngineTypes;
import com.hazelcast.jet.demo.types.Species;
import com.hazelcast.jet.demo.types.SpeedType;
import com.hazelcast.jet.demo.types.VerticalSpeedType;
import com.hazelcast.jet.demo.types.WakeTurbulanceCategory;
import com.hazelcast.jet.impl.util.Util;

import java.io.Serializable;
import java.util.List;

/**
 * DTO represents an aircraft.
 */
public class Aircraft implements Serializable {

    /**
     * The unique identifier of the aircraft
     */
    private long id;
    /**
     * The number of seconds that the aircraft has been tracked for
     */
    private long tsecs;
    /**
     * The ID of the feed that last supplied information about the aircraft
     */
    private long rcvr;
    /**
     * The ICAO of the aircraft
     */
    private String icao;
    /**
     * The registration of the aircraft
     */
    private String reg;
    /**
     * The altitude in feet at standard pressure
     */
    private long alt;
    /**
     * The altitude adjusted for local air pressure, should be roughly
     * the height above mean sea level
     */
    private long galt;
    /**
     * The target altitude, in feet, set on the autopilot / FMS etc
     */
    private long talt;
    /**
     * The callsign of the aircraft
     */
    private String call;
    /**
     * The aircraft's latitude over the ground
     */
    private double lat;
    /**
     * The aircraft's longitude over the ground.
     */
    private double lon;
    /**
     * The time (at UTC) that the position was last reported by the aircraft
     */
    private long posTime;
    /**
     * True if the last position update is older than the display timeout value
     */
    private boolean posStale;
    /**
     * The ground speed in knots
     */
    private double spd;
    /**
     * The type of speed that Spd represents.
     */
    private SpeedType spdType;
    /**
     * Vertical speed in feet per minute
     */
    private long vsi;
    /**
     * The type of vertical speed that vsi represents.
     */
    private VerticalSpeedType vsiType;
    /**
     * The aircraft model's ICAO type code.
     */
    private String type;
    /**
     * The aircraft manufacturer's name.
     */
    private String man;
    /**
     * A description of the aircraft's model. Usually also includes the manufacturer's name
     */
    private String mdl;
    /**
     * The code and name of the departure airport
     */
    private String from;
    /**
     * The code and name of the arrival airport
     */
    private String to;
    /**
     * An array of strings, each being a stopover on the route
     */
    private List<String> stops;
    /**
     * The name of the aircraft's operator
     */
    private String op;
    /**
     * The operator's ICAO code
     */
    private String opCode;
    /**
     * The wake turbulence category of the aircraft
     */
    private WakeTurbulanceCategory wtc;
    /**
     * The number of engines the aircraft has. Usually '1', '2' etc
     */
    private String engines;
    /**
     * The type of engine the aircraft uses
     */
    private EngineTypes engtype;
    /**
     * The placement of engines on the aircraft
     */
    private EngineMount engMount;
    /**
     * The species of the aircraft (helicopter, jet etc.)
     */
    private Species species;
    /**
     * True if the aircraft appears to be operated by the military
     */
    private boolean mil;
    /**
     * The country that the aircraft is registered to.
     */
    private String cou;
    /**
     * True if the aircraft is on the ground
     */
    private boolean gnd;
    /**
     * The year that the aircraft was manufactured
     */
    private String year;
    /**
     * Nearest airport to the aircraft at current location
     */
    private String airport;
    /**
     * Vertical direction of the aircraft.
     */
    private VerticalDirection verticalDirection = VerticalDirection.UNKNOWN;

    public long getId() {
        return id;
    }

    public long getTsecs() {
        return tsecs;
    }

    public long getRcvr() {
        return rcvr;
    }

    public String getIcao() {
        return icao;
    }

    public String getReg() {
        return reg;
    }

    public long getAlt() {
        return alt;
    }

    public long getGalt() {
        return galt;
    }

    public long getTalt() {
        return talt;
    }

    public String getCall() {
        return call;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public long getPosTime() {
        return posTime;
    }

    public boolean isPosStale() {
        return posStale;
    }

    public double getSpd() {
        return spd;
    }

    public SpeedType getSpdType() {
        return spdType;
    }

    public long getVsi() {
        return vsi;
    }

    public VerticalSpeedType getVsiType() {
        return vsiType;
    }

    public String getType() {
        return type;
    }

    public String getMdl() {
        return mdl;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public List<String> getStops() {
        return stops;
    }

    public String getOp() {
        return op;
    }

    public String getOpCode() {
        return opCode;
    }

    public String getMan() {
        return man;
    }

    public WakeTurbulanceCategory getWtc() {
        return wtc;
    }

    public String getEngines() {
        return engines;
    }

    public EngineTypes getEngtype() {
        return engtype;
    }

    public EngineMount getEngMount() {
        return engMount;
    }

    public Species getSpecies() {
        return species;
    }

    public boolean isMil() {
        return mil;
    }

    public String getCou() {
        return cou;
    }

    public boolean isGnd() {
        return gnd;
    }

    public String getYear() {
        return year;
    }

    public String getAirport() {
        return airport;
    }

    public void setAirport(String airport) {
        this.airport = airport;
    }

    public VerticalDirection getVerticalDirection() {
        return verticalDirection;
    }

    public void setVerticalDirection(VerticalDirection verticalDirection) {
        this.verticalDirection = verticalDirection;
    }

    @Override
    public String toString() {
        return "Aircraft{" +
                "reg='" + reg + '\'' +
                ", alt=" + alt +
                ", coord=" + String.format("%.2f,%.2f", lat, lon) +
                ", mdl='" + mdl + '\'' +
                ", city='" + airport + '\'' +
                ", posTime=" + Util.toLocalDateTime(posTime).toLocalTime() +
                '}';
    }

    public String fullString() {
        return "Aircraft{" +
                "id=" + id +
                ", tsecs=" + tsecs +
                ", rcvr=" + rcvr +
                ", icao='" + icao + '\'' +
                ", reg='" + reg + '\'' +
                ", alt=" + alt +
                ", galt=" + galt +
                ", talt=" + talt +
                ", call='" + call + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                ", city=" + airport +
                ", posTime=" + Util.toLocalDateTime(posTime) +
                ", posTimeTs=" + posTime +
                ", posStale=" + posStale +
                ", spd=" + spd +
                ", spdType=" + spdType +
                ", vsi=" + vsi +
                ", vsiType=" + vsiType +
                ", type='" + type + '\'' +
                ", mdl='" + mdl + '\'' +
                ", man='" + man + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", stops=" + stops +
                ", op='" + op + '\'' +
                ", opCode='" + opCode + '\'' +
                ", wtc=" + wtc +
                ", engines='" + engines + '\'' +
                ", engtype=" + engtype +
                ", engMount=" + engMount +
                ", species=" + species +
                ", mil=" + mil +
                ", cou='" + cou + '\'' +
                ", gnd=" + gnd +
                ", year='" + year + '\'' +
                '}';
    }

    enum VerticalDirection {
        UNKNOWN,
        CRUISE,
        ASCENDING,
        DESCENDING
    }

    public static class AircraftList {
        public List<Aircraft> acList;
    }
}
