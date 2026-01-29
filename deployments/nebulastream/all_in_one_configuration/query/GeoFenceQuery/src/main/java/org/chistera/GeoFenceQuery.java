package org.chistera;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import stream.nebula.operators.sinks.MQTTSink;
import stream.nebula.runtime.NebulaStreamRuntime;
import stream.nebula.runtime.Query;
import stream.nebula.udf.MapFunction;

import java.awt.geom.Path2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static stream.nebula.expression.Expressions.attribute;

public class GeoFenceQuery {

    static class Name2IdInput {
        long timestamp;
        String robot_name;
        double position_x;
        double position_y;
    }

    static class Name2IdOutput {
        long timestamp;
        long robot_id;
        double position_x;
        double position_y;
    }

    static class Name2Id implements MapFunction<Name2IdInput, Name2IdOutput> {
        @Override
        public Name2IdOutput map(final Name2IdInput input) {
            Name2IdOutput output = new Name2IdOutput();
            output.timestamp = input.timestamp;
            switch (input.robot_name) {
                case "leader":
                    output.robot_id = 0L;
                    break;
                case "follower":
                    output.robot_id = 1L;
                    break;
                case "leader_inv":
                    output.robot_id = 2L;
                    break;
                default:
                    output.robot_id = 10L;
                    break;
            }
            output.position_x = input.position_x;
            output.position_y = input.position_y;
            return output;
        }
    }

    static class Id2NameInput {
        long timestamp;
        long robot_id;
        double position_x;
        double position_y;
        boolean exited;
    }

    static class Id2NameOutput {
        long timestamp;
        String robot_name;
        double position_x;
        double position_y;
        boolean exited;
    }

    static class Id2Name implements MapFunction<Id2NameInput, Id2NameOutput> {
        @Override
        public Id2NameOutput map(final Id2NameInput input) {
            Id2NameOutput output = new Id2NameOutput();
            output.timestamp = input.timestamp;
            if (input.robot_id == 0L) {
                output.robot_name = "leader";
            } else if (input.robot_id == 1L) {
                output.robot_name = "follower";
            } else if (input.robot_id == 2L) {
                output.robot_name = "leader_inv";
            } else {
                output.robot_name = "default";
            }
            output.position_x = input.position_x;
            output.position_y = input.position_y;
            output.exited = input.exited;
            return output;
        }
    }

    static class GeoFenceInput {
        long timestamp;
        long robot_id;
        double position_x;
        double position_y;
    }

    static class GeoFenceOutput {
        long timestamp;
        long robot_id;
        double position_x;
        double position_y;
        boolean exited;
    }

    static class GeoFence implements MapFunction<GeoFenceInput, GeoFenceOutput> {
        private final Path2D.Double field;

        public GeoFence(Path2D.Double field) {
            this.field = field;
        }

        @Override
        public GeoFenceOutput map(final GeoFenceInput input) {
            GeoFenceOutput output = new GeoFenceOutput();
            output.timestamp = input.timestamp;
            output.robot_id = input.robot_id;
            output.position_x = input.position_x;
            output.position_y = input.position_y;
            output.exited = !this.field.contains(input.position_x, input.position_y);
            return output;
        }
    }

    private static byte[] hexStringToByteArray(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    public static Path2D.Double loadFieldShape(String filePath) throws IOException, ParseException {
        Path2D.Double path = new Path2D.Double();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String wkbHexString = br.readLine();
            if (wkbHexString == null || wkbHexString.trim().isEmpty()) {
                throw new IOException("WKB file is empty: " + filePath);
            }

            byte[] wkbBytes = hexStringToByteArray(wkbHexString.trim());
            WKBReader wkbReader = new WKBReader();
            Geometry geometry = wkbReader.read(wkbBytes);

            if (!(geometry instanceof Polygon)) {
                throw new ParseException("WKB data did not decode to a Polygon");
            }

            Polygon polygon = (Polygon) geometry;
            Coordinate[] coordinates = polygon.getExteriorRing().getCoordinates();

            if (coordinates.length == 0) {
                throw new IOException("Polygon has no coordinates.");
            }

            path.moveTo(coordinates[0].x, coordinates[0].y);
            for (int i = 1; i < coordinates.length; i++) {
                path.lineTo(coordinates[i].x, coordinates[i].y);
            }
            path.closePath();
        }
        return path;
    }

    public static void main(String[] args) {
        try {
            String filePath = args[0];

            String nesIp = System.getenv("NES_COORDINATOR_IP");
            String nesPortStr = System.getenv("NES_COORDINATOR_REST_PORT");
            String mqttIp = System.getenv("QUERY_HOST_IP");
            String mqttPortStr = System.getenv("MQTT_PORT");
            String geoFenceQuery = System.getenv("GEOFENCE_QUERY");

            int nesPort = Integer.parseInt(nesPortStr);
            String mqttUrl = "ws://" + mqttIp + ":" + mqttPortStr;

            Path2D.Double fieldShape = loadFieldShape(filePath);

            NebulaStreamRuntime nebulaStreamRuntime = NebulaStreamRuntime.getRuntime(nesIp, nesPort);
            Query query = nebulaStreamRuntime.readFromSource("gps_position");
            query.map(new Name2Id());
            query.map(new GeoFence(fieldShape));
            query.filter(attribute("exited"));
            query.map(new Id2Name());
            query.sink(new MQTTSink(mqttUrl, geoFenceQuery, "user", 1000, MQTTSink.TimeUnits.milliseconds, 0, MQTTSink.ServiceQualities.atLeastOnce, true));

            int queryId = nebulaStreamRuntime.executeQuery(query, "BottomUp");
            System.out.println("Query started with ID: " + queryId);

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }
}
