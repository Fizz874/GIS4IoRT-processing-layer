package org.chistera;

import stream.nebula.operators.sinks.MQTTSink;
import stream.nebula.operators.window.TumblingWindow;
import stream.nebula.runtime.NebulaStreamRuntime;
import stream.nebula.runtime.Query;
import stream.nebula.udf.MapFunction;

import java.io.IOException;

import static stream.nebula.expression.Expressions.attribute;
import static stream.nebula.expression.Expressions.literal;
import static stream.nebula.operators.Aggregation.*;
import static stream.nebula.operators.window.Duration.seconds;
import static stream.nebula.operators.window.EventTime.eventTime;

public class HumidityQuery {

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
        long start;
        long end;
        long robot_count;
        long robot_id;
        double robot_position_x;
        double robot_position_y;
        long sensor_count;
        long sensor_id;
        double sensor_position_x;
        double sensor_position_y;
        double humidity;
        double distance;
    }

    static class Id2NameOutput {
        long start;
        long end;
        long robot_count;
        String robot_name;
        double robot_position_x;
        double robot_position_y;
        long sensor_count;
        long sensor_id;
        double sensor_position_x;
        double sensor_position_y;
        double humidity;
        double distance;
    }

    static class Id2Name implements MapFunction<Id2NameInput, Id2NameOutput> {
        @Override
        public Id2NameOutput map(final Id2NameInput input) {
            Id2NameOutput output = new Id2NameOutput();
            output.start = input.start;
            output.end = input.end;
            output.robot_count = input.robot_count;
            if (input.robot_id == 0L) {
                output.robot_name = "leader";
            } else if (input.robot_id == 1L) {
                output.robot_name = "follower";
            } else if (input.robot_id == 2L) {
                output.robot_name = "leader_inv";
            } else {
                output.robot_name = "default";
            }
            output.robot_position_x = input.robot_position_x;
            output.robot_position_y = input.robot_position_y;
            output.sensor_count = input.sensor_count;
            output.sensor_id = input.sensor_id;
            output.sensor_position_x = input.sensor_position_x;
            output.sensor_position_y = input.sensor_position_y;
            output.humidity = input.humidity;
            output.distance = input.distance;
            return output;
        }
    }

    static class JoinInput {
        long gps_position$start;
        long gps_position$end;
        long gps_position$count;
        long gps_position$robot_id;
        double gps_position$position_x;
        double gps_position$position_y;
        long soil_humidity$count;
        long soil_humidity$sensor_id;
        double soil_humidity$position_x;
        double soil_humidity$position_y;
        double soil_humidity$humidity;
    }

    static class JoinOutput {
        long start;
        long end;
        long robot_count;
        long robot_id;
        double robot_position_x;
        double robot_position_y;
        long sensor_count;
        long sensor_id;
        double sensor_position_x;
        double sensor_position_y;
        double humidity;
        double distance;
    }

    static class CalculateDistance implements MapFunction<JoinInput, JoinOutput> {

        private double calculateDistanceInMeters(double lat1, double lon1, double lat2, double lon2) {
            final int R = 6371000;
            double latDistance = Math.toRadians(lat2 - lat1);
            double lonDistance = Math.toRadians(lon2 - lon1);
            double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }

        @Override
        public JoinOutput map(final JoinInput input) {
            JoinOutput output = new JoinOutput();
            output.start = input.gps_position$start;
            output.end = input.gps_position$end;
            output.robot_count = input.gps_position$count;
            output.robot_id = input.gps_position$robot_id;
            output.robot_position_x = input.gps_position$position_x;
            output.robot_position_y = input.gps_position$position_y;
            output.sensor_count = input.soil_humidity$count;
            output.sensor_id = input.soil_humidity$sensor_id;
            output.sensor_position_x = input.soil_humidity$position_x;
            output.sensor_position_y = input.soil_humidity$position_y;
            output.humidity = input.soil_humidity$humidity;
            output.distance = calculateDistanceInMeters(input.gps_position$position_y, input.gps_position$position_x, input.soil_humidity$position_y, input.soil_humidity$position_x);
            return output;
        }
    }

    public static void main(String[] args) {
        try {
            String nesIp = System.getenv("NES_COORDINATOR_IP");
            String nesPortStr = System.getenv("NES_COORDINATOR_REST_PORT");
            String mqttIp = System.getenv("QUERY_HOST_IP");
            String mqttPortStr = System.getenv("MQTT_PORT");
            String humidityQuery = System.getenv("HUMIDITY_QUERY");
            String humThresholdStr = System.getenv("HUMIDITY_THRESHOLD");
            String bufferRadiusStr = System.getenv("BUFFER_RADIUS");

            int nesPort = Integer.parseInt(nesPortStr);
            double humThreshold = Double.parseDouble(humThresholdStr);
            double bufferRadius = Double.parseDouble(bufferRadiusStr);
            String mqttUrl = "ws://" + mqttIp + ":" + mqttPortStr;

            NebulaStreamRuntime nebulaStreamRuntime = NebulaStreamRuntime.getRuntime(nesIp, nesPort);
            Query q1 = nebulaStreamRuntime.readFromSource("gps_position");
            q1.map(new Name2Id());
            q1.window(TumblingWindow.of(eventTime("timestamp"), seconds(1))).byKey("robot_id").apply(count(), average("position_x"), average("position_y"));
            q1.map("joinKey", literal(1));
            Query q2 = nebulaStreamRuntime.readFromSource("soil_humidity");
            q2.window(TumblingWindow.of(eventTime("timestamp"), seconds(1))).byKey("sensor_id").apply(count(), average("position_x"), average("position_y"), max("humidity"));
            q2.map("joinKey", literal(1));
            Query finalQuery = q1.joinWith(q2).where(attribute("joinKey").equalTo(attribute("joinKey"))).window(TumblingWindow.of(eventTime("start"), seconds(1)));
            finalQuery.project(attribute("gps_position$start"), attribute("gps_position$end"), attribute("gps_position$count"), attribute("gps_position$robot_id"), attribute("gps_position$position_x"), attribute("gps_position$position_y"), attribute("soil_humidity$count"), attribute("soil_humidity$sensor_id"), attribute("soil_humidity$position_x"), attribute("soil_humidity$position_y"), attribute("soil_humidity$humidity"));
            finalQuery.map(new CalculateDistance());
            finalQuery.filter(attribute("distance").lessThanOrEqual(bufferRadius));
            finalQuery.filter(attribute("humidity").greaterThan(humThreshold));
            finalQuery.map(new Id2Name());
            finalQuery.sink(new MQTTSink(mqttUrl, humidityQuery, "user", 1000, MQTTSink.TimeUnits.milliseconds, 0, MQTTSink.ServiceQualities.atLeastOnce, true));

            int queryId = nebulaStreamRuntime.executeQuery(finalQuery, "BottomUp");
            System.out.println("Query started with ID: " + queryId);

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }
}
