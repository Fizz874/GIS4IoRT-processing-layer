package com.gis;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;

@UdfDescription(name = "INGEOFENCE", description = "Checks if point is inside any of the provided polygons (separated by pipe)")
public class GeoUdf {

    private final GeometryFactory geometryFactory = new GeometryFactory();
    // Returns true if point is inside at least one polygon
    @Udf
    public Boolean INGEOFENCE(
        @UdfParameter(description = "lat") Double lat, 
        @UdfParameter(description = "lon") Double lon, 
        @UdfParameter(description = "hex_string") String hexString
    ) {
        if (lat == null || lon == null || hexString == null || hexString.isEmpty()) {
            return false;
        }

        try {
            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
            WKBReader reader = new WKBReader(geometryFactory);

            // Zone separation
            String[] hexes = hexString.split("\\|");

            for (String rawHex : hexes) {
                if (rawHex == null || rawHex.trim().isEmpty()) continue;
                
                try {
                    String cleanHex = rawHex.trim();

                    byte[] bytes = WKBReader.hexToBytes(cleanHex);
                    
                    Geometry polygon = reader.read(bytes);

                    if (polygon.contains(point)) {
                        return true; 
                    }
                } catch (Exception e) {
                    continue;
                }
            }

            return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}

    // Helpers

    //private byte[] hexToBytes(String s) {
    //    int len = s.length();
    //    byte[] data = new byte[len / 2];
    //    for (int i = 0; i < len; i += 2) {
    //        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
    //                             + Character.digit(s.charAt(i+1), 16));
    //    }
    //    return data;
    //}

    //private byte[] cleanEWKB(byte[] bytes) {
    //    if (bytes.length < 5) return bytes;
        
    //    boolean isLittleEndian = (bytes[0] == 1);
    //    int type = 0;
        
    //    if (isLittleEndian) {
    //        type = (bytes[1] & 0xFF) | ((bytes[2] & 0xFF) << 8) | ((bytes[3] & 0xFF) << 16) | ((bytes[4] & 0xFF) << 24);
    //    } else {
    //        type = (bytes[4] & 0xFF) | ((bytes[3] & 0xFF) << 8) | ((bytes[2] & 0xFF) << 16) | ((bytes[1] & 0xFF) << 24);
    //    }

        // If detected flag SRID (0x20000000)
    //    if ((type & 0x20000000) != 0) {
    //        byte[] newBytes = new byte[bytes.length - 4];
    //        newBytes[0] = bytes[0]; 

    //        if (isLittleEndian) {
    //            newBytes[1] = bytes[1]; newBytes[2] = bytes[2]; newBytes[3] = bytes[3];
    //            newBytes[4] = (byte)(bytes[4] & 0xDF);
    //        } else {
    //            newBytes[1] = (byte)(bytes[1] & 0xDF);
    //            newBytes[2] = bytes[2]; newBytes[3] = bytes[3]; newBytes[4] = bytes[4];
    //        }
            
    //        System.arraycopy(bytes, 9, newBytes, 5, bytes.length - 9);
    //        return newBytes;
    //    }
    //    return bytes;
    //}
//}