import rclpy
from rclpy.node import Node
from visualization_msgs.msg import Marker
from geometry_msgs.msg import Point
from builtin_interfaces.msg import Duration
from sensor_msgs.msg import NavSatFix
import pandas as pd
from shapely import wkb, geometry, affinity
from shapely.ops import unary_union
import binascii
from pyproj import Transformer
import os
from math import sqrt
from functools import partial  
import json                      
from math import sqrt, cos, sin, pi

class CsvToRvizMarkers(Node):
    def __init__(self):
        super().__init__('csv_to_rviz_markers')
        

        self.publisher = self.create_publisher(Marker, 'visualization_marker', 10)
        
        self.csv_path = os.path.expanduser('/app/data/parcelles.csv')
        self.sensor_config_path = os.path.expanduser('/app/data/sensor_config.json')
        self.transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)
        self.global_centroid = None

        self.declare_parameter('robot_list', ['leader', 'follower'])
        self.robot_list = self.get_parameter('robot_list').get_parameter_value().string_array_value

        self.declare_parameter('centroid_shape_name', '1 MONT')
        self.centroid_shape_name = self.get_parameter('centroid_shape_name').get_parameter_value().string_value

    
        self.robot_paths = {}
        self.subscriptions_list = [] 

        colors = [
            (0.0, 0.0, 1.0), # Blue
            (0.0, 1.0, 0.0), # Green
            (1.0, 0.0, 0.0), # Red
            (1.0, 1.0, 0.0), # Yellow
            (0.0, 1.0, 1.0), # Cyan
            (1.0, 0.0, 1.0)  # Magenta
        ]

        self.get_logger().info(f"Initialization for robots: {self.robot_list}")

        for i, robot_name in enumerate(self.robot_list):
            path_marker = Marker()
            path_marker.header.frame_id = 'map'
            path_marker.ns = 'robot_paths'  
            path_marker.type = Marker.LINE_STRIP
            path_marker.action = Marker.ADD
            path_marker.id = 10000 + i      
            path_marker.scale.x = 1.0       
            
            r, g, b = colors[i % len(colors)]
            path_marker.color.r = r
            path_marker.color.g = g
            path_marker.color.b = b
            path_marker.color.a = 1.0

            path_marker.points = []
            
            self.robot_paths[robot_name] = path_marker

            gps_topic = f'/{robot_name}/gps/fix'
            
            callback = partial(self.gps_callback, robot_name=robot_name)
            
            sub = self.create_subscription(NavSatFix, gps_topic, callback, 10)
            self.subscriptions_list.append(sub)
            
            self.get_logger().info(f"Listening to: {gps_topic} (Color: R={r} G={g} B={b})")

        self.timer = self.create_timer(2.0, self.publish_map_markers)


    def publish_map_markers(self):

        if not os.path.exists(self.csv_path):
            self.get_logger().warn(f"CSV file missing: {self.csv_path}")
            return

        df = pd.read_csv(self.csv_path)

        if self.global_centroid is None:
            metric_geometries = []
            centroid_geometries = []

            for i, row in df.iterrows():
                try:
                    geom = wkb.loads(binascii.unhexlify(row['geom']))
                    
                    polygons_list = []
                    if geom.geom_type == 'Polygon':
                        polygons_list.append(geom)
                    elif geom.geom_type == 'MultiPolygon':
                        polygons_list.extend(geom.geoms)
                    
                    for poly in polygons_list:
                        exterior = [self.transformer.transform(y, x) for x, y in poly.exterior.coords]
                        proj_poly = geometry.Polygon(exterior)
                        metric_geometries.append(proj_poly)
                        
                        if row['name'] == self.centroid_shape_name:
                            centroid_geometries.append(proj_poly)
                            
                except Exception as e:
                    self.get_logger().warn(f'Error in geometry {row.get("id", "?")}: {e}')

            if not metric_geometries:
                self.get_logger().warn("No valid geometries to process.")
                return

            if not centroid_geometries:
                self.get_logger().warn(f"Could not find '{self.centroid_shape_name}' — using all.")
                combined = unary_union(metric_geometries)
            else:
                combined = unary_union(centroid_geometries)

            self.global_centroid = combined.centroid
            self.get_logger().info(f"Centroid established: {self.global_centroid.x}, {self.global_centroid.y}")


        for i, row in df.iterrows():
            try:
                geom = wkb.loads(binascii.unhexlify(row['geom']))
                
                metric_polys = []
                source_polys = []
                
                if geom.geom_type == 'Polygon':
                    source_polys.append(geom)
                elif geom.geom_type == 'MultiPolygon':
                    source_polys.extend(geom.geoms)
                else:
                    continue 

                for poly in source_polys:
                    exterior = [self.transformer.transform(y, x) for x, y in poly.exterior.coords]
                    metric_polys.append(geometry.Polygon(exterior))

                if len(metric_polys) == 1:
                    metric_geom = metric_polys[0]
                else:
                    metric_geom = geometry.MultiPolygon(metric_polys)

                shifted = affinity.translate(metric_geom, xoff=-self.global_centroid.x, yoff=-self.global_centroid.y)

                marker = Marker()
                marker.header.frame_id = 'map'
                marker.ns = 'parcels' 
                marker.type = Marker.LINE_STRIP
                marker.action = Marker.ADD
                marker.id = int(row['id'])
                marker.scale.x = 0.5
                marker.color.r = 1.0
                marker.color.g = 0.0
                marker.color.b = 0.0
                marker.color.a = 1.0
                marker.lifetime = Duration(sec=0, nanosec=0)

                polys_to_draw = shifted.geoms if shifted.geom_type == 'MultiPolygon' else [shifted]
                
                for p in polys_to_draw:
                    for x, y in p.exterior.coords:
                        marker.points.append(Point(x=x, y=y, z=0.0))
                
                self.publisher.publish(marker)

            except Exception as e:
                self.get_logger().warn(f'Error drawing parcel {row.get("id", "?")}: {e}')
        self.publish_sensor_markers()

    def publish_sensor_markers(self):
        if not os.path.exists(self.sensor_config_path):
            self.get_logger().warn(f"WAITING FOR FILE: {self.sensor_config_path} does not exist!") 
            return

        if self.global_centroid is None:
            return

        try:
            with open(self.sensor_config_path, 'r') as f:
                sensors = json.load(f)


            self.get_logger().info(f"Drawing {len(sensors)} sensors...") 

            for s in sensors:
                sensor_id = s.get('sensor_id')
                lat = s.get('lat')
                lon = s.get('lon')
                radius = s.get('radius', 30.0)

                mx, my = self.transformer.transform(lat, lon)

                cx = mx - self.global_centroid.x
                cy = my - self.global_centroid.y

                marker = Marker()
                marker.header.frame_id = 'map'
                marker.ns = 'sensors'
                marker.type = Marker.LINE_STRIP
                marker.action = Marker.ADD
                marker.id = int(sensor_id)
                marker.scale.x = 0.5  

                marker.color.r = 1.0
                marker.color.g = 0.65
                marker.color.b = 0.0
                marker.color.a = 1.0
                marker.lifetime = Duration(sec=0, nanosec=0)

                num_segments = 64
                for i in range(num_segments + 1):
                    angle = (i / num_segments) * 2 * pi
                    px = cx + radius * cos(angle)
                    py = cy + radius * sin(angle)
                    marker.points.append(Point(x=px, y=py, z=0.0))

                self.publisher.publish(marker)

        except Exception as e:
            self.get_logger().warn(f"Error drawing sensors: {e}")


    def gps_callback(self, msg: NavSatFix, robot_name: str):

        if self.global_centroid is None:
            return

        mx, my = self.transformer.transform(msg.latitude, msg.longitude)
        dx = mx - self.global_centroid.x
        dy = my - self.global_centroid.y
        new_point = Point(x=dx, y=dy, z=0.0)

        marker = self.robot_paths[robot_name]

        if marker.points:
            last_point = marker.points[-1]
            dist = sqrt((new_point.x - last_point.x)**2 + (new_point.y - last_point.y)**2)

            if dist > 5.0:
                self.get_logger().info(f"[{robot_name}] Jump {dist:.2f}m — resetting path.")
                marker.points = []

        marker.points.append(new_point)
        marker.header.stamp = msg.header.stamp
        
        self.publisher.publish(marker)

        # self.get_logger().info(f'[{robot_name}] Update: x={dx:.2f}, y={dy:.2f}')


def main(args=None):
    rclpy.init(args=args)
    node = CsvToRvizMarkers()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info("Interrupted by user (Ctrl+C)")
    except rclpy.executors.ExternalShutdownException:
        node.get_logger().info("ROS2 context was closed")
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()