"""
файл, который помогает опредлять стандартные разрезы rzr1 и rzr2
"""

import pandas as pd
from shapely.geometry import Point, LineString
from database import DatabaseUploader
import configparser

class Rzr:
    def define_rzr1(self, points):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.db = DatabaseUploader.connect(self,
                                           config['postgresql']['database'],
                                           config['postgresql']['user'],
                                           config['postgresql']['password'],
                                           config['postgresql']['host'])
        self.conn = self.db[1]
        self.cursor = self.db[0]

        df = pd.read_csv('rzr.csv', encoding='ansi', sep=';')

        rzr1 = []
        rzr2 = []
        lat = []
        lon = []
        point = []

        for i, row in df.iterrows():
            rzr1.append(row['rzr1'])
            rzr2.append(row['rzr2'])
            lat.append(row['shir'])
            lon.append(row['dolg'])

        for i in zip(lat, lon):
            point.append(i)

        a = Rzr.convert_coords(point)

        lines = {'shir': a[0], 'dolg': a[1], 'rzr1': rzr1}

        df_lines = pd.DataFrame(lines)

        polygons = df_lines.groupby('rzr1').apply(
            lambda x: [[shir, dolg] for shir, dolg in zip(x['shir'], x['dolg'])]).to_dict()

        df = Rzr.assign_line(self, points, polygons)

        return df

    def define_rzr2(self, points):
        # словарь с координатами точек
        coords = {
            '09C': [[71.1667, 10.0]],
            '036': [[70.25, 41.3333]],
            '014': [[69.6167, 49.25], [69.1, 50.4667]],
            '042': [[69.1333, 53.6333]],
            '043': [[74.0, 50.0]],
            '046': [[75.45, 17.8]],
            '01A': [[67.5, 10.5]],
            '08C': [[69.3333, 8.0], [69.3333, -11.0]],
            'КРЧ': [[62.5, -7.0]],
            '028': [[76.75, 25.7]],
            '037': [[68.75, 43.25]],
            '029': [[74.5, 33.5]],
            '11C': [[74.5, 18.5]],
            '048': [[75.6667, 21.0]],
            '050': [[76.8333, 22.0]],
            '45W': [[45.3333, -45.0]],
            '07C': [[67.50, 11.50]],
            '051': [[77.25, 23.3833], [76.1667, 23.5]],
            '052': [[75.9167, 13.75]],
            '054': [[76.5, 12.5]],
            '055': [[77.5833, 13.5]],
            '056': [[77.4167, 11.0]],
            '059': [[78.5, 10.8333]],
            '061': [[78.9167, 8.5]],
            '062': [[79.5, 10.3333]],
            '38A': [[56.0, -56.0833]],
            '45A': [[75.1333, 16.3], [74.5, 12.5833]],
            'BON': [[48.7333, -52.9667], [50.0, -49.0]],
        }

        results = []
        for point in points:
            for name, coords_list in coords.items():
                if any([point == tuple(coord) for coord in coords_list]):
                    results.append((point[0], point[1], name))
                # else:
                #     results.append((point[0], point[1], None))

        df = pd.DataFrame(results, columns=['shir', 'dolg', 'rzr2'])
        if df.empty:
            for point in points:
                results.append((point[0], point[1], None))
                df = pd.DataFrame(results, columns=['shir', 'dolg', 'rzr2'])

        return df


    @staticmethod
    def convert_coords(point):
        x = []
        y = []
        for lat, lon in point:
            if lat >= 0:
                lat_deg = int(lat/100)
                lat_min = round((lat/100)-lat_deg, 4)
                lat_min = lat_min*100
                lat_min = round(lat_min/60, 4)
                lat_deg = lat_deg + lat_min
                x.append(lat_deg)
            elif lat <= 0:
                lat_deg = int(lat / 100)
                lat_min = round((lat/100) - lat_deg, 4)
                lat_min = -lat_min * 100
                lat_min = round(lat_min/60, 4)
                lat_deg = lat_deg - lat_min
                x.append(lat_deg)

            if lon <= 0:
                lon_deg = int(lon / 100)
                lon_min = round((lon / 100) - lon_deg, 4)
                lon_min = -lon_min * 100
                lon_min = round(lon_min / 60, 4)
                lon_deg = lon_deg - lon_min
                y.append(lon_deg)
            elif lon >= 0:
                lon_deg = int(lon / 100)
                lon_min = round((lon / 100) - lon_deg, 4)
                lon_min = lon_min * 100
                lon_min = round(lon_min / 60, 4)
                lon_deg = lon_deg + lon_min
                y.append(lon_deg)

        return x, y

    def find_line(self, point, lines):
        for line_name, line_coords in lines.items():
            line = LineString(line_coords)
            if line.contains(Point(point)):
                return line_name

        return None

    def assign_line(self, points, lines):
        result = []
        for point in points:
            line_name = Rzr.find_line(self, point, lines)
            if line_name is not None and (point[0], point[1]) in tuple(lines):
                result.append((point[0], point[1], line_name))
            else:
                min_distance = float('inf')
                nearest_line = None
                nearest_point = None
                for line_name, line_coords in lines.items():
                    line = LineString(line_coords)
                    distance = line.distance(Point(point))
                    if distance < min_distance:
                        min_distance = distance
                        nearest_line = line_name
                        nearest_point = line_coords[0]
                        for i in range(1, len(line_coords)):
                            segment = LineString([line_coords[i - 1], line_coords[i]])
                            distance = segment.distance(Point(point))
                            if distance < min_distance:
                                min_distance = distance
                                nearest_line = line_name
                                nearest_point = segment.interpolate(segment.project(Point(point)))
                result.append((point[0], point[1], nearest_line))
        df = pd.DataFrame(result, columns=['shir', 'dolg', 'rzr1'])
        return df












