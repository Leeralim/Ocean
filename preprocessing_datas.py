from shapely.geometry import Point, Polygon, LineString
"""
Методы данного класса определяют районы ИКЕС, экономические зоны и локальные районы
На вход подаются точки (координаты широты и долготы) и полигоны (выборка из БД существующих районов - тоже по коордам шир. и долг.)
"""


class PreprocDatasRegions:
# ----------------------------------------------------------------------------------------------------   

    def assign_polygon_id(self, points, polygons):
        self.result = {}
        self.polygon_objs = {poly_id: Polygon(coords) for poly_id, coords in polygons.items()}
        for point in points:
            self.found_match = False
            self.polygon_ids = list(self.polygon_objs.keys())
            i = 0
            while not self.found_match and i < len(self.polygon_ids):
                self.polygon_id = self.polygon_ids[i]
                self.polygon = self.polygon_objs[self.polygon_id]
                if self.polygon.contains(Point(point)):
                    if self.polygon_id not in self.result:
                        self.result[self.polygon_id] = []
                    self.result[self.polygon_id].append(point)
                    self.found_match = True
                else:
                    self.boundary = self.polygon.boundary
                    if self.boundary.type == 'LineString' or self.boundary.type == 'MultiLineString':
                        self.dist = Point(point).distance(self.boundary)
                        if self.dist < 1e-4:
                            if self.polygon_id not in self.result:
                                self.result[self.polygon_id] = []
                            self.result[self.polygon_id].append(point)
                            self.found_match = True
                        else:
                            for x, y in zip(*self.boundary.xy):
                                if Point(x, y).equals(Point(point)):
                                    if self.polygon_id not in self.result:
                                        self.result[self.polygon_id] = []
                                    self.result[self.polygon_id].append(point)
                                    self.found_match = True
                                    break
                    elif self.boundary.type == 'Point':
                        if Point(self.boundary).equals(Point(point)):
                            if self.polygon_id not in self.result:
                                self.result[self.polygon_id] = []
                            self.result[self.polygon_id].append(point)
                            self.found_match = True
                i += 1
            if not self.found_match:
                self.result[None] = self.result.get(None, []) + [point]
        return self.result  

# ----------------------------------------------------------------------------------------------------   

    def assign_regloc_id(self, points, local_areas):
        self.points_by_area = {}
        for point in points:
            self.min_distance = float('inf')
            self.assigned_area = None
            for name, area in local_areas.items():
                for i in range(len(area[0])):
                    if (point[0] >= area[0][i] and point[0] <= area[1][i] and
                        point[1] >= area[2][i] and point[1] <= area[3][i]):
                                self.distance = ((point[0] - area[4][0]) ** 2 + (point[1] - area[4][1]) ** 2) ** 0.5
                                if self.distance < self.min_distance:
                                    self.min_distance = self.distance
                                    self.assigned_area = name
            if self.assigned_area is not None:
                if self.assigned_area in self.points_by_area:
                    self.points_by_area[self.assigned_area].append(tuple(point))
                else:
                    self.points_by_area[self.assigned_area] = [tuple(point)]
            else:
                if self.assigned_area in self.points_by_area:
                    self.points_by_area[self.assigned_area].append(tuple(point))
                else:
                    self.points_by_area[self.assigned_area] = [tuple(point)]

        return self.points_by_area       
# ----------------------------------------------------------------------------------------------------   


