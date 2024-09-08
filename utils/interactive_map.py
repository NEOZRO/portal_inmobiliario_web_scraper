import os
import json
import numpy as np
from ipyleaflet import Map, DrawControl, TileLayer, GeoJSON
from tqdm.auto import tqdm

class InteractiveMap:
    """ class for handling maps with ipyleaflet """
    def __init__(self):
        self.picked_pts_features = None
        self.handle_draw = None
        self.folder_save_name = None
        self.full_path = None
        self.json_polygon_path = None
        self.main_interactive_map = None
        self.theme = None
        self.center_map_coordinates = None


    def save_visualization_map_polygon_selection(self):
        """ save map selection as html visualization """
        path_save = os.path.join(self.full_path, f'{self.folder_save_name}.html')
        path_load = self.json_polygon_path
        with open(path_load, 'r') as json_file:
            data = json.load(json_file)

        geo_json = GeoJSON(data=data)
        polygon_coordinates_array = np.asarray(data["geometry"]["coordinates"][0])

        map_to_save = self.init_map_ipyflet(theme=self.theme,
                                            center_map_cordinates=(np.mean(polygon_coordinates_array[:, 1]),
                                                                   np.mean(polygon_coordinates_array[:, 0])),
                                            zoom=14)

        map_to_save.add(geo_json)
        map_to_save.save(path_save)

    def save_json_polygon_selection(self):
        """ save map selection as json """
        path_save = os.path.join(self.full_path, f'{self.folder_save_name}.json')
        with open(path_save, 'w') as json_file:
            json.dump(self.picked_pts_features[0], json_file)

    def load_json_polygon_selection(self):
        """ load map selection as json """
        path_load = self.json_polygon_path
        with open(path_load, 'r') as json_file:
            self.picked_pts_features = [json.load(json_file)]


    def init_map_ipyflet(self, center_map_cordinates, theme="default", zoom=13, geojson_data=None):
        """ init Map from ipyfleet based con configurations"""

        dark_map_layer = TileLayer(
            url="https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}.png	",
            attribution='&copy; <a href="https://carto.com/">Carto</a>',
            name='Neon'
        )

        white_map_layer = TileLayer(
            url="https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png",
            attribution='&copy; <a href="https://carto.com/">Carto</a>',
            name='white'
        )
        map = None

        if theme == "dark":

            map = Map(layers=[dark_map_layer, ], center=center_map_cordinates, zoom=zoom)

        elif theme == "default":

            map = Map(center=center_map_cordinates, zoom=zoom)

        elif theme == "white":
            map = Map(layers=[white_map_layer, ], center=center_map_cordinates, zoom=zoom)


        if geojson_data:
            for feature in geojson_data:
                geojson_layer = GeoJSON(data=feature)
                map.add(geojson_layer)

        return map

    def map_picker(self):
        """
            allows the user to pick from global map a location to search in
            more themes: https://wiki.openstreetmap.org/wiki/Raster_tile_providers
            """
        self.main_interactive_map = self.init_map_ipyflet(theme=self.theme,
                                                          center_map_cordinates=self.center_map_coordinates)

        draw_control = DrawControl()

        draw_control.on_draw(self.handle_draw)

        self.main_interactive_map.add(draw_control)
        display(self.main_interactive_map)  # NOQA

        # self.bar.se = tqdm(total=100, desc="SELECT REGION TO ANALYSE. THEN RE-RUN PROGRAM")