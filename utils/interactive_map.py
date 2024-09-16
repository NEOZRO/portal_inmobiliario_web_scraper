import os
import json
import numpy as np
from ipyleaflet import Map, DrawControl, TileLayer, GeoJSON
from tqdm.auto import tqdm
from database import DatabaseManager
import ast

class InteractiveMap(DatabaseManager):
    """ class for handling maps with ipyleaflet """
    def __init__(self):
        super().__init__()
        self.selected_map_id = None
        self.actions_list = None
        self.folder_save_name = None
        self.full_path = None
        self.json_polygon_path = None
        self.main_interactive_map = None
        self.theme = None
        self.center_map_coordinates = None


    def save_visualization_map_polygon_selection(self,path_save):
        """
        deprecated, need to be fixed
        save map selection as html visualization
        :param path_save: path, file must be html
        """

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


    def handle_draw(self, self_2, action, geo_json):
        """internal thread of map picker to run funtions once the selection is completed"""
        self.actions_list.append(action)

        if action == 'created':
            self.picked_pts_features.append(geo_json)
            self.insert_new_cluser_map(self.folder_save_name, str(self.picked_pts_features))

        elif action == 'deleted':
            cleaned_geojson = [d for d in self.picked_pts_features if d.get('geometry') != geo_json.get('geometry')] # noqa
            self.picked_pts_features = cleaned_geojson
            self.insert_new_cluser_map(self.folder_save_name, str(self.picked_pts_features))


    def get_geofences_center_coords(self):
        """ get the mean coords for all geo fences in the current cluster map"""

        if len(self.picked_pts_features) > 0:
            total_coords = []
            for data in self.picked_pts_features:
                coords = np.asarray(data["geometry"]["coordinates"][0])
                total_coords.append((np.mean(coords[:, 1]),np.mean(coords[:, 0])))

            return tuple(np.mean(total_coords,axis=0))
        else:
            return (-33.4489, -70.6693)  # Stgo,Chile


    def load_geojson_data(self):
        """
        load geojson data from database
        :param selected_mapID: selected group id
        """
        df = self.get_maps_data(self.selected_map_id)
        if len(df) > 0:
            geojson_data = df.query(f"mapID == {self.selected_map_id}")["geojson_data"].values[0]
            json_data_selected = ast.literal_eval(geojson_data)
            self.folder_save_name = df.query(f"mapID == {self.selected_map_id}")["geo_ref_name"].values[0]
            self.picked_pts_features = json_data_selected