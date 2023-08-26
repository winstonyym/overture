# Import library
import duckdb
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import json
import argparse

# Load arguments
parser = argparse.ArgumentParser(description="Main module that runs segmentation workflow on folder")

# Add positional arguments
parser.add_argument('country', help='Specify country name', type=str)
# parser.add_argument('secret', default=1.0, help='Image segmentation threshold value', type=float)

args = parser.parse_args()

def main(): 
    # Connect database and load extensions
    db = duckdb.connect()
    db.execute("INSTALL spatial")
    db.execute("INSTALL httpfs")
    db.execute("""
        LOAD spatial;
        LOAD httpfs;
        SET s3_region='us-west-2';
        """)

    # Read Boundary File
    with open('./polygons.json') as f:
        tiles = json.load(f)

    if args.country == "Singapore":
        country_gdf = gpd.read_file(tiles[f'{args.country}.geojson'])
    else:
        country_gdf = gpd.read_file(tiles[f'{args.country}_tile.geojson'])

    minx, miny, maxx, maxy = country_gdf.total_bounds

    # Get building geojson
    db.execute(f"""
    copy(select 
            id,
            JSON(names) as names,
            level,
            height,
            numfloors,
            class,
            JSON(bbox) as bbox,
            ST_GeomFromWkb(geometry) AS geometry
            from read_parquet('building_data/*')
            where
            bbox.minx > {minx-0.01} 
            AND bbox.maxx < {maxx+0.01}
            AND bbox.miny > {miny-0.01} 
            AND bbox.maxy < {maxy+0.01})
    to '{args.country}_buildings.geojson'
    with (FORMAT GDAL, DRIVER 'GeoJSON');
    """)

    selected_buildings = gpd.read_file(f'{args.country}_buildings.geojson')

    # Obtain intersection between bbox points and actual country boundary
    res_intersect = country_gdf.overlay(selected_buildings)
    
    res_intersect.to_file(f'{args.country}_buildings.geojson')


if __name__ == "__main__":
    main()
