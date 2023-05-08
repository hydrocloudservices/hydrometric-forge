from prefect import task, Flow
import pandas as pd
import os
import xarray as xr
from prefect.executors import DaskExecutor
from pangeo_forge_recipes.storage import FSSpecTarget
from fsspec.implementations.local import LocalFileSystem
import shutil
import fsspec

from config import Config
f

@task
def merge_datasets():

    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)

    datasets_list = [fs.get_mapper(Config.DEH_ZARR_BUCKET),
                         ]

    ds = xr.open_mfdataset(datasets_list, 
                    engine='zarr',
                    consolidated=True, 
                    parallel=True)
    
    #ds = ds.chunk({'id': 1, 'time_agg': 1, 'timestep': 1, 'time': -1, 'spatial_agg': 1})
    for var in ds.variables:
        ds[var].encoding.clear()

    ds['name'] = ds.name.astype(object)
    ds['id'] = ds.id.astype(object)
    ds['latitude'] = ds.latitude.astype('float32')
    ds['longitude'] = ds.longitude.astype('float32')
    ds['province'] = ds.province.astype(object)
    ds['drainage_area'] = ds.drainage_area.astype('float32')
    ds['regulated'] = ds.regulated.astype(object)
    ds['source'] = ds.source.astype(object)
    ds['timestep'] = ds.timestep.astype(object)
    ds['spatial_agg'] = ds.spatial_agg.astype(object)
    ds['time_agg'] = ds.time_agg.astype(object)
    ds['variable'] = ds.time_agg.astype(object)
    
        
    ds.to_zarr('/tmp/combined/timeseries', consolidated=True)

@ task()
def push_data_to_bucket():
    lfs = LocalFileSystem()
    target = FSSpecTarget(fs=lfs, root_path='/tmp/combined/timeseries')
    fs = fsspec.filesystem('s3', **Config.STORAGE_OPTIONS)
    fs.put(target.root_path, os.path.dirname(Config.DEH_ZARR_BUCKET), recursive=True)
    shutil.rmtree(target.root_path)

if __name__ == '__main__':
    with Flow("Hydrometric-ETL", executor=DaskExecutor()) as flow:

        merged = merge_datasets()
        push_data_to_bucket(upstream_tasks=[merged])

    flow.run()