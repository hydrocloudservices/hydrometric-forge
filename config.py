from datetime import datetime, timedelta
import os


class Config(object):

    CLIENT_KWARGS = {'endpoint_url': 'https://s3.us-east-1.wasabisys.com',
                     'region_name': 'us-east-1'}
    CONFIG_KWARGS = {'max_pool_connections': 30}
    PROFILE = 'default'
    STORAGE_OPTIONS = {'profile': PROFILE,
                       'client_kwargs': CLIENT_KWARGS,
                       'config_kwargs': CONFIG_KWARGS
                       }

   
    DEH_ZARR_BUCKET = f"hydrometric/source/deh/zarr/{datetime.now().strftime('%Y-%m-%d')}"
    #TODO : make dynamic
    HYDAT_ZARR_BUCKET = f"hydrometric/source/hydat/zarr/20230505"

    COMBINED_ZARR_BUCKET = f"hydrometric/source/combined/zarr/{datetime.now().strftime('%Y-%m-%d')}"