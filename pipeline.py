from prefect import task, Flow, case, agent
import prefect
from prefect.schedules import IntervalSchedule
from datetime import datetime, timedelta
import urllib.request
import s3fs
import os
from pipeline.models.hydat import get_available_stations_from_hydat, import_hydat_to_parquet, verify_data_type_exists
from prefect.utilities.configuration import set_temporary_config
from pipeline.utils import get_url_paths


@task
def extract_hydat_path(url, ext):
    # fetch reference data
    path = get_url_paths(url=url, ext=ext)[1]
    return path

@task
def verify_if_to_date(path):
    # fetch reference data
    # Wasabi cloud storage configurations
    basename = os.path.basename(path)
    client_kwargs = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                     'region_name': 'us-east-2'}
    config_kwargs = {'max_pool_connections': 30}

    s3 = s3fs.S3FileSystem(profile='default',
                           client_kwargs=client_kwargs,
                           config_kwargs=config_kwargs)  # public read

    return s3.exists(os.path.join('s3://hydat-sqlite',
                                  basename))

@task
def download_hydat_file(path):
    # fetch reference data
    # Wasabi cloud storage configurations
    basename = os.path.basename(path)

    urllib.request.urlretrieve(os.path.join("https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www",
                                            basename),
                               os.path.join('/tmp',
                                            basename))

    client_kwargs = {'endpoint_url': 'https://s3.us-east-2.wasabisys.com',
                     'region_name': 'us-east-2'}
    config_kwargs = {'max_pool_connections': 30}

    s3 = s3fs.S3FileSystem(profile='default',
                           client_kwargs=client_kwargs,
                           config_kwargs=config_kwargs)  # public read

    s3.put(os.path.join('/tmp',
                        basename),
           os.path.join('s3://hydat-sqlite',
                        basename))
    return path

@task
def update_hydat_database():
    stations_list = get_available_stations_from_hydat()
    #
    # results = []
    for station_number in stations_list[0:10]:
        if verify_data_type_exists(station_number, 'Flow'):
            import_hydat_to_parquet(station_number)


# schedule to run every 12 hours
schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(hours=12),
    end_date=datetime.utcnow() + timedelta(seconds=20))

# import pendulum
#
# from prefect.schedules import Schedule
# from prefect.schedules.clocks import DatesClock
#
# schedule = Schedule(
#     clocks=[DatesClock([pendulum.now().add(seconds=1)])])

temp_config = {
    "cloud.agent.auth_token": prefect.config.cloud.agent.auth_token,
}


with Flow("Hydat-ETL", schedule=schedule) as flow:

    with set_temporary_config(temp_config):
        if flow.run_config is not None:
            labels = list(flow.run_config.labels or ())
        elif flow.environment is not None:
            labels = list(flow.environment.labels or ())
        else:
            labels = []
        agent = agent.local.LocalAgent(
            labels=labels, max_polls=30
        )

    url = 'https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/'
    ext = 'zip'

    path = extract_hydat_path(url, ext)
    cond = verify_if_to_date(path)

    with case(cond, False):
        download_hydat_file(path)
        update_hydat_database()

flow.register(project_name="hydat-file-upload")
agent.start()
