import pandas as pd
from datetime import datetime, timedelta
from prefect import task, Flow, Parameter
from prefect.schedules import CronSchedule, IntervalSchedule

@task(max_retries=10, retry_delay=timedelta(seconds=30))
def extract(url) -> pd.DataFrame:
    out = pd.read_json(url)
    if out.shape[0] == 0:
        raise Exception('No data extracted')
    return out

@task
def transform(df) -> pd.DataFrame:
    out_df = df[['bay_id',
                 'st_marker_id',
                 'status']]
    out_df['status'] = out_df['status'].apply(lambda x: 1 if x == 'Present' else 0)
    out_df['datetime'] = datetime.now().replace(second = 0, microsecond = 0)
    return out_df

@task
def load(data, path) -> None:
    stamp = int(datetime.now().timestamp())
    out_path = path + f'parking_{stamp}.csv'
    data.to_csv(path_or_buf = out_path, index = False)

# scheduler = CronSchedule(
#     # run every five minutes
#     cron = '*/5 * * * * *'
# )

scheduler = IntervalSchedule(
    interval=timedelta(minutes=10)
)

def prefect_flow():
    with Flow(name = 'parking_pipeline', schedule = scheduler) as flow:
        param_url = Parameter(name = 'p_url', required = True)

        df = extract(url=param_url)
        df = transform(df)
        # parking_{int(datetime.now().timestamp())}.csv
        load(data=df, path=f'./raw_data/')
    return flow


if __name__ == '__main__':
    flow = prefect_flow()
    flow.run(
        parameters = {
            'p_url': 'https://data.melbourne.vic.gov.au/resource/vh2v-4nfs.json'
        })
