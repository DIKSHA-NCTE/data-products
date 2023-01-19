"""
CMO-PMO Dashbaord report generation.
Reads daily metric data from blob storage and uploads
"""
import time
import pandas as pd

from datetime import datetime, timedelta
from pathlib import Path

from dataproducts.util.utils import post_data_to_blob, create_json, get_tenant_info, get_data_from_blob, \
    push_metric_event

class CMODashboard:
    def __init__(self, data_store_location, execution_date, org_search):
        self.data_store_location = Path(data_store_location)
        self.execution_date = execution_date
        self.org_search = org_search

    def data_wrangling(self, result_loc_, date_):
        """
        Extract last 30 days' daily metrics data from date of run.
        :param result_loc_: Path object for file path
        :param date_: datetime object for date sorting
        :return: None
        """
        df = pd.read_csv(result_loc_)[['Date', 'Total Content Plays']]
        df.index = pd.to_datetime(df.Date, format='%d-%m-%Y')
        df = df.loc[(date_ - timedelta(days=30)).strftime('%Y-%m-%d'):date_.strftime('%Y-%m-%d')]
        df.to_csv(result_loc_.parent.joinpath('cmo_dashboard.csv'), index=False)
        if result_loc_.parent.name == 'overall':
            result_loc_.parent.parent.joinpath('public').mkdir(exist_ok=True)
            df.to_csv(result_loc_.parent.parent.joinpath('public', 'cmo_dashboard.csv'), index=False)

    def init(self):
        start_time_sec = int(round(time.time()))
        print("START:CMO Dashboard")
        data_store_location = self.data_store_location.joinpath('portal_dashboards')
        data_store_location.mkdir(exist_ok=True)
        analysis_date = datetime.strptime(self.execution_date, "%d/%m/%Y")
        data_store_location.joinpath('public').mkdir(exist_ok=True)
        get_data_from_blob(data_store_location.joinpath('overall', 'daily_metrics.csv'))
        self.data_wrangling(result_loc_=data_store_location.joinpath('overall', 'daily_metrics.csv'),
                            date_=analysis_date)
        create_json(data_store_location.joinpath('public', 'cmo_dashboard.csv'), last_update=True)
        post_data_to_blob(data_store_location.joinpath('public', 'cmo_dashboard.csv'))
        get_tenant_info(result_loc_=data_store_location.parent.joinpath('textbook_reports'),
                        org_search_=self.org_search,
                        date_=analysis_date)
        board_slug = pd.read_csv(
            data_store_location.parent.joinpath('textbook_reports', analysis_date.strftime('%Y-%m-%d'),
                                                'tenant_info.csv'))
        slug_list = board_slug['slug'].unique().tolist()
        for slug in slug_list:
            try:
                get_data_from_blob(result_loc_=data_store_location.joinpath(slug, 'daily_metrics.csv'))
                self.data_wrangling(result_loc_=data_store_location.joinpath(slug, 'daily_metrics.csv'),
                                    date_=analysis_date)
                create_json(read_loc_=data_store_location.joinpath(slug, 'cmo_dashboard.csv'), last_update=True)
                post_data_to_blob(result_loc_=data_store_location.joinpath(slug, 'cmo_dashboard.csv'))
            except:
                pass
        print("END:CMO Dashboard")

        end_time_sec = int(round(time.time()))
        time_taken = end_time_sec - start_time_sec
        metrics = [
            {
                "metric": "timeTakenSecs",
                "value": time_taken
            },
            {
                "metric": "date",
                "value": analysis_date.strftime("%Y-%m-%d")
            }
        ]
        push_metric_event(metrics, "CMO Dashboard")
