import requests
import os
import sys
import json
import pandas as pd
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

todays_date = date.today()
tomm_date = date.today() + timedelta(days=1)
# start_date = todays_date.strftime("%Y-%m-%d");
# end_date = tomm_date.strftime("%Y-%m-%d");
start_date = sys.argv[1]
end_date = sys.argv[2]

os.environ['TZ'] = 'UTC'
output_dir = "/home/nithin/Druid_Tibil/Mauka_Report/"


def Mauka_Report():
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
        "queryType": "topN",
        "dataSource": "summary-rollup-syncts",
        "aggregations": [
            {
                "type": "longSum",
                "name": "sum__total_count",
                "fieldName": "total_count"
            }
        ],
        "granularity": "all",
        "postAggregations": [],
        "intervals": start_date + "/" + end_date,
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "collection_type",
                    "value": "Course"
                },
                {
                    "type": "and",
                    "fields": [
                        {
                            "type": "selector",
                            "dimension": "dimensions_type",
                            "value": "content"
                        },
                        {
                            "type": "and",
                            "fields": [
                                {
                                    "type": "selector",
                                    "dimension": "dimensions_mode",
                                    "value": "play"
                                },
                                {
                                    "type": "or",
                                    "fields": [
                                        {
                                            "type": "selector",
                                            "dimension": "dimensions_pdata_id",
                                            "value": "prod.diksha.app"
                                        },
                                        {
                                            "type": "selector",
                                            "dimension": "dimensions_pdata_id",
                                            "value": "prod.diksha.portal"
                                        },
                                        {
                                            "type": "selector",
                                            "dimension": "dimensions_pdata_id",
                                            "value": "prod.diksha.desktop"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        "threshold": 10000,
        "metric": "sum__total_count",
        "dimension": "collection_created_for"
    }
    jsondata = json.dumps(query_str)
    print(jsondata)
    final_list = []
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()

        if (len(x) != 0):
            result_list = x[0]['result']
            df_pandas = pd.DataFrame(result_list)
            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

            client = gspread.authorize(creds)

            sheet1 = client.open("Mauka_Two_Report_Final").worksheet("MapFile")

            content_id_list = sheet1.get_all_values()

            df_map = pd.DataFrame(content_id_list[1:],columns=content_id_list[0])
            df_merge = df_pandas.merge(df_map, on=['collection_created_for'], how='inner')
            df_req = df_merge[['tenant_name', 'sum__total_count']]
            df_req['Date'] = start_date
            df_agg   = df_req.groupby(['Date', 'tenant_name'])['sum__total_count'].sum().reset_index()
            df_agg['tenant_name']=df_agg['tenant_name'].str.strip()
            df_pivot = df_agg.set_index(['Date', 'tenant_name']).sum__total_count.unstack().reset_index()
            file_name = output_dir + "Mauka_Report_" + start_date + ".csv"
            df_pivot.to_csv(file_name, index=0)
            #df_pivot['Tripura']=0;
            # df_pivot['Meghalaya']=0;
            # df_pivot['Mizoram'] =0;
            # df_pivot['Daman & Dadra']=0
            # df_pivot['Uttarakhand'] = 0
            # df_pivot['Andaman and Nicobar Islands'] =0;
            # df_pivot['Nagaland'] =0;
            # df_pivot['Manipur']=0;
            # df_pivot['Arunachal Pradesh']=0;
            #df_pivot['Tamilnadu']=0
            #df_pivot['Punjab']=0;

            df_final = df_pivot[
                ['Date', 'Gujarat', 'Madhya Pradesh', 'UP', 'CBSE', 'The Teacher App', 'Rajasthan', 'Haryana', 'Delhi',
                 'NCERT', 'Pondicherry', 'Jharkhand State', 'APEKX', 'Andaman and Nicobar Islands', 'Tamilnadu',
                 'Bihar', 'Manipur', 'Meghalaya', 'Odisha', 'Karnataka', 'Mizoram', 'Nagaland', 'Uttarakhand',
                 'Arunachal Pradesh', 'Chandigarh', 'Chhattisgarh', 'Tripura', 'Assam', 'Jammu And Kashmir',
                 'Maharashtra', 'Daman & Dadra','Ladakh','Lakshadweep','Telangana','CISCE content partner','Sikkim','Himachal Pradesh','Goa','Punjab','Tenant Ambigious']]
            print(df_final)

            return df_final
        else:
            df_final = pd.DataFrame(
                {'Date': [start_date], 'Gujarat': [0], 'Madhya Pradesh': [0], 'UP': [0], 'CBSE': [0],
                 'The Teacher App': [0], 'Rajasthan': [0], 'Haryana': [0], 'Delhi': [0], 'NCERT': [0],
                 'Pondicherry': [0], 'Jharkhand State': [0], 'APEKX': [0], 'Andaman and Nicobar Islands': [0],
                 'Tamilnadu': [0]})
            return df_final


    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


df_data = Mauka_Report()
header_list = list(df_data.columns)
overall_snapshot = df_data.values.tolist()
try:
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        '/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

    client = gspread.authorize(creds)

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    sheet1 = client.open("Mauka_Two_Report_Final").worksheet("Mauka-Rewamp")
    length_check_one = sheet1.get_all_values()
    if (len(length_check_one) == 0):
        sheet1.insert_row(header_list, 1)
    xi = sheet1.get_all_values()
    zi = len(xi)
    for w in overall_snapshot:
        zi += 1
        sheet1.insert_row(w, zi)
except Exception as e:
    print(e)
