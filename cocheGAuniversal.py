#Se agregan modulos para poder ser usados.
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import re
from google.cloud import bigquery
import unicodedata
from datetime import datetime, timedelta, timezone
import json 
import requests

#Se declara proyecto y id de tabla de BQ
# project_id = 'inter-bd-apis'
# table_id = 'inter-bd-apis.ga_reports_etl.ga-coche-lead-v03'

# #Se declaran variables para poder ser usadas
# client = bigquery.Client(project=project_id)
# table_obj = client.get_table(table_id)

#Se agrega variable con los valores de los campos a recibir y que serán mandados a tabla de BQ
camposBQ=['date', 'sourceMedium', 'campaign', 'hostname', 'eventAction', 'dimension10', 'dimension16']

with open("/Users/pablovega/Documents/GAUniversal_code/credentials_1.json", "r") as file:
    data = json.load(file)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = data
gaId = 'UA-160615176-1'


#se inicializa el método que hará el llamado al API de Google Analytics, en el cual se pasan las credenciales para poder autentificar usuario de GCP
def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(KEY_FILE_LOCATION, SCOPES)
    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics
  
def yesterday_date_cdmx():
  mexico_tz = timezone(timedelta(hours=-6))
  yesterday_datetime = datetime.now(mexico_tz) - timedelta(days=1)
  return yesterday_datetime.strftime('%Y-%m-%d') 

#Se genera reporte para poder obtener datos especificos del request.

def get_report(analytics):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
          "reportRequests": [
              {
                  "dateRanges": [{"startDate": "2023-05-01", "endDate": "2023-05-31"}],
                  "dimensions": [{"name": "ga:date"},
                                {"name": "ga:sourceMedium"},
                                {"name": "ga:campaign"},
                                {"name": "ga:dimension10"},
                                {"name": "ga:dimension16"},
                                {"name": "ga:hostname"}],
                  "viewId": '213597872',
                  "pageSize": "100000",
                  "dimensionFilterClauses": [
                    {"filters": [{
                      "dimensionName": "ga:eventAction",
                      "operator": "REGEXP",
                      "expressions": [
                        "(^)(step_2_submit)($)"
                      ]
                        } 
                      ]
                    }
                  ]
              }
          ]
      }
  ).execute()
  



#se genera lógica para poder obtener los datos y poder ordenarlos de una manera adecuada para ser enviados a la base de datos en la tabla de big query ya creada.
def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
      response: An Analytics Reporting API V4 response.
    """
    mi_dict = {}
    mi_lista = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        # print(report)
        # se hace iteración y se van agregando valores a un dict nuevo
        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            mi_dict['date'] = dimensions[0]
            mi_dict['sourceMedium'] = dimensions[1]
            mi_dict['campaign'] = dimensions[2]
            mi_dict['dimension10'] = dimensions[3]
            mi_dict['dimension16'] = dimensions[4]
            mi_dict['hostname'] = dimensions[5]
            ga_noCotizacion = dimensions[4]
              
            hex_array = re.findall(r'[0-9a-fA-F]+', dimensions[3])
            # Decode the hexadecimal values to their corresponding characters
            decoded_str = ''.join([chr(int(x[:-1], 16)) for x in hex_array])
            decoded_str_1= re.sub(r'[\u0300-\u036f]|[^a-zA-Z0-9\-\_\.@]', '',unicodedata.normalize("NFD",decoded_str)).lower()
        
            # print(decoded_str)
            records = {
                        'ga_date':mi_dict['date'],
                        'ga_sourceMedium':mi_dict['sourceMedium'],
                        'ga_campaign':mi_dict['campaign'],
                        'mail':decoded_str_1,
                        'ga_noCotizacion':ga_noCotizacion,
                        'ga_hostname':mi_dict['hostname']
                    }
            i = 0
            mi_lista.insert(i, records)
            i += 1
            # print(row)
        # errors = client.insert_rows(table=table_obj, rows=mi_lista)
        # if errors == []:
        #   print("New rows have been added.")
        # # # En caso de insertar más data histórica antes del 01-01-2023
        # #   data = {"text": "Fuente Google Analytics: "+ gaId + "\n" +"Cliente: inter\nInforme: Leads\nProducto: Médico y Coche\nFechas comprendidas: 2023-01-01 al " + str(yesterday_date_cdmx()) + "\nBD: " + table_id + "\ntotal de filas insertadas " + str(yesterday_date_cdmx()) + ": " + str(len(mi_lista))}
        # #   headers = {'Content-type': 'application/json'}
        # #   response = requests.post('https://hooks.slack.com/services/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', data=json.dumps(data), headers=headers)
        # else:
        #   print("Encountered errors while inserting rows: {}".format(errors))
        # #   data = {"text": "Error al cargar los datos "}
        # #   headers = {'Content-type': 'application/json'}
        # #   response = requests.post('https://hooks.slack.com/services/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', data=json.dumps(data), headers=headers)
        #   return("Encountered errors while inserting rows: {}".format(errors))
              

def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    print_response(response)
    return (response)
main()