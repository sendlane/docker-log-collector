import settings
import traceback
import pickle
import os
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class GoogleSheet():

    def __init__(self):
        self.spreadsheet_id = os.getenv("TICKET_SPREADSHEET_ID")
        self.range = os.getenv("TICKET_SPREADSHEET_RANGE")
        self.column = int(os.getenv("TICKET_NUMBER_COLUMN"))
        self.google_pickle_file = os.getenv("GOOGLE_SHEETS_PICKLE_FILE")
        self.google_credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        self.credentials = self._get_credentials()
        self.google_service = build('sheets', 'v4', credentials=self.credentials, cache_discovery=False)
        self.service = self.google_service.spreadsheets()


    def _get_credentials(self):
        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self.google_pickle_file):
            with open(self.google_pickle_file, 'rb') as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.google_credentials_file, SCOPES)
                creds = flow.run_local_server(
                    'localhost', 56567)    # random port to avoid port collisions
                # try:
                #     creds = flow.run_local_server()
                # except:
                #     print("run server failed")
            # Save the credentials for the next run
            with open(self.google_pickle_file, 'wb') as token:
                pickle.dump(creds, token)
        return creds



    def get_sheet(self):
        result = self.service.values().get(spreadsheetId=self.spreadsheet_id,
                                     range=self.range).execute()
        return result

    def update(self, range_name, values):
        payload=dict(
            range=range_name,
            majorDimension="ROWS",
            values=values
        )
        return self.service.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=payload).execute()

    def append(self, values):    #, updates):
        payload = {"majorDimension": "ROWS", "values": values}
        return self.service.values().append(
            spreadsheetId=self.spreadsheet_id,
            range=self.range,
            body=payload,
            valueInputOption="USER_ENTERED"
        ).execute()

    def delete_row(self,sheet_name,row_number):
        sheet_metadata = self.service.get(spreadsheetId=self.spreadsheet_id).execute()
        sheets = sheet_metadata.get("sheets", [])
        for sheet in sheets:
            if sheet['properties']['title'] == sheet_name:
                sheet_id = sheet['properties']['sheetId']
                payload = { "requests": [{"deleteDimension": { "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": row_number, "endIndex": row_number+1} } }] }
                return self.service.batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=payload,
                ).execute()
                sys.exit(1)


    def get_ticket_to_row_mapping(self):
        sheet = self.get_sheet()
        counter = 0
        ticket_to_row = dict()
        for row in sheet['values']:
            try:
                ticket_number = row[self.column]
                title = row[6]
                ticket_to_row[int(ticket_number)] = counter
            except Exception:
                pass
            counter += 1
        return ticket_to_row

if __name__ == '__main__':
    gs = GoogleSheet()
    ticket_map = gs.get_ticket_to_row_mapping()
