import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

year_key = '15YBNlDKB7XQDMZBYMRMwfcuEt0MgeDKtM_nWSap1IBY' #2021
tableau_key = '14Psm8QbKMXC6K3EYQNMg0afa76VcRO-t6qJhZ2w9WXE' #2021
vor_key = '1OIZkzR_TzdfOj1HPU7CDd4RAlLUTxerMx8B4Cy327EM' #2021

cred = 'sleeper_project.json'
scope = ['https://spreadsheets.google.com/feeds',
     'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(cred, scope)

def open_spreadsheet():
 
    gc = gspread.authorize(credentials)
    
    return gc.open_by_key(year_key)


def select_sheet(sheet):
     return open_spreadsheet().worksheet(sheet)
    
    
def predraft(scoring_type='ppr'):
    l = scoring_type.lower()
    if l == 'standard':
        st = 'Standard'
    elif l == 'half ppr':
        st = 'Half PPR'
    elif l == 'ppr':
        st = 'PPR'
    else:
        raise ValueError("'" + scoring_type + "' is not allowed. Please use 'PPR', 'Half PPR', or 'Standard'.")
    
    sheet = open_spreadsheet().worksheet(st)
    return get_as_dataframe(sheet, usecols=[0,1,2,3,4,5], evaluate_formulas=True).dropna()

def set_vor_sheets():
    vor = pd.read_csv('Files/vor.csv')
    players = pd.read_csv('Files/tableau.csv', usecols=['player_id', 'full_name', 'team']).drop_duplicates().reset_index(drop=True)
    
    vor = vor.merge(players, on='player_id', how = 'left')
    vor = vor[['full_name', 'position', 'team','league_size','scoring_type','vor','adp', 'pts']]
    
    dppr = vor[vor['scoring_type'] == 'ppr']
    dppr['scoring_type'] = 'dynasty_ppr'
    
    d2qb = vor[vor['scoring_type'] == '2qb']
    d2qb['scoring_type'] = 'dynasty_2qb'
    
    vor = vor.append(dppr)
    vor = vor.append(d2qb)
    
    
    vor = vor.sort_values(by=['scoring_type','league_size','vor'], ascending=[True, False, False])
    vor.columns = ['Full Name','Position', 'Team', 'League Size', 'Scoring Type', 'VOR', 'ADP', 'Projection']
    
    vor = vor.dropna()
    vor = vor.drop_duplicates(subset=['Full Name', 'Position', 'League Size','Scoring Type', 'VOR'])

    gc = gspread.authorize(credentials)
    
    book = gc.open_by_key(vor_key)
    sheet = book.worksheet('All VORs')
    
    sheet.clear()
    set_with_dataframe(sheet, vor)
    
def get_mock_drafts(location = year_key):
    s = select_sheet('Messages')
    df = get_as_dataframe(s, usecols=[2], dtype = {'draft_id': 'str'}).dropna()
    return df