import pandas as pd
import os
from datetime import datetime

def get_existing_leagues(file = 'Files/leagues_info.csv', backup = 'Files/backups/leagues_info.csv'):
    existing_leagues = pd.read_csv(file, dtype='str').sort_values(by='league_id', ascending=False)
    existing_leagues.to_csv(backup, index = False)
    
    return existing_leagues


def get_existing_users(file = 'Files/user_list.csv', backup = 'Files/backups/user_list.csv'):
    existing_users = pd.read_csv(file, dtype='str').sort_values(by='picked_by', ascending=False)
    existing_users.to_csv(backup, index = False)
    
    return existing_users

def get_draft_meta(file = 'Files/draft_meta.csv', backup = 'Files/backups/draft_meta.csv'):
    draft_meta = pd.read_csv(file, dtype='str').sort_values(by='draft_id', ascending=False)
    draft_meta['draft_time'] == draft_meta['draft_time'].astype('datetime64')
    draft_meta.to_csv(backup, index=False)
    
    return draft_meta

def get_regular_drafts(file = 'Files/draft_meta.csv', scoring_types = ['ppr', 'half_ppr', 'std', '2qb', 'idp'], draft_types = ['snake']):
    dm = pd.read_csv(file, dtype='str')
    dm = dm[dm['scoring_type'].isin(scoring_types)]
    dm = dm[dm['type'].isin(draft_types)]
    
    return dm

def get_draft_results(file = 'Files/draft_results.csv', backup='Files/backups/draft_results.csv'):
    dr = pd.read_csv(file, dtype='str')
    dr['run_time'] = dr['run_time'].astype('datetime64[ns]')
    dr.to_csv(backup, index=False)
    
    return dr

def in_season_leagues():
    df = get_existing_leagues()
    df = df[df['status'] == 'in_season']
    
    return df

def in_season_drafts():
    df = in_season_leagues()
    drafts = df.drop_duplicates(subset=['draft_id']).draft_id.to_list()
    
    return drafts

def get_season_projections():
    df = pd.read_csv('Files/season_projections.csv')
    
    return df

def set_superflex():
    existing_leagues['superflex'] = [item.count('SUPER_FLEX') for item in existing_leagues['roster_positions']]
    
def get_active_leagues():
    path = 'Files/active_leagues.csv'
    
    t = os.path.getmtime(path)
    lu = datetime.fromtimestamp(t).date()
    today = datetime.today().date()
    
    df = pd.read_csv('Files/active_leagues.csv')
    