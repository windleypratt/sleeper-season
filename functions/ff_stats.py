from functions.storage_functions import *

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import http.client, urllib.request, urllib.parse


def season_player_projections(update = True, season = None):
    t = os.path.getmtime('Files/season_projections.csv')
    lu = datetime.fromtimestamp(t).date()
    today = datetime.today().date()

    if lu < today:
        
        print('Updating pseason projections for today.')

        if season == None:
            yr = datetime.today().year
        else:
            yr = season


        conn = http.client.HTTPSConnection('api.sleeper.app')
        conn.request("GET", "/projections/nfl/" + str(yr) + "?season_type=regular&order_by=pts_ppr")
        response = conn.getresponse().read()

        proj_df = pd.json_normalize(json.loads(response))
        proj_df.columns = proj_df.columns.str.replace('stats.','')
        proj_df.columns = proj_df.columns.str.replace('player.','')
        proj_df.columns = proj_df.columns.str.replace('metadata.','')
        proj_df.rename(columns={'id':'player_id'}, inplace=True)
        proj_df.index = proj_df['player_id']

        proj_df_pts = proj_df.filter(regex='pts_')
        proj_df_pts['player_id'] = proj_df_pts.index.astype(str)
        proj_df_pts.columns = proj_df_pts.columns.str.replace('pts_','')
        proj_df_pts['ppr'] = np.where(proj_df_pts['ppr'].isnull(), proj_df_pts['std'],proj_df_pts['ppr'])
        proj_df_pts = proj_df_pts.melt(id_vars=['player_id'], value_vars=list(proj_df_pts.columns).remove('player_id'), var_name='scoring_type', value_name='pts')

        qb = proj_df_pts[proj_df_pts['scoring_type'] == 'ppr']
        qb['scoring_type'] = '2qb'

        idp = proj_df_pts[proj_df_pts['scoring_type'] == 'ppr']
        idp['scoring_type'] = 'idp'

        proj_df_pts = proj_df_pts.append(qb).append(idp)

        proj_df_adp = proj_df.filter(regex='adp_')
        proj_df_adp['player_id'] = proj_df_adp.index.astype(str)
        proj_df_adp.columns = proj_df_adp.columns.str.replace('adp_','')
        proj_df_adp = proj_df_adp.melt(id_vars=['player_id'], value_vars=list(proj_df_adp.columns).remove('player_id'), var_name='scoring_type', value_name='adp')

        pts_adp = proj_df_adp.merge(proj_df_pts,on=['player_id', 'scoring_type'])

        #proj_df['player_id'] = proj_df.index.astype(str)
        proj_df.to_csv('Files/season_projections.csv', index=False)
        pts_adp.to_csv('Files/pts_adp.csv', index=False)
        
        conn.close()
    
    else:
        print('Projections have already been updated today. Returning results.')
        pts_adp = pd.read_csv('Files/pts_adp.csv')

    return pts_adp


def calculate_vor(days_back = 180, file='Files/vor.csv', backup = 'Files/backup.vor.csv'):
    
    py_df = get_draft_results()        
    dm = get_draft_meta()
    
    proj = season_player_projections()
    
    py_df = py_df.merge(dm[['draft_id', 'scoring_type','draft_time']])
    
    t = (datetime.now() - timedelta(days_back)).date() 
    
    py_df = py_df[py_df['draft_time'].astype('datetime64').dt.date >= t]
    
    league_sizes = py_df.groupby(['league_size', 'scoring_type']).draft_id.nunique().reset_index(name = 'leagues_count')
    
    vors = py_df[py_df['pick_no'].astype(int) < (10 * py_df['league_size'].astype(int))].groupby(['position', 'league_size','scoring_type']).size().reset_index(name = 'count')
    vors = vors.merge(league_sizes, on=['league_size', 'scoring_type'], how='left')
    vors['vor_base'] = round(vors['count'] / vors['leagues_count'])
    
    vor_df = py_df[['player_id', 'position', 'league_size','scoring_type']].drop_duplicates().reset_index(drop=True)
    vor_df = vor_df.merge(vors, on=['position', 'league_size','scoring_type'])
    
    vor_df = vor_df.merge(proj, on=['player_id','scoring_type'], how='left').drop_duplicates().reset_index(drop=True)
    
    ranks = vor_df[['player_id', 'position', 'league_size', 'scoring_type','vor_base', 'pts']].copy()
    ranks['rank_all'] = ranks.groupby(['position','league_size','scoring_type']).pts.rank(method='dense', ascending=False)
    
    base = ranks[['player_id','position', 'league_size','scoring_type','vor_base','pts','rank_all']].copy()
    base = base[base['rank_all'] == base['vor_base'] + 1]
    base = base[['position', 'league_size','scoring_type','pts']]
    base.columns = ['position', 'league_size','scoring_type','base_pts']
    
    vor_df = vor_df.merge(base, on=['position','league_size','scoring_type'])

    vor_df['vor'] = vor_df['pts'] - vor_df['base_pts']
    out_vor = vor_df[['player_id','position','league_size', 'scoring_type','vor_base','pts','adp','base_pts','vor']]
    
    dppr = out_vor[out_vor['scoring_type'] == 'ppr']
    dppr['scoring_type'] = 'dynasty_ppr'
    d2qb = out_vor[out_vor['scoring_type'] == 'ppr']
    d2qb['scoring_type'] = 'dynasty_2qb'
    
    out_vor = out_vor.append(dppr)
    out_vor = out_vor.append(d2qb)
    
    out_vor.to_csv(file, index=False)
    
    return out_vor

