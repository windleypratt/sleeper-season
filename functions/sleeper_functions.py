from functions.storage_functions import *
from functions.ff_stats import *
from functions.logsheets import *
from datetime import datetime, timedelta
import pandas as pd
from random import sample as sp
import json
import os
import http.client, urllib.request, urllib.parse
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import gspread


'''Calling functions from logsheets.py '''

def update_players(path = 'Files/players.csv', manual = False, status = 'all'):
    t = os.path.getmtime(path)
    lu = datetime.fromtimestamp(t).date()
    today = datetime.today().date()
    
    if lu < today:
        print('Updating players from API...')
        conn = http.client.HTTPSConnection('api.sleeper.app')
        conn.request("GET", "/v1/players/nfl")
        response = conn.getresponse().read()

        df = pd.read_json(response)
        df = df.transpose()
        df.to_csv('Files/players.csv', header=True, index=False)
    else:
        print('Players already up to date.')
        df = pd.read_csv('Files/players.csv')
    
    cred = 'sleeper_project.json'
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(cred, scope)
    
    
    pb = '1KdacEVzGxsqG3-hHzVc3g1x6eACzb8Q1kqbfhFqLrZA'
    gc = gspread.authorize(credentials)
    book = gc.open_by_key(pb)
    sheet = book.worksheet('Players')
    sheet.clear()
    set_with_dataframe(sheet, df[df['active'] != 'True'], include_index=False)
    
    
    if status.lower() == 'all':
        return df
    else:
        return df[df['status'] != 'Inactive'].reset_index(drop=True)

def get_league(key = 'ons', league_id = None):
    if league_id != None:
        s = str(league_id)
    else:
        s = str(leagues.get(key))
        
    conn = http.client.HTTPSConnection('api.sleeper.app')

    try:
        conn.request("GET", '/v1/league/' + str(s) + '/users')
        start_conn = conn.getresponse().read()
        ls = json.loads(start_conn)

        lineups = pd.DataFrame(ls)

    except Exception as e:
        raise(e)

    return lineups


def leagues_from_users(users = None, limit=None, sample=None, year=None, update=False):
    
    if (limit != None) and (sample != None):
        if limit > sample:
            raise ValueError('Limit cannot be less than sample.')
    
    existing_leagues = get_existing_leagues()
    
    if users == None:
        existing_users = get_existing_users()
        users = existing_users.picked_by.to_list()
    else:
        if type(users) == list:
            users = users
        else:
            raise ValueError('users object must be of type: list')
    
    
    if sample != None:
        users = sp(users, sample)
    
    if limit != None:
        user_ids = users[:limit]
    else:
        user_ids = users

    
    if year == None:
        yr = datetime.today().year
    else:
        yr = year
    
    used = existing_leagues[existing_leagues['status'] == 'in_season'].league_id.to_list()

    conn = http.client.HTTPSConnection('api.sleeper.app')
    now = datetime.now()

    leagues = pd.DataFrame()
    ids = len(user_ids)
    i = 0
    start = datetime.now()
    for user_id in user_ids:
        i += 1
        if i % 250 == 0:
            pct = (i/ids)
            span = (datetime.now() - start) / pct
            print(str(i) + '/' + str(ids) + ' (' + "%.2f" % (pct * 100) + '%) ETR: ' + str(start + span))

        try:
            conn.request("GET", "/v1/user/" + str(user_id) + '/leagues/nfl/' + str(yr))
            response = conn.getresponse().read()
            jsonResponse = json.loads(response)

            users_leagues = pd.json_normalize(jsonResponse)

            these = users_leagues.league_id.to_list()

            users_leagues = users_leagues[~users_leagues['league_id'].isin(used)]

            used.extend(these)

            users_leagues = users_leagues[users_leagues['total_rosters'].between(8,14)]

            if len(users_leagues) > 0:
                leagues = leagues.append(users_leagues[['total_rosters', 'status', 'sport', 'shard', 'season_type', 'season', 'roster_positions', 'previous_league_id', 'name', 'league_id', 'last_message_time', 'group_id', 'draft_id', 'display_order', 'company_id', 'settings.max_keepers']])
            #leagues2 = leagues2.append(users_leagues[['league_id','draft_id', 'status', 'total_rosters']])
        except:
            pass
    
    conn.close()
    
    if update == True:
        a = len(existing_leagues)
        
        existing_leagues.index = existing_leagues['league_id']
        leagues.index = leagues['league_id']

        existing_leagues.update(leagues)
        
        existing_leagues = existing_leagues.append(leagues)
        existing_leagues = existing_leagues.drop_duplicates(subset=['league_id'])
        
        existing_leagues.to_csv('Files/leagues_info.csv', index = False)
        
        new = len(existing_leagues)-a
        
        print('Updated leagues with ' + str(new) + ' new records.')

    return existing_leagues.tail(new).reset_index(drop=True)



def users_from_leagues(leagues=None, limit=None, sample=None, update=False):
    
    if (limit != None) and (sample != None):
        if limit > sample:
            raise ValueError('Limit cannot be less than sample.')
    
    if leagues == None:
        leagues = get_existing_leagues()
        league_ls = leagues.league_id.to_list()
    else:
        if type(leagues) == list:
            league_ls = leagues
        else:
            raise ValueError('leagues object must be of type: list')
    del leagues
    
    if sample != None:
        league_ls = sp(league_ls, sample)
    
    if limit != None:
        league_ids = league_ls[:limit]
    else:
        league_ids = league_ls
    
    
    conn = http.client.HTTPSConnection('api.sleeper.app')
    
    users = pd.DataFrame()
    
    i = 0
    n = 0
    start = datetime.now()
    cnt = len(league_ids)
    
    for league in league_ids:
        i += 1
        if i // 250 != n:
            n += 1
            pct = (i/cnt)
            span = (datetime.now() - start) / pct
            print(str(i) + '/' + str(cnt) + ' (' + "%.2f" % (pct * 100) + '%) ETR: ' + str(start + span))
        
        try:
            conn.request("GET", '/v1/league/' + str(league) + '/users')
            start_conn = conn.getresponse().read()
            ls = json.loads(start_conn)

            lineups = pd.DataFrame(ls)

            user_ids = pd.DataFrame(lineups.user_id)
            users = users.append(user_ids)
        except:
            pass
    
    users.columns = ['picked_by']
    users = users.drop_duplicates()
    
    if update == True:
        
        eu = get_existing_users()
        
        a = len(eu)

        eu = eu.append(users)
        eu = eu.drop_duplicates(subset=['picked_by'])
        
        eu.to_csv('Files/user_list.csv', index = False)
        
        new = len(eu)-a
        
        print('Updated users with ' + str(new) + ' new records.')
    
    conn.close()
    return eu.tail(new).reset_index(drop=True)


def update_draft_meta(drafts='real',limit=None, sample=None, update=False):
    if (limit != None) and (sample != None):
        if limit > sample:
            raise ValueError('Limit cannot be less than sample.')
            
    el = get_existing_leagues()
    
    dm = get_draft_meta()
    used = dm.drop_duplicates(subset=['draft_id']).draft_id.to_list()
    
    el = el[~el['draft_id'].isin(used)]

    draft_ls = el[el['status']=='in_season'].drop_duplicates(subset=['draft_id']).draft_id.to_list()
    

    
    if sample != None:
        draft_ls = sp(draft_ls, sample)
    
    if limit != None:
        draft_ids = draft_ls[:limit]
    else:
        draft_ids = draft_ls
        
    print(len(draft_ids))
    conn = http.client.HTTPSConnection('api.sleeper.app')
    
    list_len = len(draft_ids)
    draft_details = pd.DataFrame()
    i = 0
    start = datetime.now()

    for d in draft_ids:
        i += 1
        if i % 250 == 0:
            pct = (i/list_len)
            span = (datetime.now() - start) / pct
            print(str(i) + '/' + str(list_len) + ' (' + "%.2f" % (pct * 100) + '%) ETR: ' + str(start + span))
        
        try:
            conn.request("GET", "/v1/draft/" + str(d))
            response = conn.getresponse().read()
            json_response = json.loads(response)

            dd = pd.json_normalize(json_response, record_prefix=True, meta_prefix=True)
            dd = dd.copy()
            dd = dd.reindex(columns =['draft_id','status','type','settings.teams','settings.rounds','last_picked','metadata.scoring_type','settings.slots_qb', 'settings.slots_rb','settings.slots_wr', 'settings.slots_te','settings.slots_flex'])
            dd.columns = ['draft_id','status','type','teams','rounds','last_picked','scoring_type','slots_qb', 'slots_rb','slots_wr', 'slots_te','slots_flex']

            dd['draft_time'] = datetime.fromtimestamp(dd.last_picked/1000)

            draft_details = draft_details.append(dd)
            
        except:
            #print('error on draft ' + str(d) + ' ('+ str(d_number) + ')')
            continue
    
    conn.close()
    
    draft_details = draft_details[draft_details['status'] == 'complete']
    draft_details = draft_details.reset_index(drop=True)
    
    if update == True:
        a = len(dm)
        all_drafts = dm.append(draft_details)
        all_drafts['last_picked'] = all_drafts['last_picked'].astype('str')
        all_drafts = all_drafts.sort_values(by='last_picked', ascending=False).drop_duplicates(subset=['draft_id'])
        b = len(all_drafts)
        print('Updated Draft Meta with ' + str(b-a) + ' new draft records.')
        all_drafts.to_csv('Files/draft_meta.csv', index=False)
        
        fin = all_drafts
    else:
        fin = draft_details
    
    return fin


def update_draft_results(drafts='regular', scoring_types=['dynasty_ppr','dynasty_2qb','ppr','half_ppr', 'std', '2qb', 'idp'], draft_types=['snake'], limit=None, sample=None, update=False):
    if (limit != None) and (sample != None):
        if limit > sample:
            raise ValueError('Limit cannot be less than sample.')
    
    ed = get_draft_results()
    used = ed.drop_duplicates(subset=['draft_id']).draft_id.to_list()
    
    bots = pd.read_csv('Files/bots.csv', dtype='object')
    used.extend(bots.bots.drop_duplicates().to_list())
            
    if drafts == 'all':
        dm = get_draft_meta()
    elif drafts == 'mock':
        dm = get_mock_drafts()
        update = False
    else:
        dm = get_regular_drafts(scoring_types = scoring_types, draft_types = draft_types)
        
    dm = dm[~dm['draft_id'].isin(used)]
    
    draft_ls = dm.drop_duplicates(subset=['draft_id']).draft_id.to_list()
    
    if sample != None:
        if sample < len(draft_ls):
            draft_ls = sp(draft_ls, sample)
    
    if limit != None:
        draft_ids = draft_ls[:limit]
    else:
        draft_ids = draft_ls
    
    draft = pd.DataFrame()
    bot_list = list()
                       
    idCount = 0
    errs = 0
    start = datetime.now()

    conn = http.client.HTTPSConnection('api.sleeper.app')
    print("Getting Sleeper Draft Picks from " + str(len(draft_ids)) + " league drafts.")
    for draft_id in draft_ids:
        try:
            idCount += 1
            if idCount % 250 == 0:
                pct = (idCount/len(draft_ids))
                span = (datetime.now() - start) / pct
                print(str(idCount) + '/' + str(len(draft_ids)) + ' (' + "%.2f" % (pct * 100) + '%) ETR: ' + str(start + span))
            
            conn.request("GET", "/v1/draft/" + str(draft_id) + "/picks")
            response = conn.getresponse().read()
            try:
                jsonResponse = json.loads(response)
            except:
                pass

            if (jsonResponse is not None) and (not len(jsonResponse) == 0):
                df = pd.json_normalize(jsonResponse, record_prefix=True, meta_prefix=True)
                length = len(df)
                league = max(df.draft_slot)
                leagueList = [league] * length
                df['league_size'] = leagueList

                timeList = [datetime.now()] * length
                df['run_time'] = timeList
                df['full_name'] = df['metadata.first_name'] + ' ' + df['metadata.last_name']

                real = df[df['picked_by'] != '']['picked_by'].nunique()
                ratio = real/league

                if ratio >= .75:
                    df = df[['round', 'roster_id', 'player_id', 'picked_by', 'pick_no', 'is_keeper',
           'draft_slot', 'draft_id', 'metadata.years_exp', 'metadata.team',
           'metadata.status', 'metadata.position', 'metadata.number', 'metadata.injury_status',
           'league_size', 'run_time', 'full_name']]
                    df.columns = ['round', 'roster_id', 'player_id', 'picked_by', 'pick_no', 'is_keeper',
           'draft_slot', 'draft_id', 'years_exp', 'team',
           'status', 'position', 'number', 'injury_status',
           'league_size', 'run_time', 'full_name']


                    if max(df.years_exp) == 0:
                        draft_type = 'rookie'
                    else:
                        draft_type = 'other'

                    df['draft_type'] = draft_type 

                    draft = draft.append(df, ignore_index=True)
                    
                else:
                    #print('Not enough real users')
                    #idCount = idCount - 1
                    bot_list.append(draft_id)
                    pass

            else:
                errs = errs + 1
                #idCount = idCount - 1
                #print("error " + str(errs))
                pass

            conn.close()
        except Exception as e:
            #print("[Errno {0}] {1}".format(e.errno, e.strerror))
            #print(e)
            pass

    bots = bots.append(pd.DataFrame(bot_list, columns=['bots']))
    bots.to_csv('Files/bots.csv', index=False)     
                    
                       
    if update == True:
        a = len(used)
        all_drafts = ed.append(draft)
        all_drafts = all_drafts.drop_duplicates()
        b = len(all_drafts.drop_duplicates(subset=['draft_id']).draft_id.to_list())
        print('Updated Draft Results with ' + str(b-a) + ' new drafts.')
        all_drafts.to_csv('Files/draft_results.csv', index=False)
        
        
        eu = get_existing_users()
        c = len(eu)
        pb = all_drafts[['picked_by']].drop_duplicates()
        eu = eu.append(pb).drop_duplicates()
        d = len(eu)
        print('Updated User List with ' + str(d - c) + ' new users.')
        eu.to_csv('Files/user_list.csv', index=False)
        
        fin = all_drafts
    else:
        fin = draft
    
    return fin

def prep_tableau(days_back = 45):
    t = (datetime.now() - timedelta(days_back)).date()
    
    dr = get_draft_results()
    dm = get_draft_meta()
    vor = calculate_vor(days_back = days_back)
    
    tableau = dr.merge(dm[['draft_id', 'draft_time','scoring_type','rounds']], on = 'draft_id', how='left').astype(str)
    tableau = tableau[tableau['draft_time'].astype('datetime64').dt.date >= t]
    
    inj = tableau[['player_id','injury_status', 'draft_time']]
    inj['draft_time'] = inj['draft_time'].astype('datetime64')
    inj = inj.loc[inj.groupby('player_id').draft_time.idxmax()][['player_id','injury_status']]
    
    tableau = tableau.drop('injury_status', axis=1)
    tableau = tableau.merge(inj, on='player_id', how='left')
    
    tableau.to_csv('Files/tableau.csv', index=False)
    set_vor_sheets()
    
    r1 = tableau[tableau['round'].astype(int) == 1]
    r1 = r1[['round','player_id','full_name','position','draft_id','draft_slot','scoring_type','league_size']]
    r1['key'] = r1['draft_id'].astype(str) + r1['draft_slot'].astype(str)
    r2 = tableau[tableau['round'].astype(int) == 2]
    r2 = r2[['round','player_id','full_name','position','draft_id','draft_slot','scoring_type','league_size']]
    r2['key'] = r2['draft_id'].astype(str) + r2['draft_slot'].astype(str)
    sankey = r1.merge(r2, on='key', suffixes=('_r1','_r2'))
    
    sankey.to_csv('Files/sankey.csv', index=False)
    
    return tableau


def ul_spider(seconds = 600, sample=500):
    start = datetime.now()
    print(str(start) + ": running for ~" + str(seconds/60) + " minutes.")
    i = 0
    l = pd.DataFrame()
    nu = 0
    nl = 0
    s = 0
    #us = 0
    while (datetime.now()-start).total_seconds() < seconds:
        i += 1
        a = datetime.now()
        print("Loop " + str(i))

        if len(l) == 0:
            print(" -- New Leagues From Sample")
            l = leagues_from_users(sample=sample, update=True)
            nl += len(l)
            s += 1
            ls = l.league_id.to_list()
        else: 
            ls = l.league_id.to_list()

        u = users_from_leagues(leagues=ls, update=True)
        nu += len(u)

        if len(u) == 0:
            print("-- New Leagues From Sample...")
            s += 1
            l = leagues_from_users(sample=sample, update=True)
            nl += len(l)
        else:
            us = u.picked_by.to_list()
            try:
                l = leagues_from_users(users=us, update=True)
                nl += len(l)
            except:
                pass

        print("--- " + str(datetime.now()-a))
    
    df = pd.read_csv('Files/userleaguedata.csv')
    df = df.append(pd.DataFrame([[seconds, sample, nu, nl, i, s]], columns = ['seconds', 'sample','new users','new leagues', 'loops','league_samples']))
    df.to_csv('Files/userleaguedata.csv', index=False)

    print("Updated with " + str(nu) + " new users and " + str(nl) + " new leagues.")
    print("Finished in " + str(datetime.now()-start))
    
def league_details():
    el=get_existing_leagues()
    dm=get_draft_meta()
    
    leagues = el.merge(dm[['draft_id','scoring_type','teams']], on='draft_id', how='left')
    leagues['is_superflex'] = leagues['roster_positions'].str.contains('SUPER')
    leagues = leagues[['league_id','draft_id','teams','scoring_type','is_superflex','last_message_time']]
    
    leagues = leagues.dropna(subset=['scoring_type']).sort_values(by='last_message_time', ascending=False)
    
    leagues.to_csv('Files/league_summary.csv', index=False)
    
    return leagues