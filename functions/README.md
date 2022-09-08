## Function Files

### 1. Storage Functions

Storage Functions are used to get or place stored data (.csv files) from the local directory. Some of them also include backups. The files themselves are not included in the git repository because of file size, but I will include headers in this so they can be replicated.

#### get_existing_leagues()

returns dataframe of existing unique leagues and info in the local archive

#### get_existing_users()

returns dataframe of existing unique users in the archive

#### get_draft_meta()

returns a dataframe of league draft metadata

#### get_regular_drafts(scoring_types=['ppr','half_ppr','std','2qb','idp'], draft_type=['snake'])

returns dataframe of filtered draft metadata according to the `scoring_types` and `draft_types` criteria

#### get_draft_results()

returns a very large dataframe with each pick of each draft in the local archive

#### in_season_leagues()

returns a dataframe list of all leagues currently "in season"

#### in_season_drafts()

returns a dataframe list of the drafts from `in_season_leagues`

#### get_season_projections()

returns a dataframe of season projections

#### get_active_leagues()

returns dataframe of leagues with recent transactions



### 2. Sleeper Functions

These functions work primarily in the [Sleeper.app API](https://docs.sleeper.app/#players), augmented by other functions in files within the same folder.

#### update_players(path='Files/palyers.csv', manual=False, status='all'):

The Players API from Sleeper is heavy, so out of respect this function attempts to update data at most once per day. It checks the last-modified time of `Files/players.csv` and runs the function only if the file has not been updated `today`.


### 3. Transaction Functions

#### get_transactions()

returns dataframe of transactions from input list of leagues