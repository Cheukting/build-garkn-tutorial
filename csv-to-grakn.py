from grakn.client import GraknClient
import pandas as pd
from tqdm import tqdm
tqdm.pandas()

def read_csv(path_to_file):
    """
    Reading the csv with only the columns that we are interested
    and filling the missing data using pandas.
    """
    using_cols = ['name',
                  'title',
                  'male',
                  'culture',
                  'mother',
                  'father',
                  'heir',
                  'house',
                  'spouse',
                  'age',
                  'isAlive']
    # specify the columns that we are interested in
    data = pd.read_csv(path_to_file, usecols = using_cols)
    # rename the column 'name' to 'char_name'
    # as 'name' is an attribute for pandas DataFrame
    data = data.rename(columns={'name': 'char_name'})
    # missing data will be filled the same data type
    # filling missing age with -1
    data['age'] = data['age'].fillna(-1).astype('int')
    # others will be filled with empty string
    data = data.fillna("")
    return data

def insert_one_character(df,session):
    """
    Given one row of data, insert one character to the graph.
    """
    # parsing the data for 'gender' and 'alive'
    if df.male:
        gender = 'male'
    else:
        gender = 'female'
    if df.isAlive:
        alive = 'true'
    else:
        alive ='false'
    # write the graql query
    graql_insert_query = f'insert $character isa character, ' \
                         f'has name "{df.char_name}", ' \
                         f'has title "{df.title}", ' \
                         f'has gender "{gender}", ' \
                         f'has culture "{df.culture}", ' \
                         f'has age {df.age}, ' \
                         f'has alive {alive};'

    with session.transaction().write() as transaction:
        # make a write transection with the query
        transaction.query(graql_insert_query)
        # remember to commit at the end
        transaction.commit()

def insert_one_house(house,session):
    """
    Insert one house to the graph.
    """
    with session.transaction().write() as transaction:
        # make a write transection with the query
        transaction.query(f'insert $house isa house, ' \
                          f'has name "{house}";')
        # remember to commit at the end
        transaction.commit()

def insert_one_marriage(df,session):
    """
    Given one row of data, insert one marriage to the graph.
    """
    # do nothing if `spouse` data is missing
    if df['spouse'] == "":
        return None

    with session.transaction().write() as transaction:
        # write the graql query
        graql_insert_query = f'match $character isa character, ' \
                           f'has name "{df.char_name}"; ' \
                           f'$spouse isa character, ' \
                           f'has name "{df.spouse}";' \
                           f'insert $marriage(partner1: $spouse, partner2: $character) isa marrage;'
        # make a write transection with the query
        transaction.query(graql_insert_query)
        # remember to commit at the end
        transaction.commit()

def insert_one_membership(df,session):
    """
    Given one row of data, insert one membership to the graph.
    """
    # do nothing if `house` data is missing
    if df['house'] == "":
        return None

    with session.transaction().write() as transaction:
        # write the graql query
        graql_insert_query = f'match $character isa character, ' \
                           f'has name "{df.char_name}"; ' \
                           f'$house isa house, ' \
                           f'has name "{df.house}";' \
                           f'insert $membership(organization: $house, member: $character) isa membership;'
        # make a write transection with the query
        transaction.query(graql_insert_query)
        # remember to commit at the end
        transaction.commit()

def insert_one_parental(df,session):
    """
    Given one row of data, insert all parental relationship related to that character to the graph.
    """
    # do nothing if all remationship is missing
    if (df[['mother','father','heir']] == "").all():
        return None

    # first half of the query, all queries are based on the same character
    graql_query_base = f'match $character isa character, ' \
                       f'has name "{df.char_name}"; '

    with session.transaction().write() as transaction:
        # if there is a `father` given, complete the query with `father`
        if df['father'] != "":
            graql_query = graql_query_base + \
                         f'$father isa character, ' \
                         f'has name "{df.father}"; ' \
                         f'insert $parental(parent: $father, heir: $character) isa parental;'
            # make a write transection with the query
            transaction.query(graql_query)
            # remember to commit at the end
            transaction.commit()

    with session.transaction().write() as transaction:
        # if there is a `mother` given, complete the query with `mpther`
        if df['mother'] != "":
            graql_query = graql_query_base + \
                         f'$mother isa character, ' \
                         f'has name "{df.mother}"; ' \
                         f'insert $parental(parent: $mother, heir: $character) isa parental;'
            # make a write transection with the query
            transaction.query(graql_query)
            # remember to commit at the end
            transaction.commit()

    with session.transaction().write() as transaction:
        # if there is a `mother` given, complete the query with `mpther`
        if df['heir'] != "":
            graql_query = graql_query_base + \
                         f'$heir isa character, ' \
                         f'has name "{df.heir}"; ' \
                         f'insert $parental(parent: $character, heir: $heir) isa parental;'
            # make a write transection with the query
            transaction.query(graql_query)
            # remember to commit at the end
            transaction.commit()

def load_data_into_grakn(session,input_df):
    """
    Loading the data form the DataFrame to the graph in parts
    """

    print("Inserting characters...")
    # using progress_apply instead of apply so we have a progress bar form tqdm
    input_df.progress_apply(insert_one_character, axis=1, session=session)

    print("Inserting houses...")
    # get all_houses as a list form the DataFrame
    all_houses = list(input_df['house'].unique())
    # remove the empty string
    all_houses.remove("")
    # load them in one by one, with tqdm giving a progress bar
    for house in tqdm(all_houses):
        insert_one_house(house,session)

    print("Inserting parental...")
    # using progress_apply instead of apply so we have a progress bar form tqdm
    input_df.progress_apply(insert_one_parental, axis=1, session=session)

    print("Inserting marrage...")
    # using progress_apply instead of apply so we have a progress bar form tqdm
    input_df.progress_apply(insert_one_marriage, axis=1, session=session)

    print("Inserting membership...")
    # using progress_apply instead of apply so we have a progress bar form tqdm
    input_df.progress_apply(insert_one_membership, axis=1, session=session)

def build_grakn_graph(input_df, keyspace_name):
    """
    Create a connection with the graph with a specifil keyspace
    using the GraknClient and load the DataFrame into the graph
    """
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace = keyspace_name) as session:
            load_data_into_grakn(session,input_df)

### main part of the program ###

# read the csv into DataFrame, it is stored in the sub-directory named 'data'
raw_data = read_csv('data/game-of-thrones-character-predictions.csv')
# call the function to build and load the graph from the DataFrame
build_grakn_graph(raw_data, 'game_of_thrones')
