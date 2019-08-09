from grakn.client import GraknClient
import pandas as pd
from tqdm import tqdm
tqdm.pandas()

def read_csv(path_to_file):
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
    data = pd.read_csv(path_to_file, usecols = using_cols)
    data = data.rename(columns={'name': 'char_name'})
    data['age'] = data['age'].fillna(-1).astype('int')
    data = data.fillna("")
    return data

def insert_one_character(df,session):
    if df.male:
        gender = 'male'
    else:
        gender = 'female'
    if df.isAlive:
        alive = 'true'
    else:
        alive ='false'
    graql_insert_query = f'insert $character isa character, ' \
                         f'has name "{df.char_name}", ' \
                         f'has title "{df.title}", ' \
                         f'has gender "{gender}", ' \
                         f'has culture "{df.culture}", ' \
                         f'has age {df.age}, ' \
                         f'has alive {alive};'
    with session.transaction().write() as transaction:
        transaction.query(graql_insert_query)
        transaction.commit()

def insert_one_house(house,session):
    with session.transaction().write() as transaction:
        transaction.query(f'insert $house isa house, ' \
                          f'has name "{house}";')
        transaction.commit()

def insert_one_marriage(df,session):
    if df['spouse'] == "":
        return None

    with session.transaction().write() as transaction:
        graql_insert_query = f'match $character isa character, ' \
                           f'has name "{df.char_name}"; ' \
                           f'$spouse isa character, ' \
                           f'has name "{df.spouse}";' \
                           f'insert $marriage(partner1: $spouse, partner2: $character) isa marrage;'
        transaction.query(graql_insert_query)
        transaction.commit()

def insert_one_membership(df,session):
    if df['house'] == "":
        return None

    with session.transaction().write() as transaction:
        graql_insert_query = f'match $character isa character, ' \
                           f'has name "{df.char_name}"; ' \
                           f'$house isa house, ' \
                           f'has name "{df.house}";' \
                           f'insert $membership(organization: $house, member: $character) isa membership;'
        transaction.query(graql_insert_query)
        transaction.commit()

def insert_one_parental(df,session):
    if (df[['mother','father','heir']] == "").all():
        return None
    graql_query_base = f'match $character isa character, ' \
                       f'has name "{df.char_name}"; '
    with session.transaction().write() as transaction:
        if df['father'] != "":
            graql_query = graql_query_base + \
                         f'$father isa character, ' \
                         f'has name "{df.father}"; ' \
                         f'insert $parental(parent: $father, heir: $character) isa parental;'
            transaction.query(graql_query)
            transaction.commit()
    with session.transaction().write() as transaction:
        if df['mother'] != "":
            graql_query = graql_query_base + \
                         f'$mother isa character, ' \
                         f'has name "{df.mother}"; ' \
                         f'insert $parental(parent: $mother, heir: $character) isa parental;'
            transaction.query(graql_query)
            transaction.commit()
    with session.transaction().write() as transaction:
        if df['heir'] != "":
            graql_query = graql_query_base + \
                         f'$heir isa character, ' \
                         f'has name "{df.heir}"; ' \
                         f'insert $parental(parent: $character, heir: $heir) isa parental;'
            transaction.query(graql_query)
            transaction.commit()

def load_data_into_grakn(session,input_df):
    print("Inserting characters...")
    input_df.progress_apply(insert_one_character, axis=1, session=session)
    print("Inserting houses...")
    all_houses = list(input_df['house'].unique())
    all_houses.remove("")
    for house in tqdm(all_houses):
        insert_one_house(house,session)
    print("Inserting parental...")
    input_df.progress_apply(insert_one_parental, axis=1, session=session)
    print("Inserting marrage...")
    input_df.progress_apply(insert_one_marriage, axis=1, session=session)
    print("Inserting membership...")
    input_df.progress_apply(insert_one_membership, axis=1, session=session)

def build_grakn_graph(input_df, keyspace_name):
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace = keyspace_name) as session:
            load_data_into_grakn(session,input_df)

raw_data = read_csv('data/game-of-thrones-character-predictions.csv')
build_grakn_graph(raw_data, 'game_of_thrones')
