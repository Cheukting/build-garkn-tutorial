# Building a Grakn Knowledge Graph for Game Of Throne

In this tutorial, we will use the character information from the Game of Throne to build a knowledge graph using [Grakn Client in Python](https://dev.grakn.ai/docs/client-api/python). The data that we use is originally form this [csv](https://github.com/ShirinG/blog_posts_prep/blob/master/GoT/character-predictions.csv). We are using Python >= 3.6.

For the advantage of using Grakn and what we can get out of this knowledge graph, please refer to this [blog post](#link-to-blog-post).

We assume you have already downloaded Grakn Core. To install Grakn Core, you can follow the instruction on their [GitHub](https://medium.com/r/?url=https%3A%2F%2Fgithub.com%2Fgraknlabs%2Fgrakn).

## Creating a Schema

#### Creating a gql file

Schema of the knowledge graph is defined in a Graql file (.gql), and in the file, we have to define some `relations` `entities` and `attributes`.

First let's define some `relation`s in `game-of-theones-schema.gql`:

```graql
define

  parental sub relation,
    relates parent,
    relates heir;

  marriage sub relation,
    relates partner1,
    relates partner2;

  membership sub relation,
    relates organization,
    relates member;
```
As you may guessed, `relation` is like a link between 2 things, for example, the `patental` `relation` link up `parent` and `heir`.

So now you may think, do we have to define `parent` and `heir` etc? The answer is yes, but through defining `entities`:

```graql
character sub entity,
  plays parent,
  plays heir,
  plays partner1,
  plays partner2,
  plays member,
  has name,
  has title,
  has gender,
  has culture,
  has age,
  has alive;

house sub entity,
  plays organization,
  has name;
```

Now there is some interesting things. Let's look at the `character` `entity`. It `plays` certain rows and we recognize some familiar names like `parent` and `heir`. So by defining what role this `entity` can play, we define what `relation` can be attached to this `entity`.

In `character`, we also see that it `has` many `attributes` like `name`, `title` and `gender`. The next step is to define those `attributes`, e.g. what `datatypes` they are:

```graql
name sub attribute,
  datatype string;
title sub attribute,
  datatype string;
gender sub attribute,
  datatype string;
culture sub attribute,
  datatype string;
age sub attribute,
  datatype long;
alive sub attribute,
  datatype boolean;
```

You can see `attribute` can be common `datatypes` such as `string`, `long` and `boolean`.

#### Loading in the schema

Now, start the grakn by:
```
grakn server start
```
then load in the schema by:
```
grakn console --keyspace game_of_thrones --file $(pwd)/game-of-theones-schema.gql
```
after, you can check what is loaded by using the grakn console:
```
grakn console --keyspace game_of_thrones
```
in the console, type:
```
match $x sub thing; get;
```

It should show

```graql
{$x type thing;}
{$x type relation;}
{$x type entity;}
{$x type attribute;}
{$x type membership sub relation;}
{$x type @has-attribute sub relation;}
{$x type parental sub relation;}
{$x type marrage sub relation;}
{$x type house sub entity;}
{$x type character sub entity;}
{$x type culture sub attribute;}
{$x type name sub attribute;}
{$x type gender sub attribute;}
{$x type alive sub attribute;}
{$x type title sub attribute;}
{$x type age sub attribute;}
{$x type @has-culture sub @has-attribute;}
{$x type @has-name sub @has-attribute;}
{$x type @has-age sub @has-attribute;}
{$x type @has-gender sub @has-attribute;}
{$x type @has-title sub @has-attribute;}
{$x type @has-alive sub @has-attribute;}
```

To know more about Grakn schema, you can refer to [official documentation](https://dev.grakn.ai/docs/schema/overview) on Grakn.ai

## From csv to Grakn

To load data from csv to Grakn programmatically using Python, we need to also install the `grakn-client` using pip:
```
pip install grakn-client==1.5.3
```
We will also use [pandas](https://pandas.pydata.org/pandas-docs/stable/index.html) to do some simple data manipulation for us:
```
pip install pandas==0.24.2
```
and use [tqdm](https://tqdm.github.io/) for the progress bar:
```
pip install tqdm==4.32.2
```

Now we are ready to go, first, lets import the libraries in `csv-to-grakn.py`:

```python
from grakn.client import GraknClient
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
```

First of all, we will have to read the data from csv into a pandas data frame:
```python
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
```
Notice that we are not using all columns in the csv so we only select the columns that we are interested. Also, we have to do some data cleaning:
1. renaming column `name` to `char_name` as `name` is a reserved attribute for pandas DataFrame.
2. filling `na`s in age with -1
3. filling the rest of the `na`s to be an empty string.

Then we have to think about what we have to load in the graph. Remeber our schema? We have 2 entities `character` and `house`, and 3 relations `parental`, `marriage` and `membership`. So when we load in the data, it will be more or less like this:

```python
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
    print("Inserting marriage...")
    input_df.progress_apply(insert_one_marriage, axis=1, session=session)
    print("Inserting membership...")
    input_df.progress_apply(insert_one_membership, axis=1, session=session)
```
To explain:
1. progress_apply is the same as [pandas apply](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.apply.html), just a tqdm version with the progress bar.
2. the `sessions` that got passed around is the session connecting to the grakn graph, it is needed to make the queries. You will see it being using when we define the functions.
3. All but `house` are loaded in row by row by using apply.
4. For `house`,  we will just use the `unique` method in pandas to get a list of `house`s and load them in with a for-loop
5. We have to load things in order, the entities first than relations.

Let's see how each functions for loading in one item are defined:

#### character

```python
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
```

It has 2 main parts:

1. Composing the query - incluing the data manipulation at the beginning, and
2. passing the query using the client

[*documentation for how to make an insert query using graql*](https://dev.grakn.ai/docs/query/insert-query)

#### House

```python
def insert_one_house(house,session):
    with session.transaction().write() as transaction:
        transaction.query(f'insert $house isa house, ' \
                          f'has name "{house}";')
        transaction.commit()
```

It is much simpler than `character` but the idea is the same.

#### Marriage

```python
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
```
For relations, we have to be sure the relation exist so there's the check at the beginning. Also, the query is a bit different, it consist of a [match](https://dev.grakn.ai/docs/query/match-clause) then insert.

#### Membership

```python
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
```

#### Parental

```python
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
```

For `parental` it is more complicated than `marriage` and `membership`, as for each row of the data frame, it may have 3 relations: father and character, mother and character, and character and heir. So this function is a bit like a 3-in-1.

Finally, we use a function to wrap up loading data into grakn:

```python
def build_grakn_graph(input_df, keyspace_name):
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace = keyspace_name) as session:
            load_data_into_grakn(session,input_df)
```

and in the end, we run 2 lines of code to do the job:

```python
raw_data = read_csv('data/game-of-thrones-character-predictions.csv')
build_grakn_graph(raw_data, 'game_of_thrones')
```

We put the csv in a directory called `data`, you may change the path to where you put your csv.

Now the Python script is ready (you may have a look at the final product [here](#link-to-py-file-on-github)), run the script by:

```
python csv-to-grakn.py
```

After loading all the data, we can start making graql queries to answer all the questions we have about Game of Thrones. (ok, maybe not all questions but a lot of questions)

## Simple Graql Query

Actually this session should deserve another tutorial for that, but to test out our work for this tutorial, we will demonstrate some simple use for grakn and graql queries. For more about what Grakn can do, please refer to this [article](#link-to-other-blog-or-tutorial)

#### Find all members of the Night's Watch

```graql
match $char isa character, has name $char_name; $house isa house, has name "Night's Watch"; $mem (organization:$house, member:$char) isa membership; get $char_name;
```

#### Find all the father and son pairs

```graql
match $father isa character, has name $fa_name, has gender 'male'; $son isa character,has name $son_name, has gender 'male'; $fa_son (parent:$father, heir:$son) isa parental; get $fa_name, $son_name;
```
