from grakn.client import GraknClient

def forming_ally(session):
    """
    Joining the characters in the same house as allies.
    """

    # write an insert query to create new relations using rule
    graql_insert_query = """
    define
    allies sub relation,
        relates ally1,
        relates ally2;
    join-allies sub rule,
    when {
        (member: $char1, organization: $house) isa membership;
        (member: $char2, organization: $house) isa membership;
        $char1 != $char2;
    }, then {
        (ally1: $char1, ally2: $char2) isa allies;
    };
    """

    with session.transaction().write() as transaction:
        # make a write transection with the query
        transaction.query(graql_insert_query)
        # remember to commit at the end
        transaction.commit()

def _convert_id_to_name(cluster, transaction):
    """
    Helper function to convert the cluster with ids to cluster with names.
    """
    new_cluster = set()
    for element in cluster:
        graql_query = f'match $char id {element}, has name $name; get $name;'
        iterator = transaction.query(graql_query)
        answers = iterator.collect_concepts()
        for answer in answers:
            new_cluster.add(answer.value())
    return new_cluster

def getting_biggest_group(session):
    """
    Finding the biggest group of related characters.
    """
    graql_query = f'compute cluster in [character, allies, marriage, parental], ' \
                  f'using connected-component;'
    with session.transaction().read() as transaction:
        # exicute the query and getting the clusters
        iterator = transaction.query(graql_query)
        result = [item.set() for item in iterator]

        # extracting the name of the characters in each clusters
        new_result = []
        for cluster in result:
            new_cluster = _convert_id_to_name(cluster, transaction)
            new_result.append(new_cluster)

        # finding the biggest group of people
        biggest_group = None
        max_size = 0
        for group in new_result:
            if len(group) > max_size:
                max_size = len(group)
                biggest_group = group

    return max_size, biggest_group

def getting_main_character(session):
    """
    Finding the character(s) that relate(s) to most other characters.
    """
    graql_query = f'compute centrality in [character, allies, marriage, parental], ' \
                  f'using degree;'
    with session.transaction().read() as transaction:
        # exicute the query and returning the answer
        iterator = transaction.query(graql_query)
        result = [(item.measurement(),item.set()) for item in iterator]

        # finding the biggest cluster
        biggest_cluster = None
        max_measure = 0
        for (measure,group) in result:
            if measure > max_measure:
                max_measure = measure
                biggest_cluster = group

        # finding the name of the characters
        main_characters = _convert_id_to_name(biggest_cluster, transaction)

    return max_measure, main_characters

### main part of the program ###

with GraknClient(uri="localhost:48555") as client:
    with client.session(keyspace = 'game_of_thrones') as session:
        # first forming allies if characters are in the same house
        forming_ally(session)

        # now we can answer some questions:
        print("What is the biggest group of friends and families?")
        max_size,biggest_group = getting_biggest_group(session)
        print(f'The biggest group is {biggest_group} with {max_size} members.')

        print() # extra line before next question

        print("Which character(s) relate(s) to most other characters?")
        max_measure, main_characters = getting_main_character(session)
        if len(main_characters) == 1:
            print(f'{list(main_characters)[0]} relates to the most, ' \
                  f'he/she related to {max_measure} characters')
        else:
            print(f'{main_characters} relate to the most, ' \
                  f'they all related to {max_measure} characters')

        # if there is only one most important character,
        # is that character in the biggest group?
        if len(main_characters) == 1:
            print() # extra line before next question
            print("Is he/she in the biggest group?")
            print(main_characters in biggest_group)
