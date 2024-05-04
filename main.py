from py2neo import Graph, Node, Relationship

graph = Graph("bolt://localhost:7687", auth=("neo4j", "123"))

def importTeams2neo4j(filePath):
    with open(filePath, 'r') as file:
        next(file)
        for line in file:
            data = line.strip().split(';')
            teamId = int(data[0])
            node = Node("Team",
                        id=teamId, nazwa=data[1], sezon_rozgrywek=data[2],
                        gole_strzelone_w_domu=data[3], gole_strzelone_na_wyjezdzie=data[4],
                        gole_stracone_w_domu=data[5], gole_stracone_na_wyjezdzie=data[6],
                        zolte_kartki=data[7], czerwone_kartki=data[8], mecze_domowe=data[9],
                        mecze_na_wyjezdzie=data[10], srednia_goli_straconych_w_domu=data[11],
                        srednia_goli_strzelonych_w_domu=data[12], srednia_goli_straconych_na_wyjezdzie=data[13],
                        srednia_goli_strzelonych_na_wyjezdzie=data[14], wygrane_w_domu=data[15],
                        wygrane_na_wyjezdzie=data[16], przegrane_w_domu=data[17],
                        przegrane_na_wyjezdzie=data[18], remisy_w_domu=data[19],
                        remisy_na_wyjezdzie=data[20])
            graph.create(node)

def importPlayers2neo4j(filePath):
    with open(filePath, 'r', encoding='utf-8') as file:
        next(file)
        for line in file:
            data = line.strip().split(';')
            playerId = int(data[0])
            player = Node("Player",
                        id=playerId, fisrt_name=data[1], last_name=data[2],
                        season=data[4], age=data[5], height=data[6],
                        weight=data[7], country=data[8], appearances=data[9],
                        minutes=data[10], position=data[11], rating=data[12],
                        shots=data[13], shots_on_target=data[14], goals=data[15],
                        passes=data[16],  pass_accuracy=data[17],
                        key_passes=data[18], assists=data[19], duels=data[20],
                        duels_won=data[21], interceptions=data[22], successful_interceptions=data[23],
                        dribbles=data[24], successful_dribbles=data[25], fouls_drawn=data[26],
                        fouls_committed=data[27], yellow_cards=data[28], red_cards=data[29],
                        injured=data[30])
            graph.create(player)

            teamId = int(data[3])
            team = graph.nodes.match("Team", id=teamId).first()

            if team:
                relationship = Relationship(player, "Gra_dla", team)
                graph.create(relationship)
            else:
                print(f"Błąd, drużyna o id = {teamId} nie istnieje")

csvTeamPath = "druzyna.csv"
csvPlayersPath = "pilka.csv"

importTeams2neo4j(csvTeamPath)
importPlayers2neo4j(csvPlayersPath)

