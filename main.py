from py2neo import Graph, Node, Relationship

graph = Graph("bolt://localhost:7687", auth=("neo4j", "123"))

def importTeams2neo4j(filePath):
    with open(filePath, 'r') as file:
        next(file)
        for line in file:
            data = line.strip().split(';')
            teamId = int(data[0])
            team = Node("Team",
                        id=teamId, nazwa=data[1], sezon_rozgrywek=data[2],
                        gole_strzelone_w_domu=float(data[3]), gole_strzelone_na_wyjezdzie=float(data[4]),
                        gole_stracone_w_domu=float(data[5]), gole_stracone_na_wyjezdzie=float(data[6]),
                        zolte_kartki=float(data[7]), czerwone_kartki=float(data[8]), mecze_domowe=float(data[9]),
                        mecze_na_wyjezdzie=float(data[10]), srednia_goli_straconych_w_domu=float(data[11]),
                        srednia_goli_strzelonych_w_domu=float(data[12]), srednia_goli_straconych_na_wyjezdzie=float(data[13]),
                        srednia_goli_strzelonych_na_wyjezdzie=float(data[14]), wygrane_w_domu=float(data[15]),
                        wygrane_na_wyjezdzie=float(data[16]), przegrane_w_domu=float(data[17]),
                        przegrane_na_wyjezdzie=float(data[18]), remisy_w_domu=float(data[19]),
                        remisy_na_wyjezdzie=float(data[20]))
            graph.create(team)

def importPlayers2neo4j(filePath):
    with open(filePath, 'r', encoding='utf-8') as file:
        next(file)
        for line in file:
            data = line.strip().split(';')
            playerId = int(data[0])
            player = Node("Player",
                        id=playerId, fisrt_name=data[1], last_name=data[2],
                        season=data[4], age=float(data[5]), height=float(data[6]),
                        weight=float(data[7]), country=data[8], appearances=float(data[9]),
                        minutes=float(data[10]), position=data[11], rating=float(data[12]),
                        shots=float(data[13]), shots_on_target=float(data[14]), goals=float(data[15]),
                        passes=float(data[16]),  pass_accuracy=float(data[17]),
                        key_passes=float(data[18]), assists=float(data[19]), duels=float(data[20]),
                        duels_won=float(data[21]), interceptions=float(data[22]), successful_interceptions=float(data[23]),
                        dribbles=float(data[24]), successful_dribbles=float(data[25]), fouls_drawn=float(data[26]),
                        fouls_committed=float(data[27]), yellow_cards=float(data[28]), red_cards=float(data[29]),
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

