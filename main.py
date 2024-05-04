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
                        id=playerId, imie=data[1], nazwisko=data[2],
                        sezon=data[4], wiek=float(data[5]), wzrost=float(data[6]),
                        waga=float(data[7]), panstwo=data[8], wystepy=float(data[9]),
                        minuty=float(data[10]), pozycja=data[11], rating=float(data[12]),
                        strzaly=float(data[13]), strzaly_celne=float(data[14]), gole=float(data[15]),
                        podania=float(data[16]),  celnosc_podan=float(data[17]),
                        kluczowe_podania=float(data[18]), asysty=float(data[19]), pojedynki=float(data[20]),
                        wygrane_pojedynki=float(data[21]), przechwyty=float(data[22]), udane_przechwyty=float(data[23]),
                        dryblingi=float(data[24]), wygrane_dryblingi=float(data[25]), faule_popelnione=float(data[26]),
                        faule_na_zawodniku=float(data[27]), zolte_karti=float(data[28]), czerwone_kartki=float(data[29]),
                        czy_Kontuzjowany=data[30])
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

