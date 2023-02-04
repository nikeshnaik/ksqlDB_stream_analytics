import re
import json

def isolateMatchMetaData(event):

    row_record = {
        "match_type_number": event.get("info", {}).get("match_type_number", ""),
        "date": event.get("info", {}).get("dates", []),
        "city": event.get("info", {}).get("city", ""),
        "event_name":  event.get("info", {}).get("event", {}).get("name", ""),
        "gender": event.get("info", {}).get("gender", ""),
        "type": event.get("info", {}).get("match_type", ""),
        "match_referee": event.get("info", {}).get("officials", {}).get("match_referees", []),
        "match_umpire": event.get("info", {}).get("officials", {}).get("match_referees", []),
        "winner": event.get("info", {}).get("outcome", {}).get("winner", ""),
        "overs": 20,
        "season": event.get("info", {}).get("season", ""),
        "team": event.get("info", {}).get("teams", []),
        "toss": event.get("info", {}).get("toss", {}).get("decision", "") + event.get("info", {}).get("toss", {}).get("winner", ""),
        "venue": event.get("info", {}).get("venue", "")
    }

    return row_record



def flatten_list(key, array):

    result = {}
    for idx, each in enumerate(iterable=array, start=1):

        result[key + "_" + str(idx)] = each

    return result


def flatten_getRegistryPeople(key, array, people_registry):

    result = {}
    for idx, each in enumerate(array, 1 ):
        result[people_registry[each]] = each
        result["team"] = key


    return result





def isolatePlayersDetails(event):
    ## Todo take players data into one table
    team_1 = event.get("info", {}).get("teams", [])[0]
    team_2 = event.get("info", {}).get("teams", [])[1]

    row_record  = {
        "match_type_number": event.get("info", {}).get("match_type_number", None),
        **flatten_getRegistryPeople(team_1, event.get("info", {}).get("players", {}).get(team_1), event.get("info", {}).get("registry")["people"]),
        **flatten_getRegistryPeople(team_2, event.get("info", {}).get("players", {}).get(team_2),event.get("info", {}).get("registry")["people"] )
        
    }

    return row_record


# Todo Innings Isolate Details
def isolateOverDetails(event):

    team_1 = event.get("info", {}).get("teams", [])[0]
    team_2 = event.get("info", {}).get("teams", [])[1]

    row_records = []

    for inning in event.get("innings", []):

        if inning["team"] == team_1:

            team_1_overs = inning.get("overs", [])

            for each_over in team_1_overs:

                ball_no = 0
                for ball in each_over["deliveries"]:
                    ball_no = ball_no + 1
                    record = {
                            "team": team_1,
                            "over_no": each_over["over"],
                            "ball_no": ball_no,
                            "batter" : ball["batter"],
                            "bowler" : ball["bowler"],
                            "non_striker" : ball["non_striker"],
                            "runs" : ball["runs"]["batter"],
                            "extras": ball["runs"]["extras"],
                            "runs_in_over": ball["runs"]["total"]
                        }

                    row_records.append(record)

        elif inning["team"] == team_2:

            team_2_overs = inning.get("overs", [])

            for each_over in team_2_overs:

                ball_no = 0

                for ball in each_over["deliveries"]:

                    ball_no = ball_no + 1

                    record = {
                            "team": team_2,
                            "over_no": each_over["over"],
                            "ball_no": ball_no,
                            "batter" : ball["batter"],
                            "bowler" : ball["bowler"],
                            "non_striker" : ball["non_striker"],
                            "runs" : ball["runs"]["batter"],
                            "extras": ball["runs"]["extras"],
                            "runs_in_over": ball["runs"]["total"]
                        }

                    row_records.append(record)


    print("Len of row records: ", len(row_records))
    return row_records




def transform(event):

    meta_record = isolateMatchMetaData(event)
    print(meta_record)