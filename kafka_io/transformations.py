

def isolateMatchMetaData(event):

    row_record = {
        "match_type_number": event.get("info", {}).get("match_type_number", None),
        "date": event.get("info", {}).get("balls_per_over", None),
        "city": event.get("info", {}).get("dates", None).joins(" | "),
        "event_name": event.get("info", {}).get("event", {}).get("name", None),
        "gender": event.get("info", {}).get("gender", None),
        "type": event.get("info", {}).get("match_type", None),
        **flatten_list("match_referee", event.get("info", {}).get("officials", {}).get("match_referees", [])),
        **flatten_list("match_umpire", event.get("info", {}).get("officials", {}).get("match_umpires", [])),
        "outcome": event.get("info", {}).get("outcome", None),
        "overs": 20,
        "season": event.get("info", {}).get("season", None),
        **flatten_list("team", event.get("info", {}).get("teams", [])),
        "toss": event.get("info", {}).get("toss", {}).get("decision", "") + event.get("info", {}).get("toss", {}).get("winner", ""),
        "venue": event.get("info", {}).get("venue", None)
    }

    return row_record


def flatten_list(key, array):

    result = {}
    for idx, each in enumerate(1, array):
        result[key + "_" + idx] = each

    return result


def isolatePlayersDetails(event):
    ## Todo take players data into one table

    row_record  = {
        "match_type_number": event.get("info", {}).get("match_type_number", None),


    }


# Todo Innings Isolate Details
def isolateOverDetails(event):
    pass