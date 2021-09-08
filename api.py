#!/usr/bin/python3

import sys
import json
import psycopg2 as pc2

from argparse import ArgumentParser


def connect_to_database(data):
    conn_data = data["open"]
    conn = pc2.connect(
        database=conn_data["database"],
        user=conn_data["login"],
        password=conn_data["password"],
    )
    conn.autocommit = False
    return conn


def init_database(db_conn):
    with open("setup.psql", "r") as fh:
        db_cursor.execute(fh.read())
    db_conn.commit()


def leader(data, init=False):
    db_cursor.callproc(
        "leader", [data["timestamp"], data["password"], data["member"], init]
    )
    return {"status": "OK"}


def member(data):
    db_cursor.callproc("member", [data["timestamp"], data["password"], data["member"]])
    return {"status": "OK"}


def action(data, a_type):
    member(data)
    db_cursor.execute(f"SELECT * FROM Project WHERE project_id = {data['project']}")

    if db_cursor.fetchall() == []:
        db_cursor.execute(
            "INSERT INTO Project(project_id, authority) "
            f"VALUES({data['project']}, {data['authority']})"
        )

    db_cursor.execute(
        "INSERT INTO Action(action_id, action_type, project_id, member_id) "
        f"VALUES({data['action']}, '{a_type}'::varchar, "
        f"{data['project']}, {data['member']})"
    )
    return {"status": "OK"}


def support(data):
    return action(data, "support")


def protest(data):
    return action(data, "protest")


def vote(data, v_val):
    member(data)

    if v_val == 1:
        v_type = "upvotes"
    else:
        v_type = "downvotes"

    db_cursor.execute(f"SELECT * FROM Action WHERE action_id = {data['action']}")

    if db_cursor.fetchall() != []:
        db_cursor.execute(
            "INSERT INTO Vote(member_id, action_id, vote) "
            f"VALUES({data['member']}, {data['action']}, {v_val})"
        )
        db_cursor.execute(
            f"UPDATE Member SET {v_type} = {v_type} + 1 WHERE member_id = "
            f"(SELECT member_id FROM Action WHERE action_id = {data['action']})"
        )
        return {"status": "OK"}


def upvote(data):
    return vote(data, 1)


def downvote(data):
    return vote(data, -1)


def actions(data):
    leader(data)

    is_type = "type" in data
    is_project = "project" in data
    is_authority = "authority" in data

    q_and = "AND" if is_type and (is_project or is_authority) else ""
    q_where = "WHERE" if is_type or is_project or is_authority else ""
    q_type = f"action_type = '{data['type']}'" if is_type else ""

    q_spec = f"project_id = '{data['project']}'" if is_project else ""
    q_spec = f"authority = '{data['authority']}''" if is_authority else ""

    query = (
        "SELECT action_id, action_type, project_id, authority, "
        "(COUNT(vote) filter (where vote = 1) :: integer) as upvote, "
        "(COUNT(vote) filter (where vote = -1) :: integer) as downvote "
        "FROM action JOIN project USING(project_id) "
        "JOIN member USING(member_id) JOIN vote USING(action_id) "
        f"{q_where} {q_type} {q_and} {q_spec} "
        "GROUP BY action_id, action_type, project_id, authority "
        "ORDER BY action_id "
    )

    db_cursor.execute(query)
    return {"status": "OK", "data": db_cursor.fetchall()}


def projects(data):
    leader(data)
    query = "SELECT project_id, authority FROM Project ORDER BY project_id"

    if "authority" in data:
        query = "".join([query, f" WHERE authority = {data['authority']}"])

    db_cursor.execute(query)
    return {"status": "OK", "data": db_cursor.fetchall()}


def votes(data):
    leader(data)

    q_action = (f"WHERE action_id = {data['action']}") if "action" in data else ""
    q_project = (f"WHERE project_id = {data['project']}") if "project" in data else ""

    query = (
        "SELECT Member.member_id,"
        " COUNT(vote) filter (where vote = 1) as upvote,"
        " COUNT(vote) filter (where vote = -1) as downvote "
        " FROM Member LEFT JOIN Vote USING(member_id)"
        " LEFT JOIN Action USING(action_id)"
        f" {q_action} {q_project}"
        " GROUP BY Member.member_id"
        " ORDER BY Member.member_id"
    )
    db_cursor.execute(query)
    return {"status": "OK", "data": db_cursor.fetchall()}


def trolls(data):
    query = (
        "SELECT member_id, upvotes, downvotes, "
        f"CASE WHEN to_timestamp({data['timestamp']}) - last_act_time "
        "<= interval '1 year' "
        "THEN 'true' ELSE 'false' END AS is_active FROM Member "
        "WHERE downvotes > upvotes "
    )
    db_cursor.execute(query)
    return {"status": "OK", "data": db_cursor.fetchall()}


api_calls_map = {
    "leader": leader,
    "support": support,
    "protest": protest,
    "upvote": upvote,
    "downvote": downvote,
    "actions": actions,
    "projects": projects,
    "votes": votes,
    "trolls": trolls,
}

db_cursor = None


def main(args):
    ap = ArgumentParser(args)
    ap.add_argument("--init", "-i", action="store_true", help="initialize database")

    open_data = json.loads(input())
    try:
        db_conn = connect_to_database(open_data)

        global db_cursor
        db_cursor = db_conn.cursor()

        if ap.init:
            init_database(db_conn)
            api_calls_map['leader'] = lambda x: leader(x, init=True)

        print({"status": "OK"})
    except Exception as e:
        print({"status": "ERROR"})
        print(e)

    for json_call in sys.stdin:
        api_call = json.loads(json_call)
        fun_to_call = list(api_call)[0]
        result = {}
        try:
            result = api_calls_map[fun_to_call](api_call[fun_to_call])

            if "data" in result:
                result["data"] = list(map(list, result["data"]))

            if result["status"] == "OK":
                db_conn.commit()
            else:
                db_conn.rollback()
        except Exception as e:
            print(e)

            db_conn.rollback()
            result["status"] = "ERROR"
        print(result)


if __name__ == "__main__":
    main(sys.argv[1:])
