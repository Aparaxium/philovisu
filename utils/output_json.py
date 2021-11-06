import utils.database_sqlite as database_sqlite
import json
import sys


def main(*args):
    if len(args) != 2:
        print("Usage: output_json.py <database_file> <output_file>")
        sys.exit(1)

    db_file = args[0]
    output_file = args[1]

    db = database_sqlite.database(db_file)

    nodes = db.get_nodes()
    edges = db.get_edges()

    nodes_f = list(map(lambda x: {"id": x[0], "val": x[1]}, nodes))
    edges_f = list(map(lambda x: {"source": x[0], "target": x[1]}, edges))

    data = {"nodes": nodes_f, "edges": edges_f}

    # Open the output file
    with open(output_file, "w") as f:
        # Convert the data to JSON and write it to the output file
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    main(sys.argv[1:])
