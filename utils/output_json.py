import database_sqlite as database
import json


if __name__ == "__main__":
    data = {"nodes": database.get_nodes_json(), "edges": database.get_edges_json()}
    print(data)
    with open("./data/data.json", "w") as f:
        json.dump(data, f, indent=4)
