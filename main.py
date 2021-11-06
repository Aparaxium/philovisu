import argparse
import utils.output_json as output_json
import utils.database_sqlite as database_sqlite
import utils.scrape_sqlite as scrape_sqlite

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A tutorial of argparse!")

    parser.add_argument("-d", default="./data/", type=str, help="The path of the data")
    parser.add_argument(
        "-df", default="database.db", type=str, help="Database filename"
    )
    parser.add_argument("-c", action="store_true", help="Create a new database")
    parser.add_argument("-p", action="store_true", help="Populate the database")
    parser.add_argument("-j", action="store_true", help="Output Json from database")
    parser.add_argument("-jf", default="data.json", type=str, help="Json filename")

    # parser.add_argument("-s", action="store_true", help="Use sqlite3")
    # parser.add_argument("-p", action="store_true", help="Use postgresql")

    args = parser.parse_args()
    db = None
    sc = None
    print("Using sqlite3")
    if args.c:
        print(f"Creating a new database at {args.d}{args.df}")
        db = database_sqlite.database(args.d + args.df)
        db.create_database()
    else:
        print(f"Using database at {args.d}{args.df}")
        db = database_sqlite.database(args.d + args.df)
    if args.c:
        print("Populating database...")
        sc = scrape_sqlite.scraper(db)
        sc.run()
    if args.j:
        print("Outputing json...")
        output_json.main(args.d + args.df, args.d + args.jf)
