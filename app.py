"""Thin local entry point; no packaging or service installation."""
import argparse
import json
import collector
import runtime


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    item = sub.add_parser("init")
    item.add_argument("root")
    item.add_argument("control")
    item = sub.add_parser("enqueue")
    item.add_argument("root")
    item.add_argument("task_id")
    item.add_argument("url")
    for name in ("run", "downloads"):
        sub.add_parser(name).add_argument("root")
    item = sub.add_parser("read")
    item.add_argument("root")
    item.add_argument("batch_id")
    args = parser.parse_args()
    if args.command == "init":
        runtime.init(args.root, args.control)
        collector.init(args.root)
        value = {"initialized": args.root}
    elif args.command == "enqueue":
        value = collector.enqueue(args.root, args.task_id, args.url)
    elif args.command == "run":
        value = {"processed": collector.run_one(args.root)}
    elif args.command == "downloads":
        value = collector.status(args.root)
    else:
        value = runtime.read(args.root, args.batch_id)
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
