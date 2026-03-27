import os

from backfill_orders import run_backfill
from sync_orders import main as sync_main


def main() -> None:
    command = os.getenv("SYNC_COMMAND", "").strip().lower()

    if command == "run_backfill":
        run_backfill()
        return

    sync_main()


if __name__ == "__main__":
    main()