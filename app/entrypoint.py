import os

from backfill_orders import run_backfill
from claims_sync import run_claims_sync
from sync_orders import main as sync_main


def main() -> None:
    command = os.getenv("SYNC_COMMAND", "").strip().lower()

    if command == "run_backfill":
        run_backfill()
        return

    if command == "run_claims_sync":
        run_claims_sync()
        return

    sync_main()


if __name__ == "__main__":
    main()