import argparse

from app.api_checks import run_checks


def main() -> None:
    parser = argparse.ArgumentParser(description="Run API setup checks")
    parser.add_argument(
        "--step",
        choices=["telegram", "sheets", "reddit", "all"],
        default="all",
        help="Check a single API step or all",
    )
    args = parser.parse_args()
    failures = run_checks(args.step)
    if failures:
        raise SystemExit(1)
    print("All requested API checks passed.")


if __name__ == "__main__":
    main()


