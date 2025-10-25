import argparse
from typing import Final

RACE_KEY_LEN: Final[int] = 8
HORSE_KEY_LEN: Final[int] = 8
BASE_YEAR: Final[int] = 2000
RACE_BASE_URL: Final[str] = "https://db.netkeiba.com/race/{race_id}/"
HORSE_BASE_URL: Final[str] = "https://db.netkeiba.com/horse/{horse_id}/"


def build_netkeiba_race_id(race_key: str) -> str:
    """Transform JRDB race key (8 chars) into netkeiba race id string.

    Expected race_key pattern: 8 alphanumeric characters where:
      - [0:2] race course code (digits)
      - [2:4] year offset from BASE_YEAR (digits)
      - [4] meeting number (digit)
      - [5] day number (digit)
      - [6:8] race number (digits)
    """
    if len(race_key) != RACE_KEY_LEN or not race_key.isalnum():
        raise ValueError(f"Invalid race_key (expect 8 alnum chars): {race_key}")
    race_course_code = race_key[0:2]
    year = BASE_YEAR + int(race_key[2:4])
    meeting_num = race_key[4].zfill(2)
    day_num = race_key[5].zfill(2)
    race_num = race_key[6:8]
    return f"{year}{race_course_code}{meeting_num}{day_num}{race_num}"


def build_race_url(race_key: str) -> str:
    return RACE_BASE_URL.format(race_id=build_netkeiba_race_id(race_key))


def build_netkeiba_horse_id(blood_reg_no: str) -> str:
    """Build netkeiba horse id from 血統登録番号 (bloodline registration number).

    Example: '04103318' -> '2004103318' (prepend '20' to make 10-digit id)
    """
    if len(blood_reg_no) != HORSE_KEY_LEN or not blood_reg_no.isdigit():
        raise ValueError(
            f"Invalid bloodline registration number (expect 8 digits): {blood_reg_no}"
        )
    return f"20{blood_reg_no}"


def build_horse_url(blood_reg_no: str) -> str:
    return HORSE_BASE_URL.format(horse_id=build_netkeiba_horse_id(blood_reg_no))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate netkeiba URLs. Use a subcommand: 'race' for JRDB race key -> race URL, 'horse' for 血統登録番号 -> horse URL."
        )
    )

    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Type of resource to build URL for."
    )

    race_parser = subparsers.add_parser(
        "race", help="Generate race URL from JRDB race key (8 alnum chars)."
    )
    race_parser.add_argument(
        "race_key", type=str, help="JRDB race key (8 alphanumeric characters)"
    )

    horse_parser = subparsers.add_parser(
        "horse", help="Generate horse URL from 血統登録番号 (8 digits)."
    )
    horse_parser.add_argument(
        "blood_reg_no",
        type=str,
        help="血統登録番号 (bloodline registration number, 8 digits)",
    )

    args = parser.parse_args()

    try:
        if args.command == "race":
            print(build_race_url(args.race_key))
        elif args.command == "horse":
            print(build_horse_url(args.blood_reg_no))
        else:
            parser.error("Unknown command")
    except ValueError as e:
        parser.error(str(e))


if __name__ == "__main__":
    main()
