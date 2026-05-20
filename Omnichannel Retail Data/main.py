from __future__ import annotations

import argparse

from orchestration.pipeline_orchestrator import PipelineOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the end-to-end data pipeline.")
    parser.add_argument("--bucket-name", dest="bucket_name")
    parser.add_argument("--dataset-id", dest="dataset_id")
    parser.add_argument("--project-id", dest="project_id")
    parser.add_argument("--location")
    parser.add_argument("--start-date", dest="start_date")
    parser.add_argument("--end-date", dest="end_date")
    parser.add_argument(
        "--holiday",
        dest="holidays",
        action="append",
        default=None,
        help="Holiday date in YYYY-MM-DD format. Repeat the flag for multiple dates.",
    )
    parser.add_argument(
        "--write-disposition",
        dest="write_disposition",
        default="WRITE_TRUNCATE",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    orchestrator = PipelineOrchestrator(
        bucket_name=args.bucket_name,
        dataset_id=args.dataset_id,
        project_id=args.project_id,
        location=args.location,
        start_date=args.start_date,
        end_date=args.end_date,
        holidays=args.holidays,
        write_disposition=args.write_disposition,
    )
    orchestrator.run()


if __name__ == "__main__":
    main()
