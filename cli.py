from flight import FlightDataHistorical, TidyHistorical
from loguru import logger
from pathlib import Path
import typer


app = typer.Typer(
    name="flight-data-processor",
    help="Process and clean historical flight data from CSV files",
    add_completion=False,
)


@app.command()
def batch(
        date_from: str = typer.Argument(
            "2025-02-11",
            help="First date (included) for the batch download in %Y-%m-%d format",
        ),
        date_to: str = typer.Argument(
            "2025-03-11",
            help="Last date (included) for the batch download in %Y-%m-%d format",
        ),
        airport_code: str = typer.Argument(
            "BOG",
            help="The airport code (e.g., 'JFK')",
        ),
        path: Path = typer.Argument(
            "/app/data/historical",
            help="Directory containing CSV files with flight data",
            exists=True,
            dir_okay=True,
            file_okay=False,
        ),
    ):  
    """
    Download batch data for historical fligth date
    """
    try:
        FlightDataHistorical.save_data_range(
              date_start = date_from, 
              date_end = date_to, 
              airport_code = airport_code, 
              path = path, 
              overwrite = True,
        )
        logger.info(
            f"Successfully downloaded flight data for {airport_code}",
        )

    except Exception as e:
        logger.error(f"Error downloading data: {e}")
        typer.Exit(code=1)



@app.command()
def update(
    airport_code: str = typer.Argument(
        "BOG",
        help="The airport code (e.g., 'JFK')",
    ),
    path: Path = typer.Argument(
        "/app/data/historical",
        help="Directory containing CSV files with flight data",
        exists=True,
        dir_okay=True,
        file_okay=False,
    ),
):
    """
    Download flight data from today to the last day stored
    """
    try:
        FlightDataHistorical.update(path=path, airport_code=airport_code)
        logger.info(
            f"Successfully downloaded flight data for {airport_code}",
        )

    except Exception as e:
        logger.error(f"Error downloading data: {e}")
        typer.Exit(code=1)


@app.command()
def process(
    input_path: Path = typer.Argument(
        "/app/data/historical",
        help="Directory containing CSV files with flight data",
        exists=True,
        dir_okay=True,
        file_okay=False,
    ),
    output_path: Path = typer.Argument(
        "/app/dbt/seeds/raw.csv",
        help="Path to save the processed data. If not provided, outputs to [input_path]/processed.csv",
    ),
) -> None:
    """
    Process historical flight data from CSV files.

    This command reads flight data from CSV files in the specified directory,
    cleans and standardizes the data, and outputs the processed data to a file.
    """
    try:
        # Process the data
        df = TidyHistorical.tidy(path=str(input_path))

        if df.empty:
            logger.warning(
                "No data was processed. Check logs for details.",
            )
            return

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save processed data
        df.to_csv(output_path, index=False)
        logger.info(
            f"Successfully processed data and saved to {output_path}",
        )

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        typer.Exit(code=1)


if __name__ == "__main__":
    app()
