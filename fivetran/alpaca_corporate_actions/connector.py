"""ADD ONE LINE DESCRIPTION OF YOUR CONNECTOR HERE.
For example: This connector demonstrates a working starter template that syncs simple user data.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json
import datetime
# used to convert datetime for redundancy window

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Source specific libraries
from alpaca.data.historical.corporate_actions import CorporateActionsClient
from alpaca.data.requests import CorporateActionsRequest


"""
GUIDELINES TO FOLLOW WHILE WRITING A CONNECTOR:
- Import only the necessary modules and libraries to keep the code clean and efficient.
- Use clear, consistent and descriptive names for your functions and variables.
- For constants and global variables, use uppercase letters with underscores (e.g. CHECKPOINT_INTERVAL, TABLE_NAME).
- Keep constants as private by default, unless they are meant to be used outside the module (e.g. __CHECKPOINT_INTERVAL).
- Add comments to explain the purpose of each function in the docstring.
- Add comments to explain the purpose of complex logic within functions, wherever necessary. Ideally try to split the main logic into smaller functions to avoid too many comments.
- Add comments to highlight where users can make changes to the code to suit their specific use case.
- Split your code into smaller functions to improve readability and maintainability where required.
- Use logging to provide useful information about the connector's execution. Do not log excessively.
- Implement error handling to catch exceptions and log them appropriately. Catch specific exceptions where possible.
- Add retry for API requests to handle transient errors. Use exponential backoff strategy for retries.
- Define the complete data model with primary key and data types in the schema function.
- Ensure that the connector does not load all data into memory at once. This can cause memory overflow errors. Use pagination or streaming where possible.
- Add comments to explain pagination or streaming logic to help users understand how to handle large datasets.
- Add comments for upsert, update and delete to explain the purpose of upsert, update and delete. This will help users understand the upsert, update and delete processes.
- Checkpoint your state at regular intervals to ensure that the connector can resume from the last successful sync in case of interruptions.
- Add comments for checkpointing to explain the purpose of checkpoint. This will help users understand the checkpointing process.
- Refer to the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices)
"""


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    # No configuration required for this starter connector.
    # When building your own connector, uncomment and modify the example below:
    #
    required_configs = ["APCA-API-KEY-ID", "APCA-API-SECRET-KEY", "SYMBOLS"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
    pass


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    return [
        {
            "table": "raw_corporate_actions",  # Name of the table in the destination, required.
            "primary_key": [
                "id"
            ],  # Primary key column(s) for the table. We recommend defining a primary_key for each table. If not provided, fivetran computes _fivetran_id from all column values.
            "columns": {  # Definition of columns and their types, optional.
                "id": "STRING",  # Example: defining the id column type. You can define column types when needed.
                # For any columns whose names are not provided here, their data types will be inferred based on the data provided during upsert.
                # We recommend not defining all columns here to allow for schema evolution.
                "symbol": "STRING",
                "ca_type": "STRING",
                "process_date": "NAIVE_DATE",
                "payload": "JSON",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters as needed
    # TODO: Extract your configuration parameters here
    # Example: api_key = configuration.get("api_key")
    api_key = configuration.get("APCA-API-KEY-ID")
    api_secret_key = configuration.get("APCA-API-SECRET-KEY")
    symbols = configuration.get("SYMBOLS")

    # Get the state variable for the sync, if needed
    # This is useful for incremental syncs to keep track of the last synced record or timestamp.
    # For example, you might want to track the last updated timestamp to fetch only new or updated records since the last sync.
    # For the first sync, state will be empty JSON object: {}
    # You can modify this logic based on your specific use case.
    # For more information on state management, refer to: https://fivetran.com/docs/connector-sdk/working-with-connector-sdk#workingwithstatejsonfile

    today = datetime.date.today()
    last_synced = state.get("last_synced")
    if last_synced:
        # example "last_updated_at": "2024-01-01T00:00:00Z",
        # account for 1 week
        lookback_window = datetime.date.fromisoformat(last_synced) - datetime.timedelta(
            days=7
        )
    else:
        lookback_window = datetime.date(2023, 1, 1)

    try:
        # Fetch data from your source
        # TODO: Replace get_data() with your actual data fetching logic
        data = get_data(
            api_key=api_key,
            api_secret_key=api_secret_key,
            start=lookback_window,
            end=today,
            symbols=symbols.split(","),
        )

        records = flatten_corporate_actions(data)

        if not records:
            log.info("No new data to sync")
            return

        log.info(f"Processing {len(records)} record(s)")

        for record in records:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            payload = record.model_dump(mode="json")
            op.upsert(
                table="raw_corporate_actions",
                data={
                    "id": payload["id"],
                    "symbol": payload["symbol"],
                    "ca_type": payload["corporate_action_type"],
                    "process_date": payload["process_date"],
                    "payload": payload,
                },
            )

        # Update state with the current sync time for the next run
        new_state = {
            "last_synced": today.isoformat(),
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(new_state)
        log.info(f"Data synced successfully. Last updated at: {last_synced}")

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(e)}")


def get_data(api_key, api_secret_key, start, end, symbols):

    client = CorporateActionsClient(api_key=api_key, secret_key=api_secret_key)
    request = CorporateActionsRequest(symbols=symbols, start=start, end=end, limit=None)
    response = client.get_corporate_actions(request_params=request)

    return response


def flatten_corporate_actions(response):
    records = []
    for ca_type, items in response.data.items():
        records.extend(items)
    return records


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
