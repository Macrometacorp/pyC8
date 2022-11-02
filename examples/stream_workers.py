import time

from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(protocol="https", host="gdn.paas.macrometa.io", port=443, apikey="")


stream_app_definition = """
    @App:name('Sample-Cargo-App')
    @App:qlVersion("2")
    @App:description('Basic stream application to demonstrate reading data from input stream and store it in the collection. The stream and collections are automatically created if they do not already exist.')
    /**
    Testing the Stream Application:
        1. Open Stream SampleCargoAppDestStream in Console. The output can be monitored here.
        2. Upload following data into SampleCargoAppInputTable C8DB Collection
            {"weight": 1}
            {"weight": 2}
            {"weight": 3}
            {"weight": 4}
            {"weight": 5}
        3. Following messages would be shown on the SampleCargoAppDestStream Stream Console
            [2021-08-27T14:12:15.795Z] {"weight":1}
            [2021-08-27T14:12:15.799Z] {"weight":2}
            [2021-08-27T14:12:15.805Z] {"weight":3}
            [2021-08-27T14:12:15.809Z] {"weight":4}
            [2021-08-27T14:12:15.814Z] {"weight":5}
    */

    -- Create Table SampleCargoAppInputTable to process events.
    CREATE SOURCE SampleCargoAppInputTable WITH (type = 'database', collection = "SampleCargoAppInputTable", collection.type="doc", replication.type="global", map.type='json') (weight int);

    -- Create Stream SampleCargoAppDestStream
    CREATE SINK SampleCargoAppDestStream WITH (type = 'stream', stream = "SampleCargoAppDestStream", replication.type="local") (weight int);

    -- Data Processing
    @info(name='Query')
    INSERT INTO SampleCargoAppDestStream
    SELECT weight
    FROM SampleCargoAppInputTable;
    """
# Validate a stream application
client.validate_stream_app(data=stream_app_definition)

regions = []
client.create_stream_app(data=stream_app_definition, dclist=regions)


# Activate a stream application
client.activate_stream_app(streamapp_name="Sample-Cargo-App", activate=True)
# Deactivate a stream application
client.activate_stream_app(streamapp_name="Sample-Cargo-App", activate=False)

client.retrieve_stream_app()

# To operate on created apps, you need to create an instance of the app
app = client._fabric.stream_app("Sample-Cargo-App")

# Update the app using
updated_definition = """
@App:name('Sample-Cargo-App')
@App:qlVersion("2")
@App:description('Basic stream application to demonstrate reading data from input stream and store it in the collection. Thestream and collections are automatically created if they do not already exist.')
/**
    Testing the Stream Application:
    1. Open Stream SampleCargoAppDestStream in Console. The output can be monitored here.
    2. Upload following data into SampleCargoAppInputTable C8DB Collection
        {"weight": 1}
        {"weight": 2}
        {"weight": 3}
        {"weight": 4}
        {"weight": 5}
    3. Following messages would be shown on the `SampleCargoAppDestStream` Stream Console.
        [2021-08-27T14:12:15.795Z] {"weight":1}
        [2021-08-27T14:12:15.799Z] {"weight":2}
        [2021-08-27T14:12:15.805Z] {"weight":3}
        [2021-08-27T14:12:15.809Z] {"weight":4}
        [2021-08-27T14:12:15.814Z] {"weight":5}
    4. Following messages would be stored into SampleCargoAppDestTable
        {"weight":1}
        {"weight":2}
        {"weight":3}
        {"weight":4}
        {"weight":5}
*/
-- Defines Table SampleCargoAppInputTable
CREATE SOURCE SampleCargoAppInputTable WITH (type = 'database', collection = "SampleCargoAppInputTable", collection.type="doc",replication.type="global", map.type='json') (weight int);

-- Define Stream SampleCargoAppDestStream
CREATE SINK SampleCargoAppDestStream WITH (type = 'stream', stream = "SampleCargoAppDestStream", replication.type="local") (weight int);
-- Defining a Destination table to dump the data from the stream
CREATE STORE SampleCargoAppDestTable WITH (type = 'database', stream = "SampleCargoAppDestTable") (weight int);

-- Data Processing
@info(name='Query')
INSERT INTO SampleCargoAppDestStream
SELECT weight
FROM SampleCargoAppInputTable;

-- Data Processing
@info(name='Dump')
INSERT INTO SampleCargoAppDestTable
SELECT weight
FROM SampleCargoAppInputTable;
"""

# Updating existing Stream Workers, if no region is specified, the nearest one is selected
regions = []
app.update(data=updated_definition, dclist=regions)
# Update needs some wait time
time.sleep(5)
# Activate the updated stream worker
app.change_state(active=True)

# Get stream application samples
client.get_stream_app_samples()

# Delete a stream application
client.delete_stream_app(streamapp_name="Sample-Cargo-App")
