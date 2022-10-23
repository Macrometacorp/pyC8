from __future__ import absolute_import, unicode_literals
import json
import base64
import threading
import time

from c8.exceptions import (
    StreamAppChangeActiveStateError,
    StreamAppGetSampleError
)
from tests.helpers import (
    assert_raises
)

def test_streamapp_methods(client):
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
    assert client.validate_stream_app(data=stream_app_definition) is True
    # Create a stream application
    client.create_stream_app(data=stream_app_definition)
    # Retrive a stream application
    client.retrieve_stream_app()
    # Get a stream application handle for advanced operations
    client.get_stream_app('Sample-Cargo-App')
    # Activate a stream application
    client.activate_stream_app('Sample-Cargo-App', True)
    # Deactivate a stream application
    client.activate_stream_app('Sample-Cargo-App', False)

    # To operate on created apps, you need to create an instance of the app
    app = client._fabric.stream_app("Sample-Cargo-App")

    # Update the app using
    updated_definition = """
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
    CREATE SOURCE SampleCargoAppInputTable WITH (type = 'database', collection = "SampleCargoAppInputTable", collection.type="doc", replication.type="global", map.type='json') (weight int);

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

    regions = []
    result = app.update(updated_definition, regions)
    time.sleep(5)
    app.change_state(True)

    insert_data_value = ("INSERT { \"weight\": @weight } IN SampleCargoAppInputTable")
    insert_data_query = {
        "query": {
            "name": "insertTestWeight",
            "value": insert_data_value,
        }
    }

    client.create_restql(insert_data_query)
    time.sleep(5)
    
    for i in range(10):
        client.execute_restql(
        "insertTestWeight", {"bindVars": {"weight": i}}
        )

    time.sleep(2)
    # Run query on application
    q = "select * from SampleCargoAppDestTable limit 3"
    result = app.query(q)
    assert result['records'] == [[0], [1], [2]]

    # Delete a stream application
    assert client.delete_stream_app('Sample-Cargo-App') is True
    # Get stream application samples
    client.get_stream_app_samples()

    assert client.delete_collection("SampleCargoAppDestTable") is True
    assert client.delete_collection("SampleCargoAppInputTable") is True
    assert client.delete_stream("c8locals.SampleCargoAppDestStream") is True
    assert client.delete_restql("insertTestWeight") is True

def test_streamapp_http_source(client):
    stream_app_definition = """
    @App:name('Sample-HTTP-Source')
    @App:description("This application how to receive POST requests via Stream Workers API.")
    @App:qlVersion('2')
    
    /**
    Testing the Stream Application:
        1. Open Stream `SampleHTTPOutputStream` in Console to monitor the output.
    
        2. Go to Stream Workers API and try `Publish message via HTTP-Source stream.` endpoint. Run it with
        application name set to `Sample-HTTP-Source`, stream name set to `SampleHTTPSource`, and body with the next data:
            {"carId":"c1","longitude":18.4334, "latitude":30.2123}
    
        3. This application read the carId, longitude and latitude from the `SampleHTTPSource` and sends it to
        sink stream `SampleHTTPOutputStream`
    **/
    
    -- Defines `SampleHTTPSource` stream to process events having `carId`, `longitude`, and `latitude`.
    CREATE SOURCE SampleHTTPSource WITH (type = 'http', map.type='json') (carId string, longitude double, latitude double);
    
    -- Defines `SampleHTTPOutputStream` to emit the events after the data is processed by external service
    CREATE SINK STREAM SampleHTTPOutputStream (carId string, longitude double, latitude double);
    
    -- Note: Consume data received from the external service
    @info(name = 'ConsumeProcessedData')
    INSERT INTO SampleHTTPOutputStream
    SELECT carId, longitude, latitude
    FROM SampleHTTPSource;
    """
    # Validate a stream application
    assert client.validate_stream_app(data=stream_app_definition) is True
    # Create a stream application
    client.create_stream_app(data=stream_app_definition)
    # Activate a stream application
    client.activate_stream_app('Sample-HTTP-Source', True)
    time.sleep(2)
    stream = client._fabric.stream()
    def create_reader():
        reader = stream.create_reader('SampleHTTPOutputStream', "earliest", local=True)
        m1 = json.loads(reader.recv())
        msg1 = json.loads(base64.b64decode(m1["payload"]).decode('utf-8'))
        assert msg1['carId'] == "c1"
        assert msg1['longitude'] == 18.4334
        assert msg1['latitude'] == 30.2123
        reader.close()

    reader_thread = threading.Thread(target=create_reader)
    reader_thread.start()
    client.publish_message_http_source('Sample-HTTP-Source', 'SampleHTTPSource', {'carId': 'c1', 'longitude': 18.4334, 'latitude': 30.2123})
    reader_thread.join()
    assert client.delete_stream_app('Sample-HTTP-Source') is True
    assert client.delete_stream('c8locals.SampleHTTPOutputStream') is True

def test_streamapp_exceptions(client, bad_fabric_name):
    #Test with bad definitions
    stream_app_definition = """
        name('Sample-Cargo-App')
        """
    # Validate a stream application
    assert client.validate_stream_app(data=stream_app_definition) is False

    # Create a stream application
    assert client.create_stream_app(data=stream_app_definition) is False

    #Tests with bad fabric
    bad_fabric = client._tenant.useFabric(bad_fabric_name)
    # Retrive a stream application
    assert bad_fabric.retrieve_stream_app() is False
    # Get a stream application handle for advanced operations
    assert client.get_stream_app('Sample-Cargo-App') is False
    # Activate a stream application
    with assert_raises(StreamAppChangeActiveStateError):
        client.activate_stream_app('Sample-Cargo-App', True)

    # To operate on created apps, you need to create an instance of the app
    app = client._fabric.stream_app("Sample-Cargo-App")

    # Update the app using
    updated_definition = """
        name('Sample-Cargo-App')

        """

    regions = []
    resp = app.update(updated_definition, regions)
    assert resp['error'] is True

    with assert_raises(StreamAppChangeActiveStateError):
        app.change_state(True)

    # Run query on application
    q = "select * from SampleCargoAppDestTable limit 3"
    assert app.query(q) is False

    # Delete a stream application
    assert client.delete_stream_app('Sample-Cargo-App') is False
    # Get stream application samples
    with assert_raises(StreamAppGetSampleError):
        client.get_stream_app_samples()
    assert client.publish_message_http_source('Sample-HTTP-Source', 'SampleHTTPSource', {'carId': 'c1', 'longitude': 18.4334, 'latitude': 30.2123}) is False

