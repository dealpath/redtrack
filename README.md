redtrack
========
Infrastructure for tracking and loading events into Redshift using Kinesis as a data broker. For more information on motivation, design goals, and architecture, please see this blog post: 

# Installation / Dependencies

Add to Gemfile
```
gem 'redtrack', git: 'git://github.com/redhotlabs/redtrack.git'
```

Once installed, the library can be used by requiring it
```
require 'redtrack'
```

You need a Redshift cluster. If you don't have one, launch one starting here: [https://console.aws.amazon.com/redshift/home](Redshift AWS console)

# Getting Started 

There is a full application example showing usage here: https://github.com/lrajlich/sinatra_example

Redtrack is generally used through the client object. In order to get started, you need to configure & create a redtrack client, ensure you have the proper AWS resources provisioned & configured, and then you can call the APIs.

### RedTrack client constructor 
To construct a client object, you need to pass a hash of configuration parameters to its constructor
```ruby
redtrack_options = {
  :PARAMETER_NAME => PARAMETER_VALUE
  ...
}
redtrack_client = RedTrack::Client.new(redtrack_options)
...
```

##### Constructor options
```:access_key_id``` Required. String. Passed to the aws-sdk https://github.com/aws/aws-sdk-ruby<br/>
```:secret_access_key``` Required. String. Passed to the aws-sdk https://github.com/aws/aws-sdk-ruby<br/>
```:s3_bucket``` Required. String. Name of the bucket to store file uploads. Must be in same region as Redshift cluster.<br/>
```:region``` Required. String. AWS region. Passed to aws-sdk.<br/>
```:redshift_cluster_name``` Required. String. Fill in Name of the redshift cluster from redshift cluster configuration<br/>
```:redshift_host``` Required. String. This is the Endpoint under Cluster Database Properties on redshift cluster configuration<br/>
```:redshift_port``` Required. String. Port under Cluster Database Properties on redshift cluster configuration. Default is 5439<br/>
```:redshift_dbname``` Required. String. Database Name under Cluster Database Properties on redshift cluster configuration<br/>
```:redshift_user``` Required. String. Master Username under Cluster Database Properties on redshift cluster configuration<br/>
```:redshift_password``` Required. String. Password used for the above user<br/>
```:redshift_schema``` Required. Hash. Schema definition for redshift. For more information, see [https://github.com/redhotlabs/redtrack#redshift-schema](Redshift Schema section)<br/>
```:kinesis_enabled``` Required. Bool. When "true", uses Kinesis for data broker. When "false", writes to a file as a broker instead of Kinesis (use that configuration for development only).<br/>

For an example / template configuration, see [https://github.com/lrajlich/sinatra_example/blob/master/configuration.rb](example configuration)

### Creating AWS resources

RedTrack depends on a number of AWS resources to be provisioned and configured. These are:

1) Redshift cluster - This has to be done manually via the [https://console.aws.amazon.com/redshift/home](Redshift AWS console)

2) Redshift Database - You have to make sure the configuration parameter ```redshift_dbname``` has a corresponding database in redshift, otherwise loading events will fail. By default, your Redshift Cluster will have a database when you create the cluster. You can create additional databases.  

3) Redshift Tables - For every table in your schema, you need to make sure there is a Redshift table with the same name; otherwise, loading events will fail. RedTrack client provides a helper method for creating these tables:
```
redtrack_client.create_table_from_schema('SOME_TABLE_NAME')
```

An example usage can be seen here: [https://github.com/lrajlich/sinatra_example/blob/master/setup_redtrack_aws_resources.rb#L12](Create table example)

4) Kinesis Streams -For every table in your schema, you need to make sure there is a Kinesis stream that has a name like ```<redshift_cluster_name>.<redshift_db_name>.<table_name>``` RedTrack provides a helper method for creating these streams:
```
redtrack_client.create_kinesis_stream_for_table('SOME_TABLE_NAME')
```

An example usage can be seen here: [https://github.com/lrajlich/sinatra_example/blob/master/setup_redtrack_aws_resources.rb#L26](Create kinesis stream exampe)

5) Tracking Tables - The final component is that RedTrack keeps some internal state for tracking what events have been loaded. The ```kinesis_loads``` table has to exist in the database that you are loading. Like the above, there is a helper method for creating this table:
```
redtrack_client.create_kinesis_loads_table()
```

An example usage can be seen here: [https://github.com/lrajlich/sinatra_example/blob/master/setup_redtrack_aws_resources.rb#L19](Create kinesis table example)

For an example

# Interface
There's 2 interfaces for Redtrack - Write and Loader. The gist is that the Write api is called inline with application logic and writes events to the broker and the Loader is called asynchronously by a recurring job to read events from the broker and load them into redshift. For an overview of the architecture, see: <INSERT LINK HERE>.

#### Write Api
You web application will interact with the Write API in-line with web transactions. Write will validate the passed data matches the redtrack schema (since the data is loaded asynchronously into redshift) and then write it to the .

A simple example:
```ruby
redtrack_client = RedTrack::Client.new(redtrack_options)
data = {
  :message => "foo",
  :timestamp => Time.now.to_i
}
result = redtrack_client.write("test_events",data)
```

#### Loader
Run the loader periodically to drain the broker of events and then load them into 

# Redshift Schema
One of the features of redtrack is the ability to pass in a schema matching table schema. Redtrack can validate that passed events match the schema, as well, it can generate a SQL statements to create a table matching that schema or create the table directly.

In order to pass schema, you pass in a hash like this:
```ruby
SCHEMA = {
  :SOME_TABLE_NAME => {
    :columns => {
      :SOME_COLUMN_NAME => {
        :type => 'varchar(32)',
        :constraint => 'not null'
      },
      .... (OTHER COLUMNS)
    },
    :sortkey => 'SOME_COLUMN_NAME',
    :distkey => 'SOME_COLUMN_NAME'
  },
  ... (OTHER TABLES)
}
```

A simple example looks like this:
```ruby
SCHEMAS= {
    :test_events => {
        :columns => {
            :client_ip =>     { :type => 'varchar(32)', :constraint => 'not null'},
            :timestamp =>     { :type => 'integer', :constraint => 'not null'},
            :message =>       { :type => 'varchar(128)' }
        },
        :sortkey => 'timestamp'
    }
}
```

#### Redshift Type Support

Since Redtrack does asynchronous loading of events, the events are filtered before they are written to the broker in order to avoid COPY errors and to provide direct feedback to the caller of the ```write``` function

```varchar(n)``` Supported. Current behavior is to truncate any strings that exceed the provided length<br/>
```char``` Supported. <br/>
```smallint``` Supported. <br/>
```bigint``` Supported. <br/>
```timestamp``` Partially Supported. Not all time formats are supported. Timeformat for Redshift is very restrictive (simply checking for a valid Ruby time is not sufficient) and thus this is done via string matching. [Documentation](http://docs.aws.amazon.com/redshift/latest/dg/r_DATEFORMAT_and_TIMEFORMAT_strings.html)<br/>
```decimal``` Supported. Checks that the value is a numeric, eg, converts to float.

Redtrack type filtering is done here and contributions to filtering logic are welcome: 

# Documentation / Further reading

Redshift supports a handful of types. [Redshift Types](http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html)
