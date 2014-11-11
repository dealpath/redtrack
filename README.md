redtrack
========
Infrastructure for tracking and loading events into Redshift using Kinesis as a data broker. For more information on motivation, design goals, and architecture, please see: 

# Installation / Dependencies

Add to Gemfile
```
gem 'redtrack', git: 'git://github.com/redhotlabs/redtrack.git'
```

Once installed, the library can be used by requiring it
```
require 'redtrack'
```

# Getting Started

You can call 

```ruby
redtrack_client = RedTrack::Client.new(redtrack_options)
data = {
  :message => "foo",
  :timestamp => Time.now.to_i
}
result = redtrack_client.write("test_events",data)
```

# Configuration
You need to pass a hash of configuration parameters to the constructor, eg,
```ruby
redtrack_options = {
  :PARAMETER_NAME => PARAMETER_VALUE
  ...
}
redtrack_client = RedTrack::Client.new(redtrack_options)
```

#### Configuration Parameters
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
```:redshift_schema``` Required. Hash. Schema definition for redshift. See section on "Redshift Schema"<br/>
```:kinesis_enabled``` Required. Bool. When "true", uses Kinesis for data broker. When "false", writes to a file as a broker instead of Kinesis (use that configuration for development only).<br/>

# Interface
There's 2 interfaces for Redtrack - Write and Loader. The gist is that the Write api is called inline with application logic and writes events to the broker and the Loader is called asynchronously by a recurring job to read events from the broker and load them into redshift. For an overview of the architecture, see: <INSERT LINK HERE>.

#### Write Api
You web application will interact with the Write API in-line with web transactions. Write will validate the passed data matches the redtrack schema (since the data is loaded asynchronously into redshift) and then write it to the .

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
```varchar(n)``` Supported. Current behavior is to truncate any strings that exceed the provided length<br/>
```
