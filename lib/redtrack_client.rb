# The Client provides an application interface for redtrack
#
# Copyright (c) 2014 RedHotLabs, Inc.
# Licensed under the MIT License

module RedTrack
  class Client

    TAG='RedTrack::Client'

    @broker = nil
    @redshift_conn = nil
    @options = nil
    @data_types = nil
    @valid_data_types = nil

    @logger = nil

    # Constructor for the client - initialize instance variables
    #
    # @param [Hash] options Options to the client - see README.md
    def initialize(options)

      # Create logger and add to options (passed to other objects)
      @logger = Logger.new(STDOUT)
      options[:logger] = @logger

      # Create the appropriate broker
      if options[:kinesis_enabled] == true
        @logger.debug("#{TAG} Kinesis enabled. create KinesisClient")
        @broker = RedTrack::KinesisClient.new(options)
      else
        @logger.debug("#{TAG} Kinesis disabled. create FileClient")
        @broker = RedTrack::FileClient.new(options)
      end

      # Bind to the interface for checking data types
      @data_types = RedTrack::DataTypes.new(options)
      @valid_data_types = @data_types.valid_data_types

      aws_options = {
          :access_key_id => options[:access_key_id],
          :secret_access_key => options[:secret_access_key],
          :region => options[:region]
      }
      AWS.config(aws_options)

      @options = options
    end


    # Create a new loader client
    #
    # @param [Hash] loader_options The options to pass to the loader
    # @return [RedTrack::Loader] The loader client
    def new_loader(loader_options={})
      merged_options = merge_options(loader_options)

      if @redshift_conn == nil
        @redshift_conn = new_redshift_connection(loader_options)
      end

      return RedTrack::Loader.new(merged_options,@broker,@redshift_conn)
    end

    # Create a new redshift connection
    #
    # @param [Hash] connection_options A set of options to pass to PG.connect. Uses options passed to redtrack client by default
    # @return [PG::Connection] Postgres client connection
    def new_redshift_connection(connection_options={})
      merged_options = merge_options(connection_options)

      @redshift_conn = PG.connect(
        :host => merged_options[:redshift_host],
        :port => merged_options[:redshift_port],
        :dbname => merged_options[:redshift_dbname],
        :user => merged_options[:redshift_user],
        :password => merged_options[:redshift_password])

      return @redshift_conn
    end

    # Check the data to ensure it conforms to the table schema and write to the databroker for the table.
    # Determines which shard to write to randomly
    #
    # @param [String] table The name of the redshift table to write to
    # @param [Hash] data hash containing data to write to the table. Key is column name
    # @param [String] partition_key optional, used to determine which kinesis shard to write the data to
    # @return [Boolean] Whether or not the write succeeded
    def write(table,data,partition_key=nil)

      ## Get table schema...
      schema = get_table_schema(table)

      if schema == nil
        raise "Scheme does not exist for table name ='#{table}'"
      end

      ## Ensure that the keys in the passed data are symbols (this is what's expected)
      data.keys.each do |key|
        if(key.is_a?(Symbol) == false)
          raise "Data key #{key} is not a symbol!"
          # TODO: CONVERT string keys to symbols instead of raising
        end
      end

      intersection = schema[:columns].keys & data.keys

      ## Validate no data keys are passed that are not in table schema
      data.keys.each do |key|
        if(intersection.include?(key) == false)
          raise "Data key #{key} is not in schema for #{table} table!!"
        end
      end

      ## Validate that columns are not null
      schema[:columns].each do |column_name,column|
        if(column.keys.include?(:constraint) == true && column[:constraint] == "not null" && intersection.include?(column_name) == false)
          raise "Column #{column_name} is missing from passed data"
        end
      end

      ## Validate column types
      schema[:columns].each do |column_name,column|
        if(intersection.include?(column_name) == true)

          value = data[column_name.to_sym]
          column_type = column[:type]

          if column_type['('] != nil
            type_name = column_type[/(.*)\(.*/,1]
          else
            type_name = column_type
          end

          type_name_downcased = type_name.downcase

          if @valid_data_types.include? type_name_downcased
            type_name_check_function = "check_#{type_name_downcased.gsub(' ','_')}".to_sym
            data[column_name.to_sym] = @data_types.send(type_name_check_function,value,column_type,column_name)
          else
            raise "Invalid data type #{type_name}. Valid types [#{@valid_data_types.join(",")}]"
          end
        end
      end

      ## Serialize as json, we load the data as JSON into redshift
      data_string=data.to_json

      ## Write the serialized data string to the broker
      partition_key = partition_key || rand(100).to_s
      stream_name = @broker.stream_name(table)
      result = @broker.stream_write(stream_name, data_string, partition_key)

      return result
    end

    # Gets a schema hash object for a specific table
    #
    # @param [String] table The name of the redshift table
    # @return [Hash] Hash object containing the column definitions
    def get_table_schema(table)
      if (@options[:redshift_schema] == nil)
        raise 'Must pass :redshift_schema as option when creating RedTrack client'
      end

      schema = @options[:redshift_schema]

      if schema[table.to_sym]
        result = schema[table.to_sym]
      elsif schema["#{table}"]
        result = schema["#{table}"]
      end

      return result
    end


    # Returns a SQL statement for creating a Redshift per the defined schema above
    #
    # @param [String] table The name of the table
    # @param [Boolean] exec Whether to execute the statement
    # @param [Hash] schema The table schema to use - if not provided, get from passed schema
    # @return [String] Returns the create table string
    def create_table_from_schema(table,exec=true,schema=nil)

      if schema == nil
        schema = get_table_schema(table)
        if !schema
          @logger.warn("#{TAG} No schema exists for table #{table}")
          return false
        end
      end

      query = "create table #{table} (\n"
      schema[:columns].each_with_index do |(column_name,column),index|

        query += "#{column_name} " + column[:type]
        if column[:constraint] != nil
          query += " " + column[:constraint]
        end
        if index !=  schema[:columns].size - 1
          query += ","
        end
        query += "\n"
      end
      query += ")"

      # Add table attributes
      if schema[:distkey] != nil
        query += "\ndistkey(#{schema[:distkey]})"
      end
      if schema[:sortkey] != nil
        query += "\nsortkey(#{schema[:sortkey]})"
      end

      query += ";\n"

      if exec
        conn = new_redshift_connection()
        result = conn.exec(query)
      else
        result = query
      end

      return result
    end

    # Create kinesis loads table
    #
    # @param[Boolean] exec Whether to exec the query
    # @return [String] Executes query against redshift and returns the result
    def create_kinesis_loads_table(exec=true)
      schema= {
        :columns => {
          :stream_name =>                   { :type => 'varchar(64)' },
          :shard_id =>                      { :type => 'varchar(64)' },
          :table_name =>                    { :type => 'varchar(64)' },
          :starting_sequence_number =>      { :type => 'varchar(64)' },
          :ending_sequence_number =>        { :type => 'varchar(64)' },
          :load_timestamp =>                { :type => 'timestamp', :constraint => 'not null' }
        },
        :sortkey => 'load_timestamp'
      }

      return create_table_from_schema('kinesis_loads',exec,schema)
    end

    # Create a kinesis stream for the table - use configuration
    #
    # @param [String] table The name of the table
    # @param [integer] shard_count The number of shards in the stream
    def create_kinesis_stream_for_table(table,shard_count=1)
      result = false
      if @options[:kinesis_enabled]
        result = @broker.create_kinesis_stream_for_table(table,shard_count)
      else
        @logger.warn("#{TAG} Kinesis is not enabled. Nothing done.")
      end
      return result
    end

    private

    # Merge options between passed options and the default options in RedTrack client
    #
    # @param [Hash] options The set of options passed
    def merge_options(options)
      merged_options=@options
      options.each do |passed_option_key,passed_option_value|
        merged_options[passed_option_key] = passed_option_value
      end
      return merged_options
    end

  end
end
