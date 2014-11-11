# The KinesisClient provides an application interface to aws kinesis as a data broker
#
# Copyright (c) 2014 RedHotLabs, Inc.
# Licensed under the MIT License

module RedTrack
  class KinesisClient

    @verbose = false

    TAG='RedTrack::KinesisClient'

    DEFAULT_MAX_RECORDS=1000000
    DEFAULT_MAX_REQUESTS=100

    # Setup instance variables for kinesis access
    #
    # @param [Hash] options Expects :redshift_cluster_name, :redshift_dbname. Optionally :verbose
    # @return [Boolean] Success
    def initialize(options)
      @verbose = options[:verbose] || false
      @logger = options[:logger]
      if @logger == nil
        @logger = Logger.new(STDOUT)
      end
      @options=options
    end

    # Name of the stream in the data broker (This is a Kinesis stream name)
    #
    # @param [String] redshift_table Name of the redshift table
    # @return [String] Name of the stream in Kinesis
    def stream_name(redshift_table)
      if @options[:redshift_cluster_name] == nil || @options[:redshift_dbname] == nil
        raise 'Need to specify :redshift_cluster_name and :redshift_dbname as options'
      end
      result= @options[:redshift_cluster_name] + '.' + @options[:redshift_dbname] + ".#{redshift_table}"
      return result
    end

    # Get hash describing the shard from describe_stream
    #
    # @param [String] stream_name The name of the kinesis stream
    # @param [Integer] stream_shard_index The index of the shard in the array of shards
    # @return [Hash] Information regarding the stream shard, from AWS kinesis
    def get_shard_description(stream_name,stream_shard_index)
      describe_response = AWS.kinesis.client.describe_stream({:stream_name => stream_name})
      if describe_response != nil && describe_response[:stream_description] != nil
        result = describe_response[:stream_description][:shards][stream_shard_index]
        result[:success] = true
        result[:stream_description] = describe_response[:stream_description]
      else
        result = {
            success: false,
            describe_response: describe_response
        }
      end
      return result
    end

    # Create a kinesis stream for the redshift table
    #
    # @param [String] table The name of the table
    # @param [integer] shard_count The number of shards in the stream
    def create_kinesis_stream_for_table(table,shard_count=1)
      options = {
          :stream_name => stream_name(table),
          :shard_count => shard_count
      }
      result = AWS.kinesis.client.create_stream(options)
      return result
    end

    # Get the shard iterator given a checkpointed sequence number. If no checkpoint, start to read from start of shard
    #
    # @param [String] stream_name The name of the stream to get a shard iterator for
    # @param [Hash] shard_description Result from describe stream request
    # @param [String] starting_sequence_number The sequence number to get a shard iterator for, if doesn't exist, get one for start of shard
    # @return [String] The shard iterator
    def get_shard_iterator_from_sequence_number(stream_name,shard_description,starting_sequence_number=nil)

      ## Get shard iterator
      get_shard_iterator_options = {
          :stream_name => stream_name,
          :shard_id => shard_description[:shard_id]
      }

      ## Options based on starting sequence number
      if starting_sequence_number != nil
        get_shard_iterator_options[:shard_iterator_type] = 'AFTER_SEQUENCE_NUMBER'
        get_shard_iterator_options[:starting_sequence_number] = starting_sequence_number
      else
        @logger.warn('No checkpoint sequence number, reading from shard trim_horizon')
        get_shard_iterator_options[:shard_iterator_type] = 'TRIM_HORIZON'
      end

      get_shard_iterator_response = AWS.kinesis.client.get_shard_iterator(get_shard_iterator_options)
      shard_iterator = get_shard_iterator_response[:shard_iterator]
      return shard_iterator
    end

    # Read from kinesis shard into a file
    #
    # @param [String] shard_iterator  The shard iterator to start reading from - result of get_shard_iterator
    # @param [String] file The file read into.
    # @param [Hash] options Optional. Can specify :max_records, :max_requests
    # @return [Hash] Hash of # of records read and the sequence number of the last read record, number of records, and shard iterator
    def stream_read_from_shard_iterator_into_file(shard_iterator, file, options={})

      max_records = options[:max_records] || DEFAULT_MAX_RECORDS
      max_requests = options[:max_requests] || DEFAULT_MAX_REQUESTS

      start_sequence_number=nil
      end_sequence_number=nil
      records = 0

      for i in 0..max_requests

        # Execute get_records against AWS Kinesis
        get_records_response = AWS.kinesis.client.get_records({:shard_iterator => shard_iterator})

        # Process records
        if get_records_response != nil && get_records_response.data != nil && get_records_response.data[:records] != nil && get_records_response.data[:records].count > 0
          get_records_response.data[:records].each do |record|

            data_payload = JSON.parse(record[:data])
            data = data_payload['data']

            file.puts data + "\n"

            #  Seqeunce numbers
            if (start_sequence_number == nil)
              start_sequence_number = record[:sequence_number].to_i
            end
            if (end_sequence_number == nil || record[:sequence_number].to_i > end_sequence_number)
              end_sequence_number = record[:sequence_number].to_i
            else
              @logger.warn("#{TAG} Out of order sequence number: #{end_sequence_number.to_s}")
            end

            # Increment records read; check exit condition
            records+=1
            if (records >= max_records)
              break
            end
          end
        end

        # set shard iterator for next request from payload
        shard_iterator=get_records_response.data[:next_shard_iterator]

        # Check exit conditions
        if(shard_iterator == nil || records >= max_records)
          break
        end
      end

      result = {
        starting_sequence_number: start_sequence_number.to_s,
        ending_sequence_number: end_sequence_number.to_s,
        next_shard_iterator: shard_iterator,
        records: records,
        success: true
      }
      return result
    end

    # Write data to a stream. This expects the data to be a serialized string
    #
    # @param [String] stream_name The name of the stream
    # @param [String] data_string String of data to write
    # @param [String] partition_key How to keep the data partitioned in kinesis. See http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#Kinesis-PutRecord-request-PartitionKey
    # @return [Boolean] True - the write to the stream succeeded
    def stream_write(stream_name,data_string,partition_key=nil)
      result=false

      partition_key = partition_key || rand(100).to_s

      put_data = {
        :data => data_string
      }

      put_options = {
        :stream_name => stream_name,
        :partition_key => partition_key,
        :data => put_data.to_json
      }

      @logger.debug("#{TAG} write to #{stream_name} stream with data #{data_string}")

      # Write to kinesis; 3 attempts
      attempt_count=3
      last_exception=nil
      while attempt_count > 0 && !result
        begin
          put_record_result = AWS.kinesis.client.put_record(put_options)
          puts put_record_result.to_json
          @logger.warn("put record result #{put_record_result.to_json}")
          if put_record_result.http_response.status < 299
            result = true
          else
            @logger.warn("#{TAG} put_record response: HTTP #{put_record_result.http_response.status}: #{put_record_result.http_response.body}")
          end
        rescue Exception => e

          # log exception and retry with 1 second backoff
          @logger.warn("#{TAG} put_record Exception caught #{e.class}: #{e.message}\n\t#{e.backtrace.join("\n\t")}")
          attempt_count-=1
          last_exception=e
        end
      end

      # If failure after 3 retries, raise the last exception
      if !result
        raise last_exception
      end

      return result
    end

  end
end
