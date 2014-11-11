# The FileClient provides an application interface to a file-based broker for redshift data
#
# Copyright (c) 2014 RedHotLabs, Inc.
# Licensed under The MIT License

module RedTrack
  class FileClient

    @options=nil

    # Setup class variables for kinesis access
    #
    # @param [Hash] options Nothing expected
    # @return [Boolean] Success
    def initialize(options)
      @options = options
    end

    # Get Location of the stream
    #
    # @param [String] stream_name The name of the stream
    # @return [String] Url/file location for the stream
    def stream_location(stream_name)
      # V1 of data streaming - use a local file
      return "log/#{stream_name}"
    end

    # Whether or not the stream has data
    #
    # @param [String] stream_name The name of the stream
    # @return [Boolean] Whether or not the stream has data
    def stream_has_data(stream_name)
      # V1 of data streaming - use a local file
      return File.exist?(self.stream_location(stream_name))
    end

    # Write data to a stream
    #
    # @param [String] stream_name The name of the stream
    # @param [String] data_string String of data to write
    # @param [String] partition_key Ignored
    # @return [Boolean] True - the write to the stream succeeded
    def stream_write(stream_name,data_string,partition_key=nil)

      # V1 of data streaming - use a local file: open, write, close
      stream=File.open(self.stream_location(stream_name),"a")
      stream.puts data_string + "\n"
      stream.close
      return true
    end

    # Fake shard description for file, use hostname for shard_name
    #
    # @param [String] stream_name The name of the kinesis stream
    # @param [Integer] stream_shard_index The index of the shard in the array of shards
    def get_shard_description(stream_name,stream_shard_index)
      return {
          :success => true,
          :shard_id => system('hostname')
      }
    end

    # Get the shard iterator given a checkpointed sequence number. If no checkpoint, start to read from start of shard
    #
    # @param [String] stream_name The name of the stream to get a shard iterator for
    # @param [Hash] shard_description Result from describe stream request
    # @param [String] starting_sequence_number The sequence number to get a shard iterator for, if doesn't exist, get one for start of shard
    # @return [String] The shard iterator
    def get_shard_iterator_from_sequence_number(stream_name,shard_description,starting_sequence_number=nil)
      return self.stream_location(stream_name)
    end

    # Ream from kinesis shard into a file
    #
    # @param [String] shard_iterator  The shard iterator to start reading from - result of get_shard_iterator- http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
    # @param [String] file_name The filename to read into.
    # @param [Hash] options Optional. Can specify :max_records, :max_requests, :max_consecutive_requests_without_data, :backoff_no_data
    # @return [Hash] Hash of # of records read and the sequence number of the last read record, number of records, and shard iterator
    def stream_read_from_shard_iterator_into_file(shard_iterator, file, options={})

      stream_file_name = shard_iterator

      fake_sequence_number = Time.now.to_i

      file.close

      if File.exist?(stream_file_name)
        FileUtils.mv(stream_file_name, file.path)

        result = {
          :starting_sequence_number => fake_sequence_number,
          :ending_sequence_number => fake_sequence_number,
          :records => %x(wc -l "#{file.path}").to_i,
          :success => true
        }
      else
        result = {
          :starting_sequence_number => fake_sequence_number,
          :ending_sequence_number => fake_sequence_number,
          :records => 0,
          :sucess => false
        }
      end

      return result
    end

    # Name of the stream in the data broker (This is a Kinesis stream name)
    #
    # @param [String] redshift_table Name of the redshift table
    # @return [String] Name of the stream in Kinesis
    def stream_name(redshift_table)
      result= @options[:redshift_cluster_name] + '.' + @options[:redshift_dbname] + ".#{redshift_table}"
      return result
    end

  end
end