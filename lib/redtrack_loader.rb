# Redshift s3 loader. Can copy events into s3. Can also copy events into Redshift. Copies events into Redshift so
# as to avoid duplication.
#
# Copyright (c) 2014 RedHotLabs, Inc.
require 'tempfile'

module RedTrack


  class LoaderException < Exception

    attr_reader :information
    def initialize(information)
      @information = information
    end

  end

  class Loader

    TAG='RedTrack::Loader'

    # S3 parameters
    @broker=nil
    @s3_bucket = nil
    @redshift_conn = nil
    @client = nil

    @options = nil

    @max_error = nil

    @load_start_time=nil

    DEFAULT_MAX_ERROR=2 # Set max error > 0 in case of "cosmic ray" events

    # Setup class variables for redshift & s3 access
    #
    # @param [Hash] options expects access_key_id, secret_access_key, region, region, redshift_host, redshift_port, redshift_dbname, redshift_user, redshift_password, s3_bucket
    # @param [RedTrack::KinesisClient] broker The broker client, created by the RedTrack::Client object
    # @param [PG::Connection] redshift_conn The redshift connection used for loading data
    # @return [Boolean] Success
    def initialize(options,broker=nil,redshift_conn=nil)

      # Broker
      if broker
        @broker = broker
      else
        raise 'Needs to pass broker client to the loader'
      end

      # Check for redshift connection; otherwise create one
      if redshift_conn
        @redshift_conn = redshift_conn
      else
        raise 'Need to pass redshift connection to the loader'
      end

      options[:max_error] ||=  DEFAULT_MAX_ERROR

      @logger = options[:logger]
      if @logger == nil
        @logger = Logger.new(STDOUT)
      end

      @options = options

      # Create S3 connection for bucket
      @s3_bucket = AWS::S3.new.buckets[options[:s3_bucket]]

    end

    # Write a profiling message to the logger
    #
    # @param [Strimg] message The message to write to the logger
    def loader_profile(message)
      elapsed_time=(Time.now-@load_start_time).round(2)
      @logger.info("#{TAG} (#{elapsed_time}s elapsed) #{message}")
    end

    # High level function - read data from broker, upload data to s3, perform COPY command to load data into Redshift
    #
    # @param [String] redshift_table The name of the table in redshift to load
    # @param [Integer] stream_shard_index The index of the shard from describe_stream array of stream shards
    def load_redshift_from_broker(redshift_table,stream_shard_index)

      # Start time - use this for profiling messages
      @load_start_time = Time.now

      # Get metadata about the kinesis stream and shard we are going to read data from
      stream_name=@broker.stream_name(redshift_table)
      shard_description=@broker.get_shard_description(stream_name,stream_shard_index)
      if shard_description[:success] == false
        information = {
            :stream_name => stream_name,
            :shard_description => shard_description
        }
        raise RedTrack::LoaderException.new(information), 'Could not get shard description'
      end
      loader_profile('Get stream shard metadata complete')

      # Local file to store data
      file = Tempfile.new("#{stream_name}.#{shard_description[:shard_id]}.#{@load_start_time.to_i}")

      # Read from broker into a file locally - if nothing read
      broker_read_result = read_broker_shard_after_last_load_into_file(file,redshift_table,stream_name,shard_description)
      loader_profile("Read broker shard complete (#{broker_read_result[:records]} events read)")

      # check to see if we didn't read anything from broker
      if(broker_read_result[:records] == 0)
        information = {
          :stream_name => stream_name,
          :shard_description => shard_description,
          :file_name => file.path,
          :broker_read_result => broker_read_result
        }
        raise RedTrack::LoaderException.new(information), 'No events read from Broker'
      end

      # Close file
      file.close

      # Upload local file to s3
      upload_result = upload_to_s3(file.path,redshift_table)
      loader_profile('Upload to S3 complete')
      if(upload_result[:success] == false)
        information = {
            :stream_name => stream_name,
            :shard_description => shard_description,
            :file_name => file.path,
            :broker_read_result => broker_read_result,
            :upload_result => upload_result
        }
        raise RedTrack::LoaderException.new(information), 'Upload to S3 failed'
      end

      # Load the file into redshift
      load_result = load_kinesis_shard_into_redshift(upload_result[:s3_url],redshift_table,stream_name,shard_description,
                                broker_read_result[:starting_sequence_number],broker_read_result[:ending_sequence_number])
      loader_profile("Load kinesis shard into Redshift complete (#{load_result[:records]} events)")
      if(load_result[:success] == false)
        information = {
          :stream_name => stream_name,
          :shard_description => shard_description,
          :file_name => file.path,
          :broker_read_result => broker_read_result,
          :s3_upload_result => upload_result,
          :redshift_load_result => load_result
        }
        raise RedTrack::LoaderException.new(information), 'COPY into redshift failed'
      end

      # delete the file - leaves file if there is a failure
      file.unlink

      return { :success => true }
    end

    # Read the not-yet-loaded events from the data broker into a file
    #
    # @param [File] file The file handle to write the broker data into
    # @param [String] redshift_table The red shift table which we are loading, determine last loaded event
    # @param [String] stream_name The name of the stream to read events from
    # @param [Hash] shard_description The shard to read
    # @return [Hash] Returns a hash included :success (whether the stream exists and was read) and :records (number of events loaded)
    def read_broker_shard_after_last_load_into_file(file,redshift_table,stream_name,shard_description)

      # Get the last loaded sequence number
      last_shard_load = get_last_kinesis_shard_load(redshift_table,stream_name,shard_description)
      if last_shard_load != nil
        starting_sequence_number = last_shard_load['ending_sequence_number']
      else
        starting_sequence_number = nil
      end

      # Get shard iterator for the sequence number
      shard_iterator = @broker.get_shard_iterator_from_sequence_number(stream_name, shard_description, starting_sequence_number)

      # Read records after shard_iterator into file
      return @broker.stream_read_from_shard_iterator_into_file(shard_iterator, file)
    end

    # Uploads file to s3
    #
    # @param [String] file_name The file to upload
    # @param [String] redshift_table The table to upload for
    # @return [Hash] Information about the upload, included :success which is whether the file was uploaded/right size in S3
    def upload_to_s3(file_name,redshift_table)

      # Compress file if needed (based off of extension, -f in case the file already is there for whatever reason)
      if (file_name[".gz"] == nil)
        system("gzip -f #{file_name}")
        file_name="#{file_name}.gz"
      end

      # determine s3 key
      s3_key = s3_prefix(redshift_table, Time.new.utc.to_date) + File.basename(file_name)

      # Upload file to s3
      object = @s3_bucket.objects[s3_key]
      s3_write_result = object.write(Pathname.new(file_name))

      # Verify the file size in s3 matches local file size - s3 is eventually consistency.
      local_file_size = File.size(file_name)
      s3_file_size=nil
      attempt_count=3
      success=false
      while attempt_count > 0 && !success
        s3_file_size = object.content_length
        if (local_file_size == s3_file_size)
          success=true
          break
        else
          sleep 5
          attempt_count-=1
        end
      end

      # If not successful at verifying file size, raise exception
      if !success
        raise "File size mismatch. Local: #{local_file_size}. S3: #{s3_file_size}"
      end

      result = {
        success: success,
        s3_write_result: s3_write_result,
        s3_url: "s3://#{@s3_bucket.name}/#{s3_key}",
        s3_file_size: s3_file_size
      }

      return result
    end

    # Cleans up entries in s3 for a particular date.
    # Note: A simpler way to do this is to set a lifecycle policy for S3 objects
    #
    # @param [String] redshift_table The table for which we are cleaning up s3
    # @param [Date] date The date for which to clean up the
    def cleanup_s3_loads(redshift_table,date)
      @bucket.objects.with_prefix(s3_prefix(redshift_table,date)).delete_all
    end

    # Get the last load kinesis -> redshift for any given shard
    #
    # @param [String] redshift_table The name fo the table to get last shard load for
    # @param [String] stream_name The name of the kinesis stream
    # @param [Hash] shard_description Description of the shard from describe_stream
    # @return [Hash] Information about the last load for this kinesis shard
    def get_last_kinesis_shard_load(redshift_table,stream_name,shard_description)

      query = "SELECT * FROM kinesis_loads WHERE table_name='#{redshift_table}' AND stream_name='#{stream_name}' AND shard_id='#{shard_description[:shard_id]}' ORDER BY load_timestamp DESC LIMIT 1;"
      result_set = @redshift_conn.exec(query)

      if result_set.ntuples == 1
        result = {}
        result_set.each do |row|
          row.each do |hash_key,hash_value|
            result[hash_key] = hash_value
          end
        end
      elsif result_set.ntuples == 0
        result = nil
      else
        raise 'Invalid number of rows'
      end
      return result
    end

    # Checks to see if we've already loaded this sequence range into redshift, if not, performs redshift load.
    # Inspired by https://github.com/awslabs/amazon-kinesis-connectors/blob/master/src/main/java/com/amazonaws/services/kinesis/connectors/redshift/RedshiftManifestEmitter.java
    #
    # @param [String] s3_url The Url of the file in s3 to load
    # @param [String] redshift_table The name of the redshift table to load
    # @param [String] stream_name The name of the stream where the events are loaded from
    # @param [Hash] shard_description Result from describe stream
    # @param [Integer] starting_sequence_number The first sequence number to load
    # @param [Integer] ending_sequence_number The last sequence number to load
    def load_kinesis_shard_into_redshift(s3_url,redshift_table,stream_name,shard_description,starting_sequence_number,ending_sequence_number)

      shard_id = shard_description[:shard_id]

      @redshift_conn.exec('BEGIN')

      # Get all loads in the last week for this shard. Do range comparison in ruby since Kinesis sequence numbers are 56 digits,
      # Redshift cannot handle > 38 digits, and ruby can handle arbitrary large numerical numbers.
      # http://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html
      # http://patshaughnessy.net/2014/1/9/how-big-is-a-bignum
      query = 'SELECT * FROM kinesis_loads ' +
        "WHERE table_name='#{redshift_table}' AND stream_name='#{stream_name}' AND shard_id='#{shard_id}'"
      loads_result_set = @redshift_conn.exec(query)
      duplicate_load=false
      duplicated_load = nil
      loads_result_set.each do |row|
        load_starting_sequence_number=row['starting_sequence_number'].to_i
        load_ending_sequence_number=row['ending_sequence_number'].to_i
        starting_sequence_number=starting_sequence_number.to_i
        ending_sequence_number=ending_sequence_number.to_i

        # Ranges are loaded with previous ending_sequence_number equaling next loads starting_sequence_number
        if ( (load_starting_sequence_number < starting_sequence_number && starting_sequence_number < load_ending_sequence_number) ||
          (load_starting_sequence_number < ending_sequence_number  && ending_sequence_number < load_ending_sequence_number) ||
          (starting_sequence_number <= load_starting_sequence_number && load_ending_sequence_number <= ending_sequence_number) )
          duplicate_load=true

          @logger.warn("#{TAG} Overlapping load of #{redshift_table} at #{row['load_timestamp']}: Kinesis stream=#{row['stream_name']}, " +
            "shard=#{row['shard_id']}. Sequence from #{row['starting_sequence_number']} to #{row['ending_sequence_number']}")

          duplicated_load = row

          break
        end
      end

      if duplicate_load == false

        begin
          # Insert entry for load
          insert_query = 'INSERT INTO kinesis_loads VALUES ' +
            "('#{stream_name}','#{shard_id}','#{redshift_table}','#{starting_sequence_number}','#{ending_sequence_number}',getdate())"
          @redshift_conn.exec(insert_query)

          # Load into database
          load_file_result=load_file_into_redshift(redshift_table,s3_url)

          # Commit transaction
          @redshift_conn.exec('COMMIT')

          # Report back the success and the number of rows loaded into redshift
          result = {
            :success => true,
            :records => load_file_result[:records]
          }
        rescue Exception => e

          # Abort transaction
          @redshift_conn.exec('ROLLBACK')

          # Get more information about the error
          load_error = get_last_load_errors(redshift_table,s3_url)

          result = {
            :success => false,
            :load_error => load_error,
            :exception => e
          }
        end
      else

        # Abort the transaction
        @redshift_conn.exec('ROLLBACK')
        result = {
          :success => false,
          :load_error => 'Duplicated kinesis range',
          :duplicated_load => duplicated_load
        }
      end

      return result
    end

    # Load a file into redshift
    #
    # @param [String] redshift_table The table to load the data into
    # @param [String] s3_url The s3 file to load into redshift
    def load_file_into_redshift(redshift_table,s3_url)

      ## Run the copy command to load data from s3 to Redshift. This is cleaner than doing the ssh method
      cmd="COPY #{redshift_table} from '#{s3_url}' with " +
        "credentials 'aws_access_key_id=#{@options[:access_key_id]};aws_secret_access_key=#{@options[:secret_access_key]}' " +
        "json 'auto' timeformat 'auto' GZIP MAXERROR #{@options[:max_error]};"
      @logger.debug(cmd)
      records=nil

      # Check to see how many rows are loaded (via the INFO)
      @redshift_conn.set_notice_receiver {|result|
        matches=/.*,.(\d+).record.*/.match(result.error_message)
        records = matches[1].to_i
      }

      @redshift_conn.exec(cmd)

      result = {
        :success => true,
        :records => records
      }

      return result
    end

    # Print the last load error for a specific redshift table
    #
    # @param [String] redshift_table The name of the redshift table
    # @param [String] s3_url The s3 url that was attempted to be loaded into redshift
    def get_last_load_errors(redshift_table,s3_url)

      # Query to get recent load errors matching table and s3 url
      cmd = 'select  tbl, trim(name) as table_name, starttime, filename, line_number, raw_line,' +
        'colname, raw_field_value, err_code, trim(err_reason) as reason ' +
        'from stl_load_errors sl, stv_tbl_perm sp ' +
        "where sl.tbl = sp.id AND sp.name='#{redshift_table}' AND sl.filename='#{s3_url}' " +
        'ORDER BY starttime DESC LIMIT 20;'
      result_set=@redshift_conn.exec(cmd)

      # Collect the results, assume the first matching query id in stl_load_errors is the one that failed.
      result = []
      query=nil
      result_set.each do |row|
        if query == nil
          query=row['query']
        end

        if query != row['query']
          break
        end
        result.push(row)
      end
      return result
    end

    private

    # Determine the s3 prefix for loading into s3 - files are organized by date
    #
    # @param [String] redshift_table The table in redshift
    # @param [Date] date The date when the data is loaded
    # @return [String] The relative s3 location in a bucket
    def s3_prefix(redshift_table,date)
      dateyyyymmdd=date.strftime('%Y%m%d')
      return "warehouse/#{@options[:redshift_dbname]}/#{redshift_table}/#{dateyyyymmdd}/"
    end

    # Run a command against the redshift cluster (todo, should this be done differently?)
    #
    # @param [String] cmd The sql command to run
    def exec(cmd)
      @logger.debug(cmd)
      @redshift_conn.exec(cmd)
    end

  end
end
