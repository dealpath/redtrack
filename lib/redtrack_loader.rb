# Redshift s3 loader. Can copy events into s3. Can also copy events into Redshift. Copies events into Redshift so
# as to avoid duplication.
#
# Copyright (c) 2014 RedHotLabs, Inc.
# Licensed under The MIT License

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
    def load_redshift_from_broker(redshift_table)

      # Start time - use this for profiling messages
      @load_start_time = Time.now

      # Get metadata about the kinesis stream and its shards
      stream_name=@broker.stream_name(redshift_table)
      shards = @broker.get_shard_descriptions(stream_name)
      if shards == nil
        information = {
            :redshift_table => redshift_table,
            :stream_name => stream_name
        }
        raise RedTrack::LoaderException.new(information), 'Could not get shard description'
      end
      loader_profile('Get stream metadata complete')

      # Get metadata about the redshift cluster, specifically the number of slices
      num_slices = get_number_of_slices(@options[:redshift_cluster_name])
      loader_profile('Get redshift metadata complete')

      # Get last loads for each shard - do this pre-fork in order to avoid re-establishing Redshift connections post-fork
      last_shard_loads = get_last_shard_loads(redshift_table,stream_name,shards)
      loader_profile('Get last shard loads complete')

      # Determine where to upload files to s3 and number of files each shard should be produce
      load_s3_location = s3_prefix(redshift_table, Time.new.utc.to_date, "load-#{@load_start_time.to_i}")

      # For each shard, fork a process for stream read & s3 upload; create a pipe to communicate result back
      pids = {}
      result_readers = {}
      shards.each do |shard|
        result_reader,result_writer = IO.pipe
        pid = fork do
          loader_profile("#{shard[:shard_id]} fork - start")
          last_shard_load = last_shard_loads[shard[:shard_id]]
          begin
            result = read_shard_and_upload_to_s3(shard,last_shard_load,load_s3_location,stream_name,num_slices)
            result_writer.puts result.to_json
          rescue Exception => e
            @logger.warn("#{TAG} #{shard[:shard_id]} fork - Exception caught: #{e.class}: #{e.message}\n\t#{e.backtrace.join("\n\t")}")
            result = {
                :shard_id => shard[:shard_id],
                :exception => e
            }
            result_writer.puts result.to_json
          end
          loader_profile("#{shard[:shard_id]} fork - read shard & upload done")
        end
        pids[shard[:shard_id]] = pid
        result_readers[shard[:shard_id]] = result_reader
      end

      # Wait for the forked processes to finish and read the corresponding result pipe
      fork_results = []
      shards.each do |shard|
        Thread.new { Process.waitpid(pids[shard[:shard_id]]) }.join
        result_from_fork = result_readers[shard[:shard_id]].gets
        if result_from_fork != nil && result_from_fork != 'nil'
          fork_results << JSON.parse(result_from_fork, {symbolize_names: true})
        else
          fork_results << nil
        end
      end
      loader_profile('All shard read & upload forks complete')

      @logger.info("Fork Results: #{YAML::dump(fork_results)}")

      # Build manifest and check results for shards to load into redshift
      shards_to_load = []
      manifest = {
          :entries => []
      }
      fork_results.each do |fork_result|
        if fork_result[:exception] != nil
          raise "Exception in #{fork_result[:shard_id]} fork: #{fork_result[:exception]}"
        end
        if fork_result[:records] > 0
          fork_result[:s3_urls].each do |s3_url|
            entry = {
                url: s3_url,
                mandatory: true
            }
            manifest[:entries].push(entry)
          end
          shards_to_load << fork_result
        end
      end

      # Check for exit condition - no shards have anything to load
      if shards_to_load.length == 0
        @logger.warn("#{TAG} No events read from any shards. Exiting.")
        result = {
            :success => true,
            :records => 0,
            :information => {
                :redshift_table => redshift_table,
                :shards => shards,
                :last_shard_loads => last_shard_loads,
                :fork_results => fork_results,
            }
        }
        return result
      end

      # upload manifest to s3
      manifest_s3_object = @s3_bucket.objects[load_s3_location + "manifest.json"]
      manifest_s3_object.write(manifest.to_json)
      manifest_s3_url = "s3://#{manifest_s3_object.bucket.name}/#{manifest_s3_object.key}"
      loader_profile("manifest s3 upload complete #{manifest_s3_url}.")

      # reconnect to redshift
      @redshift_conn = PG.connect(
          :host => @options[:redshift_host],
          :port => @options[:redshift_port],
          :dbname => @options[:redshift_dbname],
          :user => @options[:redshift_user],
          :password => @options[:redshift_password])

      # Load the files into redshift via manifest
      load_result = load_shards_manifest_into_redshift(manifest_s3_url,redshift_table,stream_name,shards_to_load,last_shard_loads)
      loader_profile("Load kinesis shard into Redshift complete (#{load_result[:records]} events)")

      information = {
          :redshift_table => redshift_table,
          :shards => shards,
          :last_shard_loads => last_shard_loads,
          :fork_results => fork_results,
          :shards_to_load => shards_to_load,
          :manifest => manifest,
          :load_result => load_result
      }

      if(load_result[:success] == false)
        raise RedTrack::LoaderException.new(information), 'COPY into redshift failed'
      end

      return {
          :success => true,
          :records => load_result[:records],
          :addtl_information => information
      }
    end

    def read_shard_and_upload_to_s3(shard_description,last_shard_load,load_s3_location,stream_name,num_slices)

      # Create local files to store data
      files = []
      (1..num_slices).each do |i|
        file = Tempfile.new("#{shard_description[:shard_id]}")
        files.push(file)
      end

      # Start the read from the last loaded sequence number
      if last_shard_load != nil
        starting_sequence_number = last_shard_load['ending_sequence_number']
      else
        starting_sequence_number = nil
      end

      # Get shard iterator for the sequence number
      shard_iterator = @broker.get_shard_iterator_from_sequence_number(stream_name, shard_description, starting_sequence_number)

      # Read records after shard_iterator into file
      stream_read_result =  @broker.stream_read_from_shard_iterator_into_files(shard_iterator, files)
      loader_profile("#{shard_description[:shard_id]} fork - kinesis read complete. #{stream_read_result[:records]} events read.")

      files.each do |file|
        file.close
      end

      # if we read anything from kinesis, upload files to s3
      if(stream_read_result[:records] > 0)

        s3_urls = []

        # Sequentially, compress each file and upload to s3
        files.each do |file|

          # Compress file (-f in case the file already is there for whatever reason)
          system("gzip -f #{file.path}")
          file_name="#{file.path}.gz"

          # Upload file to s3
          s3_url = upload_to_s3(file_name,load_s3_location)

          # Check result,
          if !s3_url
            raise RedTrack::LoaderException.new(information), 'Upload to S3 failed'
          end

          # Delete local file
          file.unlink

          s3_urls << s3_url
          loader_profile("#{shard_description[:shard_id]} fork - s3 upload complete #{s3_url}.")
        end

        result = {
            :shard_id => shard_description[:shard_id],
            :records => stream_read_result[:records],
            :starting_sequence_number => stream_read_result[:starting_sequence_number],
            :ending_sequence_number => stream_read_result[:ending_sequence_number],
            :s3_urls => s3_urls
        }

      else
        # If stream_read_result didn't read any events, return simply that
        result = {
          :shard_id => shard_description[:shard_id],
          :records => stream_read_result[:records]
        }
      end

      return result
    end

    # Calculate the number of slices for the redshift cluster
    #
    # @param [string] cluster_name The name of the cluster to get # of slices for
    # @return [Integer] The number of slices in the cluster
    def get_number_of_slices(cluster_name)
      result = 0

      describe_clusters_response = AWS.redshift.client.describe_clusters

      describe_clusters_response[:clusters].each do |cluster|
        if cluster[:cluster_identifier] == cluster_name
          number_of_nodes = cluster[:number_of_nodes]

          # Slices per node is equal to number of vCPUs
          slices_per_node = 1
          case cluster[:node_type]
            when 'dw2.large','dw1.xlarge'
              slices_per_node = 2
            when 'dw1.8xlarge'
              slices_per_node = 16
            when 'dw2.8xlarge'
              slices_per_node = 32
            else
              raise "Unrecognized node type: #{cluster[:node_type]}"
          end

          result = number_of_nodes * slices_per_node

          puts "Result #{result}, number_of_nodes: #{number_of_nodes}, node_type: #{cluster[:node_type]}, slices_per_node: #{slices_per_node}"

          break
        end

        if result == 0
          raise "Did not find cluster with name #{cluster_name}"
        end
      end
      return result
    end

    # Uploads file to s3
    #
    # @param [String] file_name The file to upload
    # @param [String] load_s3_prefix The location to upload to in s3
    # @return [Hash] Information about the upload, included :success which is whether the file was uploaded/right size in S3
    def upload_to_s3(file_name,load_s3_prefix)

      # determine s3 key
      s3_key = load_s3_prefix + File.basename(file_name)

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
        @logger.warn("File size mismatch. Local: #{local_file_size}. S3: #{s3_file_size}")
      end

      return "s3://#{@s3_bucket.name}/#{s3_key}"
    end

    # Cleans up entries in s3 for a particular date.
    # Note: A simpler way to do this is to set a lifecycle policy for S3 objects
    #
    # @param [String] redshift_table The table for which we are cleaning up s3
    # @param [Date] date The date for which to clean up the
    def cleanup_s3_loads(redshift_table,date)
      @bucket.objects.with_prefix(s3_prefix(redshift_table,date)).delete_all
    end


    # Get the last load kinesis -> redshift for a set of shards
    #
    # @param [String] redshift_table The name fo the table to get last shard load for
    # @param [String] stream_name The name of the kinesis stream
    # @param [Array] shards description of the shard from describe_stream
    # @return [Array] Information about the last load for this kinesis shard
    def get_last_shard_loads(redshift_table,stream_name,shards)

      last_loads = {}
      shards.each do |shard|
        last_loads[shard[:shard_id]] = get_last_shard_load(redshift_table,stream_name,shard)
      end

      @logger.info("Last Shard Loads: #{YAML::dump(last_loads)}")

      return last_loads
    end

    # Get the last load kinesis -> redshift for any given shard
    #
    # @param [String] redshift_table The name fo the table to get last shard load for
    # @param [String] stream_name The name of the kinesis stream
    # @param [Hash] shard_description Description of the shard from describe_stream
    # @return [Hash] Information about the last load for this kinesis shard
    def get_last_shard_load(redshift_table,stream_name,shard_description)

      query = "SELECT * FROM kinesis_loads WHERE table_name='#{redshift_table}' AND stream_name='#{stream_name}' AND shard_id='#{shard_description[:shard_id]}' ORDER BY load_timestamp DESC LIMIT 1;"
      result_set = exec(query)

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
    # @param [String] manifest_s3_url The Url of the manifest file in s3
    # @param [String] redshift_table The name of the redshift table to load
    # @param [String] stream_name The name of the stream where the events are loaded from
    # @param [Hash] shards_to_load Shards we are loading in this loader - all shards with > 0 events
    # @param [String] last_shard_loads Set of last shard loads
    def load_shards_manifest_into_redshift(manifest_s3_url,redshift_table,stream_name,shards_to_load,last_shard_loads)

      begin

        exec('BEGIN')

        # Check that there hasn't been a load since the loader started running
        shards_to_load.each do |shard|
          last_shard_load = last_shard_loads[shard[:shard_id]]
          new_last_shard_load = get_last_shard_load(redshift_table,stream_name,shard)
          if new_last_shard_load != nil && new_last_shard_load['ending_sequence_number'] != last_shard_load['ending_sequence_number']
            @logger.warn("A new Redtrack load has occurred for shard #{shard[:shard_id]} since starting the loader")
            exec('ROLLBACK')
            result = {
                :success => false,
                :load_error => 'A new Redtrack load has occurred for this shard since starting the loader',
                :expected_last_shard_load => last_shard_load,
                :new_last_shard_load => new_last_shard_load
            }

            return result
          end
        end

        # Check that there aren't overlapped loaded sequences.
        # Since sequence numbers are 56 digits and Redshift handle 38 digits max - store as strings and compare in ruby locally
        # http://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html
        # http://patshaughnessy.net/2014/1/9/how-big-is-a-bignum
        shard_ids = []
        shards_to_load.each do |shard_to_load|
          shard_ids << "'#{shard_to_load[:shard_id]}'"
        end

        # TODO: this needs to be converted over!
        query = 'SELECT * FROM kinesis_loads' +
          " WHERE table_name='#{redshift_table}' AND stream_name='#{stream_name}' AND shard_id in (#{shard_ids.join(',')})" +
          ' ORDER BY shard_id, load_timestamp DESC'

        loads_result_set = exec(query)
        loads_result_set.each do |row|
          row_starting_sequence_number=row['starting_sequence_number'].to_i
          row_ending_sequence_number=row['ending_sequence_number'].to_i

          # Get the sequence number range from the shard that's going to be loaded
          starting_sequence_number = nil
          ending_sequence_number = nil
          shards_to_load.each do |shard_to_load|
            if shard_to_load[:shard_id] == row['shard_id']
              starting_sequence_number=shard_to_load[:starting_sequence_number].to_i
              ending_sequence_number=shard_to_load[:ending_sequence_number].to_i
              break
            end
          end

          # Ranges are loaded with previous ending_sequence_number equaling next loads starting_sequence_number
          if ( (row_starting_sequence_number < starting_sequence_number && starting_sequence_number < row_ending_sequence_number) ||
            (row_starting_sequence_number < ending_sequence_number  && ending_sequence_number < row_ending_sequence_number) ||
            (starting_sequence_number <= row_starting_sequence_number && row_ending_sequence_number <= ending_sequence_number) )

            @logger.warn("#{TAG} Overlapping load of #{redshift_table} at #{row['load_timestamp']}: Kinesis stream=#{row['stream_name']}, " +
              "shard=#{row['shard_id']}. Sequence from #{row['starting_sequence_number']} to #{row['ending_sequence_number']}")

            # Abort the transaction
            exec('ROLLBACK')
            result = {
                :success => false,
                :load_error => 'Duplicated kinesis range',
                :duplicated_load => row
            }

            return result
          end
        end

        # Insert entry for load
        insert_query = 'INSERT INTO kinesis_loads VALUES '
        insert_values = []
        shards_to_load.each do |shard_to_load|
          insert_values << "('#{stream_name}','#{shard_to_load[:shard_id]}','#{redshift_table}','#{shard_to_load[:starting_sequence_number]}','#{shard_to_load[:ending_sequence_number]}',getdate())"
        end
        insert_query += insert_values.join(",") + ';'
        exec(insert_query)

        # Load manifest into redshift & commit transaction if successful
        load_file_result = load_file_into_redshift(redshift_table,manifest_s3_url)
        if load_file_result[:success] == true
          exec('COMMIT')
        else
          @logger.warn("Load file returned a failure: #{load_file_result[:load_error]}")
          exec('ROLLBACK')
        end

        result = load_file_result

      rescue Exception => e

        # Catch exceptions & Abort transaction
        @logger.warn("#{TAG} Exception caught: #{e.class}: #{e.message}\n\t#{e.backtrace.join("\n\t")}")
        exec('ROLLBACK')

        # Get list of files in the manifest
        manifest_files = []
        shards_to_load.each do |shard_to_load|
          shard_to_load[:s3_urls].each do |s3_url|
            manifest_files << s3_url
          end
        end

        # Query for load errors
        load_error = get_last_load_errors(redshift_table,manifest_files)
        @logger.warn("Load Error: #{YAML::dump(load_error)}")
        result = {
            :success => false,
            :exception => e,
            :load_error => load_error
        }
      end

      return result
    end

    # Load a file into redshift
    #
    # @param [String] redshift_table The table to load the data into
    # @param [String] s3_url The s3 file to load into redshift
    # @param [Boolean] manifest Whether this is a COPY of a manifest file
    def load_file_into_redshift(redshift_table,s3_url,manifest=true)

      ## Run the copy command to load data from s3 to Redshift. This is cleaner than doing the ssh method
      cmd="COPY #{redshift_table} from '#{s3_url}' with " +
        "credentials 'aws_access_key_id=#{@options[:access_key_id]};aws_secret_access_key=#{@options[:secret_access_key]}' " +
        "json 'auto' timeformat 'auto' GZIP MAXERROR #{@options[:max_error]}"
      if manifest
        cmd += ' manifest'
      end
      cmd += ';'
      records=nil

      # Set receiver to check how many rows are loaded (via the INFO)
      @redshift_conn.set_notice_receiver {|result|
        matches=/.*,.(\d+).record.*/.match(result.error_message)
        records = matches[1].to_i
      }

      exec(cmd)
      result = {
          :success => true,
          :records => records
      }

      return result
    end

    # Print the last load error for a specific redshift table
    #
    # @param [String] redshift_table The name of the redshift table
    # @param [Array] s3_urls The s3 urls that were loaded into redshift
    def get_last_load_errors(redshift_table,s3_urls)

      # Query to get recent load errors matching table and s3 url
      cmd = 'select  tbl, trim(name) as table_name, starttime, trim(filename) as filename, line_number, trim(raw_line) as raw_line,' +
        'trim(colname) as colname, trim(raw_field_value) as raw_field_value, err_code, trim(err_reason) as err_reason ' +
        'from stl_load_errors sl, stv_tbl_perm sp ' +
        "where sl.tbl = sp.id AND sp.name='#{redshift_table}' AND sl.filename IN ('#{s3_urls.join("','")}') " +
        'ORDER BY starttime DESC LIMIT 20;'
      result_set=exec(cmd)

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
    # @param [String] load_identifier Identifier for the load
    # @return [String] The relative s3 location in a bucket
    def s3_prefix(redshift_table,date,load_identifier=nil)
      dateyyyymmdd=date.strftime('%Y%m%d')
      result = "redtrack/#{@options[:redshift_cluster_name]}/#{@options[:redshift_dbname]}/#{redshift_table}/#{dateyyyymmdd}/"
      if load_identifier != nil
        result += load_identifier + "/"
      end
      return result
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
