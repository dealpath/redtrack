require 'spec_helper'
require 'yaml'

RSpec.describe RedTrack::Client do

  let(:redshift_schema) {
    redshift_schema = {
        :test => {
          :columns => {
              :test_varchar =>      { :type => 'varchar(4)', :constraint => 'not null'},
              :test_integer =>      { :type => 'integer', :constraint => 'not null'},
              :payment =>           { :type => 'decimal(8,2)'},
              :test_real =>         { :type => 'real'},
              :test_double =>       { :type => 'double precision'},
              :test_bool =>         { :type => 'boolean'},
              :test_char =>         { :type => 'CHAR'},

          },
          :sortkey => 'timestamp'
        }
    }
  }

  let(:redtrack_client) {
    options = {
        :redshift_schema => redshift_schema,
        :kinesis_enabled => false
    }

    # Stub calls
    AWS.stub(:config).and_return(true)
    RedTrack::FileClient.any_instance.stub(:stream_name).and_return("")
    RedTrack::FileClient.any_instance.stub(:stream_write).and_return(true)
    redtrack_client = RedTrack::Client.new(options)
    redtrack_client
  }

  let(:fake_valid_datapoint) {
    data = {
        :test_varchar => 'test',
        :test_integer => 1,
        :payment => BigDecimal.new("1.05"),
        :test_real => 1.1,
        :test_double => 1.2,
        :test_bool => true,
        :test_char => 'test'
    }
    data
  }

  context '[Public Methods]' do
    context 'write()' do
      it 'should accept valid datapoint' do
        result = redtrack_client.write("test",fake_valid_datapoint)
        expect(result).to eq(true)
      end

      it 'should reject data without a needed field' do
        new_fake_valid_datapoint = {
            test_varchar: 'test'
        }
        expect{redtrack_client.write("test",new_fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data with a non-existent field' do
        fake_valid_datapoint[:test_nothing] = "1"

        expect{redtrack_client.write("test",fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data with invalid integer' do
        fake_valid_datapoint[:test_integer] = "1"
        expect{redtrack_client.write("test",fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data with invalid varchar' do
        fake_valid_datapoint[:test_varchar] = 1
        expect{redtrack_client.write("test",fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data with invalid decimal' do
        fake_valid_datapoint[:test_decimal] = 1.1
        expect{redtrack_client.write("test",fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data with invalid real' do
        fake_valid_datapoint[:test_real] = "1.1"
        expect{redtrack_client.write("test",fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data with invalid bool' do
        fake_valid_datapoint[:test_bool] = "true"
        expect{redtrack_client.write("test",fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data without symbols as field names' do
        new_fake_valid_datapoint = {
            "test_varchar" => 'test',
            "test_integer" => 1
        }
        expect{redtrack_client.write("test",new_fake_valid_datapoint)}.to raise_error
      end

      it 'should reject data with invalid table name' do
        expect{redtrack_client.write("test_bad_table_name",new_fake_valid_datapoint)}.to raise_error
      end

    end

    context 'create_kinesis_loads_table()' do
      it 'should create a valid table schema' do
        result = redtrack_client.create_kinesis_loads_table(false)
        puts "cReate kinesis loads result: "
        expect(result.include?('create table kinesis_loads')).to eq(true)
        expect(result.include?('stream_name varchar(64)')).to eq(true)
        expect(result.include?('shard_id varchar(64)')).to eq(true)
        expect(result.include?('table_name varchar(64)')).to eq(true)
        expect(result.include?('starting_sequence_number varchar(64)')).to eq(true)
        expect(result.include?('ending_sequence_number varchar(64)')).to eq(true)
        expect(result.include?('load_timestamp timestamp')).to eq(true)
      end
    end
  end
end
