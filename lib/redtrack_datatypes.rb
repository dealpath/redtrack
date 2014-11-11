# Datatypes provides a run time bound implementation for validating passed data types
#
# Copyright (c) 2014 RedHotLabs, Inc.
# Licensed under the MIT License

module RedTrack
  class DataTypes

    @logger = nil

    # Constructor - non-static... Want runtime bound interface
    def initialize(options)
      if options && options[:logger] != nil
        @logger = options[:logger]
      else
        @logger = Logger.new(STDOUT)
      end
    end

    # @return [Array] Return an array of valid data types
    def valid_data_types
      result = %w(smallint integer bigint decimal real boolean char varchar date timestamp)
      return result
    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid
    def check_smallint(column_name,value,type_definition)
      if value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition)
      end
      # TODO: Range / overflow check
      return value
    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid
    def check_integer(column_name,value,type_definition)
      if value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition)
      end
      # TODO: range / overflow check
      return value
    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid
    def check_bigint(column_name,value,type_definition)
      if value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition)
      end
      # TODO: range /overflow check
      return value
    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid
    def check_decimal(column_name,value,type_definition)
      if value.is_a?(String) == false || is_numeric(value) == false
        raise_exception(column_name,value,type_definition)
        #raise ""
      end

      return value
    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid
    def check_real(column_name,value,type_definition)
      if is_numeric(value) == false
        raise_exception(column_name,value,type_definition)
      end

      return value
    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid - truncated if it is too long
    def check_char(column_name,value,type_definition)
      if value.is_a?(String) == false
        raise_exception(column_name,value,type_definition)
      end
      # Truncate values that are too long
      value = truncate_string(column_name,value,type_definition)
      return value
    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid - truncated if too long
    def check_varchar(column_name,value,type_definition)
      if value.is_a?(String) == false
        raise_exception(column_name,value,type_definition)
      end
      # Truncate values that are too long
      value = truncate_string(column_name,value,type_definition)
      return value
    end

    def check_date(column_name,value,type_definition)

    end

    # Check and clean value to ensure it conforms to the redshfit data type
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @return [Object] The value if it is valid
    def check_timestamp(column_name,value,type_definition)
      if value.is_a?(String) == false || value[/\A\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\z/] == nil
        raise_exception(column_name,value,type_definition)
      end
      return value
    end

    private

    # Helper function, raise a general exception message
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    def raise_exception(column_name,value,type_definition)
      raise "Value for column #{column_name}, '#{value.to_s}', does not conform to type '#{type_definition}'"
    end

    # Determine whether the typed value is a legit number, (eg, string)
    #
    # @param [Numeric] value The value to check as valid numeric
    # @return [Boolean] Whether or not the value is a numeric
    def is_numeric(value)
      Float(value) != nil rescue false
    end

    def truncate_string(column_name,value,type_definition)
      num_chars = type_definition[/\((\d*)\)/,1].to_i
      puts "Num chars: #{num_chars}"
      if(value.length > num_chars)
        @logger.warn("#{TAG} Data for column #{column_name} is too long (#{value.length} characters) for column type and will be truncated to #{num_chars} characters: '#{value}'")
        return value[0..num_chars-1]
      else
        return value
      end
    end

  end
end