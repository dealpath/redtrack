# Datatypes provides a run time bound implementation for validating passed data types
#
# Copyright (c) 2014 RedHotLabs, Inc.
# Licensed under the MIT License
require 'bigdecimal'

module RedTrack
  class DataTypes

    @logger = nil
    @options = nil

    TAG = 'RedTrack::DataTypes'

    # Constructor - non-static... Want runtime bound interface
    def initialize(options)
      if options && options[:logger] != nil
        @logger = options[:logger]
      else
        @logger = Logger.new(STDOUT)
      end

      @options = options
    end

    # @return [Array] Return an array of valid data types
    def valid_data_types
      result = ['smallint', 'integer', 'bigint', 'decimal', 'real', 'double precision', 'boolean', 'char', 'varchar', 'date', 'timestamp']
      return result
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_smallint(value,type_definition=nil,column_name=nil)
      if value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition,"Integer")
      end
      # TODO: Range / overflow check
      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_integer(value,type_definition=nil,column_name=nil)
      if value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition,"Integer")
      end
      # TODO: range / overflow check
      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_bigint(value,type_definition=nil,column_name=nil)
      if value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition,"Integer")
      end
      # TODO: range /overflow check
      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_decimal(value,type_definition=nil,column_name=nil)
      if value.is_a?(BigDecimal) == false
        raise_exception(column_name,value,type_definition,"BigDecimal")
      end

      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_real(value,type_definition=nil,column_name=nil)
      if value.is_a?(Float) == false && value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition,"Float or Integer")
      end

      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_double_precision(value,type_definition=nil,column_name=nil)
      if value.is_a?(Float) == false && value.is_a?(Integer) == false
        raise_exception(column_name,value,type_definition,"Float or Integer")
      end

      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_boolean(value,type_definition=nil,column_name=nil)
      if value.is_a?(TrueClass) == false && value.is_a?(FalseClass) == false
        raise_exception(column_name,value,type_definition,"TrueClass or FalseClass")
      end

      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid - truncated if it is too long
    def check_char(value,type_definition=nil,column_name=nil)
      if value.is_a?(String) == false
        raise_exception(column_name,value,type_definition,"String")
      end
      # Truncate values that are too long
      value = truncate_string(column_name,value,type_definition)
      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid - truncated if too long
    def check_varchar(value,type_definition=nil,column_name=nil)
      if value.is_a?(String) == false
        raise_exception(column_name,value,type_definition,"String")
      end
      # Truncate values that are too long
      value = truncate_string(column_name,value,type_definition)
      return value
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #  http://docs.aws.amazon.com/redshift/latest/dg/r_DATEFORMAT_and_TIMEFORMAT_strings.html
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid - truncated if too long
    def check_date(value,type_definition=nil,column_name=nil)
      if value.is_a?(Date) == false
        raise_exception(column_name,value,type_definition,"Date")
      end

      # Return the value in default date format for redshift: (YYYY-MM-DD)
      return value.strftime("%Y-%m-%d")
    end

    # Check and clean value to ensure it conforms to the redshift data type
    #  http://docs.aws.amazon.com/redshift/latest/dg/r_DATEFORMAT_and_TIMEFORMAT_strings.html
    #
    # @param [Object] value the value to set for the column
    # @param [String] type_definition the the type defined by the schema
    # @param [String] column_name The name of the redshift column
    # @return [Object] The value if it is valid
    def check_timestamp(value,type_definition=nil,column_name=nil)
      if value.is_a?(Time) == false
        raise_exception(column_name,value,type_definition,"Time")
      end

      # Check to see if UTC
      if @options[:convert_timestamps_utc] == true
        value = value.utc
      end

      # Return value in default timestamp format for redshift: (YYYY-MM-DD HH:MI:SS)
      return value.strftime("%Y-%m-%d %H:%M:%S")
    end

    private

    # Helper function, raise a general exception message
    #
    # @param [String] column_name The name of the redshift column
    # @param [Object] value the value to set for the column
    # @param [String] redshift_type_definition the the type defined by the schema
    # @param [String] ruby_type_expected Ruby type expected
    def raise_exception(column_name,value,redshift_type_definition,ruby_type_expected=nil)
      message = "Value for column #{column_name}, #{value.to_s}, does not conform to redshift type '#{redshift_type_definition}'."
      if ruby_type_expected != nil
        message += "Expected specific ruby type #{ruby_type_expected}"
      end
      raise message
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
      if(value.length > num_chars)
        @logger.warn("#{TAG} Data for column #{column_name} is too long (#{value.length} characters) for column type and will be truncated to #{num_chars} characters: '#{value}'")
        return value[0..num_chars-1]
      else
        return value
      end
    end

  end
end